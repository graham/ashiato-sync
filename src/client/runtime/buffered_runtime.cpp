#include <cstdio>
#include "client/runtime/buffered_runtime.hpp"

#include "client/runtime/cue_runtime.hpp"
#include "client/store/entity_store.hpp"
#include "client/tracing.hpp"

#include "ashiato/sync/client.hpp"

#include <vector>

namespace ashiato::sync::client_detail {

ClientBufferedRuntime::ClientBufferedRuntime(std::size_t frame_capacity)
    : buffered_frames_(frame_capacity) {}

void ClientBufferedRuntime::reset_entity(std::uint32_t entity_index) noexcept {
    buffered_frames_.reset(entity_index);
}

void ClientBufferedRuntime::ensure_entity(std::uint32_t entity_index) {
    buffered_frames_.ensure(entity_index);
}

void ClientBufferedRuntime::clear_entity(std::uint32_t entity_index) noexcept {
    buffered_frames_.clear(entity_index);
}

bool ClientBufferedRuntime::apply_frames(
    ReplicationClient& client,
    ashiato::Registry& registry,
    const ReplicationClientClock::FrameRange& frames) {
    bool all_valid = true;
    if (!frames.empty()) { static SyncFrame nfd_last = 0; if (nfd_last != 0 && frames.first > nfd_last + 1) std::fprintf(stderr, "NFD frames_jump from=%u to=%u" "\n", (unsigned)nfd_last, (unsigned)frames.first); nfd_last = frames.last; }
    // FRAMES THE CLOCK SKIPPED STILL HOLD DESTROYS. After a client stall the buffered clock resumes past frames it
    // never applied, and an entity destroyed in that gap has its absent sample there and nothing after it: the
    // sampler then finds it starved at every later frame and it stays on this client for good (cockpit's
    // stall_loss: 120 ticks stalled, 24 despawned, 13 left). So the gap is swept once for absent samples.
    if (!frames.empty() && has_applied_buffered_frame_ && frames.first > last_applied_buffered_frame_ + 1U) {
        apply_skipped_destroys(client, registry, last_applied_buffered_frame_ + 1U, frames.first - 1U);
    }
    for (SyncFrame frame = frames.first; !frames.empty() && frame <= frames.last; ++frame) {
        all_valid = apply_frame(client, registry, frame) && all_valid;
        if (frame == frames.last) {
            break;
        }
    }
    return all_valid;
}

void ClientBufferedRuntime::apply_skipped_destroys(
    ReplicationClient& client,
    ashiato::Registry& registry,
    SyncFrame first,
    SyncFrame last) {
    if (!client.has_buffered_entities() || last < first) {
        return;
    }
    const SyncSettings& settings = registry.get<SyncSettings>();
    std::vector<std::uint32_t> destroyed;
    for (const std::uint32_t entity_index : client.entity_store_->buffered_entity_indices()) {
        if (entity_index >= client.entity_store_->entity_count() || buffered_frames_.empty(entity_index)) {
            continue;
        }
        EntityState& state = client.entity_store_->state_unchecked(entity_index);
        if (state.identity.local) {
            continue;
        }
        for (SyncFrame frame = first; frame <= last; ++frame) {
            EntityFrameView sample;
            if (buffered_frames_.view(entity_index, frame, sample) && !sample.entity_present) {
                if (client.apply_buffered_sample(registry, settings, state, sample)) {
                    destroyed.push_back(entity_index);
                }
                break;
            }
            if (frame == last) {
                break;
            }
        }
    }
    for (const std::uint32_t entity_index : destroyed) {
        client.erase_entity_state(registry, entity_index, false);
    }
}

bool ClientBufferedRuntime::apply_frame(
    ReplicationClient& client,
    ashiato::Registry& registry,
    SyncFrame buffered_frame,
    MissingBufferedFramePolicy missing_frame_policy) {
    last_applied_buffered_frame_ = buffered_frame;
    has_applied_buffered_frame_ = true;
    if (!client.has_buffered_entities()) {
        return true;
    }
    const SyncSettings& settings = registry.get<SyncSettings>();
    bool all_valid = true;
    std::vector<std::uint32_t> applied_destroys;
    std::vector<std::uint32_t> destroyed_unsampled;
#ifdef ASHIATO_SYNC_ENABLE_INTERPOLATION_DIAGNOSTICS
    std::uint64_t interpolation_checks = 0;
    std::uint64_t interpolation_starvations = 0;
#endif
    for (const std::uint32_t entity_index : client.entity_store_->buffered_entity_indices()) {
        if (entity_index >= client.entity_store_->entity_count()) {
            continue;
        }
        EntityState& state = client.entity_store_->state_unchecked(entity_index);
        // A DESTROY THE SAMPLER WENT PAST WITHOUT APPLYING. The absent sample is applied at its own frame; if the
        // buffered clock is already beyond it -- after a client stall the burst and the catch-up can carry it past, or
        // the sample is gone -- the entity would starve here for good while the server has destroyed it.
        if (!state.identity.local && state.replication.destroy_frame != 0 && buffered_frame > state.replication.destroy_frame) {
            destroyed_unsampled.push_back(entity_index);
            continue;
        }
        if (!state.identity.local && !state.replication.entity_present) {
            static std::uint32_t nfd_seen = 0;
            if (nfd_seen < 400) { ++nfd_seen;
                EntityFrameView nfd_sample;
                const bool nfd_view = !buffered_frames_.empty(entity_index) && buffered_frames_.view(entity_index, buffered_frame, nfd_sample);
                std::fprintf(stderr, "NFD absent idx=%u at=%u destroy_frame=%u empty=%d view=%d present_in_view=%d" "\n",
                    entity_index, (unsigned)buffered_frame, (unsigned)state.replication.frame,
                    (int)buffered_frames_.empty(entity_index), (int)nfd_view, nfd_view ? (int)nfd_sample.entity_present : -1); }
        }
        if (buffered_frames_.empty(entity_index)) {
            // AN ENTITY THE SERVER DESTROYED, WITH NOTHING LEFT TO SAMPLE: its absent frame was never viewed (after a
            // stall the history it was written into is gone), so it would starve here for good. Take it down.
            if (!state.identity.local && !state.replication.entity_present) {
                destroyed_unsampled.push_back(entity_index);
                continue;
            }
#ifdef ASHIATO_SYNC_ENABLE_INTERPOLATION_DIAGNOSTICS
            ++interpolation_checks;
            ++interpolation_starvations;
#endif
#ifdef ASHIATO_SYNC_ENABLE_TRACING
            if (client.tracer_ != nullptr && client.tracer_->enabled()) {
                SyncTraceEvent event = make_client_trace_event(SyncTraceEventType::BufferedStarved, client.client_id_, buffered_frame);
                event.server_entity = ashiato::Entity{state.identity.client_entity_network_id};
                event.local_entity = state.identity.local;
                event.client_network_id = state.identity.client_entity_network_id;
                event.wire_network_id = state.identity.wire_network_id;
                event.network_version = client_entity_network_id_version(state.identity.client_entity_network_id);
                event.archetype = state.identity.archetype;
                client.tracer_->trace(event);
            }
#endif
            continue;
        }

        EntityFrameView sample;
        if (!buffered_frames_.view(entity_index, buffered_frame, sample)) {
            const bool reused_future_entity =
                state.replication.entity_present &&
                buffered_frame < state.replication.frame &&
                client_entity_network_id_version(state.identity.client_entity_network_id) > 1U;
            const bool destroyed_past_entity =
                !state.replication.entity_present &&
                buffered_frame > state.replication.frame;
            // A DESTROYED ENTITY WHOSE ABSENT SAMPLE WAS NEVER VIEWED is erased here, not skipped. After a client stall
            // the burst and the catch-up carry the buffered frame past the destroy frame without viewing the absent
            // sample, `view` then fails at every later frame, and skipping left the entity alive on this client for
            // good while the server had destroyed it (a 120-tick stall with 24 churned: 13 ghosts).
            if (!state.identity.local && destroyed_past_entity) {
                // Apply the absent sample at the destroy frame itself -- that is what takes the replicated
                // components off the local entity -- and then erase, exactly as a viewed destroy is.
                EntityFrameView absent;
                if (buffered_frames_.view(entity_index, state.replication.frame, absent) && !absent.entity_present
                    && client.apply_buffered_sample(registry, settings, state, absent)) {
                    applied_destroys.push_back(entity_index);
                } else {
                    // The absent sample itself is gone from the history too: take the local entity down with its
                    // state, as a snapped destroy does.
                    destroyed_unsampled.push_back(entity_index);
                }
                continue;
            }
            if (!state.identity.local && reused_future_entity) {
                continue;
            }
#ifdef ASHIATO_SYNC_ENABLE_INTERPOLATION_DIAGNOSTICS
            ++interpolation_checks;
            ++interpolation_starvations;
#endif
#ifdef ASHIATO_SYNC_ENABLE_TRACING
            if (client.tracer_ != nullptr && client.tracer_->enabled()) {
                SyncTraceEvent event = make_client_trace_event(SyncTraceEventType::BufferedStarved, client.client_id_, buffered_frame);
                event.server_entity = ashiato::Entity{state.identity.client_entity_network_id};
                event.local_entity = state.identity.local;
                event.client_network_id = state.identity.client_entity_network_id;
                event.wire_network_id = state.identity.wire_network_id;
                event.network_version = client_entity_network_id_version(state.identity.client_entity_network_id);
                event.archetype = state.identity.archetype;
                client.tracer_->trace(event);
            }
#endif
            if (missing_frame_policy == MissingBufferedFramePolicy::FailApply) {
                all_valid = false;
            }
            continue;
        }
#ifdef ASHIATO_SYNC_ENABLE_INTERPOLATION_DIAGNOSTICS
        ++interpolation_checks;
#endif
        if (!client.apply_buffered_sample(registry, settings, state, sample)) {
            all_valid = false;
            continue;
        }
        if (!sample.entity_present) {
            applied_destroys.push_back(entity_index);
            continue;
        }
        client.cue_runtime_->play_buffered_for_frame(client, registry, settings, entity_index, state, buffered_frame);
    }
    if (!applied_destroys.empty()) std::fprintf(stderr, "NFD erased_by_sampler n=%zu at=%u" "\n", applied_destroys.size(), (unsigned)buffered_frame);
    if (!destroyed_unsampled.empty()) std::fprintf(stderr, "NFD erased_unsampled n=%zu" "\n", destroyed_unsampled.size());
    for (const std::uint32_t entity_index : destroyed_unsampled) {
        client.erase_entity_state(registry, entity_index, true);
    }
    for (const std::uint32_t entity_index : applied_destroys) {
        client.erase_entity_state(registry, entity_index, false);
    }
    client.cue_runtime_->discard_applied_buffered(client, buffered_frame);

#ifdef ASHIATO_SYNC_ENABLE_INTERPOLATION_DIAGNOSTICS
    client.record_interpolation_frame(interpolation_checks, interpolation_starvations);
#endif
    return all_valid;
}

}  // namespace ashiato::sync::client_detail
