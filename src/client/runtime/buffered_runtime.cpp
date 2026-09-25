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
    for (SyncFrame frame = frames.first; !frames.empty() && frame <= frames.last; ++frame) {
        all_valid = apply_frame(client, registry, frame) && all_valid;
        if (frame == frames.last) {
            break;
        }
    }
    return all_valid;
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
#ifdef ASHIATO_SYNC_ENABLE_INTERPOLATION_DIAGNOSTICS
    std::uint64_t interpolation_checks = 0;
    std::uint64_t interpolation_starvations = 0;
#endif
    for (const std::uint32_t entity_index : client.entity_store_->buffered_entity_indices()) {
        if (entity_index >= client.entity_store_->entity_count()) {
            continue;
        }
        EntityState& state = client.entity_store_->state_unchecked(entity_index);
        if (buffered_frames_.empty(entity_index)) {
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
                applied_destroys.push_back(entity_index);
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
