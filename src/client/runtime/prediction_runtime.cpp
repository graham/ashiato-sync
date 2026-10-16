#include "client/runtime/prediction_runtime.hpp"

#include "client/runtime/buffered_runtime.hpp"
#include "client/runtime/cue_runtime.hpp"
#include "client/store/cue_store.hpp"
#include "client/store/entity_store.hpp"
#include "client/frame_data.hpp"
#include "client/store/frame_ring_store.hpp"
#include "client/store/input_buffer.hpp"

#include "ashiato/sync/client.hpp"
#include "ashiato/sync/profiling.hpp"

#include <algorithm>
#include <cstdint>

namespace ashiato::sync::client_detail {
namespace {

bool all_zero(const SyncComponentOps::QuantizedBytes& bytes) {
    return std::all_of(bytes.begin(), bytes.end(), [](std::uint8_t byte) {
        return byte == 0U;
    });
}

}  // namespace

ClientPredictionRuntime::ClientPredictionRuntime(std::size_t frame_capacity)
    : predicted_frames_(frame_capacity),
      presentation_frames_(frame_capacity) {}

void ClientPredictionRuntime::reset_entity(std::uint32_t entity_index) noexcept {
    predicted_frames_.reset(entity_index);
    presentation_frames_.reset(entity_index);
}

void ClientPredictionRuntime::ensure_entity(std::uint32_t entity_index) {
    predicted_frames_.ensure(entity_index);
    presentation_frames_.ensure(entity_index);
}

void ClientPredictionRuntime::clear_entity(std::uint32_t entity_index) noexcept {
    predicted_frames_.clear(entity_index);
    presentation_frames_.clear(entity_index);
}

void ClientPredictionRuntime::reset_predicted_frame() noexcept {
    has_predicted_frame_ = false;
    last_predicted_frame_ = 0;
}

void ClientPredictionRuntime::set_active_snap_lead(SyncFrame frames) noexcept {
    active_prediction_snap_lead_frames_ = frames;
}

SyncFrame ClientPredictionRuntime::update_active_snap_lead_from_server_update(
    SyncFrame server_frame,
    bool clock_requested_prefill,
    SyncFrame target_prediction_lead_frames,
    SyncFrame current_prediction_lead_frames) noexcept {
    if (clock_requested_prefill) {
        active_prediction_snap_lead_frames_ = target_prediction_lead_frames;
        return 0;
    }
    if (active_prediction_snap_lead_frames_ == 0U) {
        return 0;
    }
    if (target_prediction_lead_frames < active_prediction_snap_lead_frames_) {
        active_prediction_snap_lead_frames_ = 0;
        return 0;
    }
    return current_prediction_lead_frames < active_prediction_snap_lead_frames_
        ? server_frame + active_prediction_snap_lead_frames_
        : 0U;
}

void ClientPredictionRuntime::schedule_catchup(SyncFrame server_frame, SyncFrame target_input_frame) noexcept {
    if (target_input_frame > pending_prediction_catchup_frame_) {
        pending_prediction_catchup_frame_ = target_input_frame;
        pending_prediction_catchup_server_frame_ = server_frame;
    }
}

bool ClientPredictionRuntime::seed_first_authoritative_frame(
    ReplicationClient& client,
    ashiato::Registry& registry,
    const SyncSettings& settings,
    SyncFrame frame) {
    if (has_predicted_frame_) {
        return true;
    }
    if (!client.fill_input_frames_through(registry, settings, frame)) {
        return false;
    }
    ++prediction_seed_count_;
    client.clock_.advance_predicted_frame_to(frame);
    last_predicted_frame_ = frame != 0U ? frame - 1U : 0U;
    has_predicted_frame_ = true;
    return true;
}

bool ClientPredictionRuntime::seed_existing_authoritative_frame(
    ReplicationClient& client,
    ashiato::Registry& registry,
    const SyncSettings& settings,
    SyncFrame frame) {
    if (has_predicted_frame_) {
        return true;
    }
    if (!client.fill_input_frames_through(registry, settings, frame)) {
        return false;
    }
    ++prediction_seed_count_;
    client.clock_.advance_predicted_frame_to(frame);
    last_predicted_frame_ = frame;
    has_predicted_frame_ = true;
    return true;
}

void ClientPredictionRuntime::refresh_pending_rollback_frame(ReplicationClient& client) noexcept {
    has_pending_prediction_rollback_ = false;
    pending_prediction_rollback_frame_ = 0;
    for (const std::uint32_t entity_index : client.entity_store_->prediction_rollback_entity_indices()) {
        if (entity_index >= client.entity_store_->entity_count()) {
            continue;
        }
        const EntityState& state = client.entity_store_->state_unchecked(entity_index);
        if (!state.prediction.rollback_pending || state.prediction.rollback_frame == 0U) {
            continue;
        }
        if (!has_pending_prediction_rollback_ ||
            state.prediction.rollback_frame < pending_prediction_rollback_frame_) {
            pending_prediction_rollback_frame_ = state.prediction.rollback_frame;
            has_pending_prediction_rollback_ = true;
        }
    }
}

void ClientPredictionRuntime::queue_rollback(ReplicationClient& client, EntityState& state, SyncFrame frame) {
    ++rollback_queued_count_;
    const std::uint32_t entity_index = client.entity_store_->index_of(state);
    client.entity_store_->queue_prediction_rollback(entity_index, frame);
    refresh_pending_rollback_frame(client);
}

bool ClientPredictionRuntime::run_frame(
    ReplicationClient& client,
    ashiato::Registry& registry,
    SyncFrame frame,
    const ashiato::RunJobsOptions& options) {
    ASHIATO_SYNC_PROFILE_SCOPE("AshiatoSync_ClientPredictionFrame");

    if (!apply_pending_rollback(client, registry, options)) {
        return false;
    }

    const SyncSettings& settings = registry.get<SyncSettings>();
    if (!client.apply_input_frame(registry, settings, frame)) {
        return false;
    }
    registry.write<FrameInfo>().frame = frame;
    {
        ASHIATO_SYNC_PROFILE_SCOPE("AshiatoSync_ClientPredictionJobGraph");
        registry.run_jobs(options);
    }

    {
        ASHIATO_SYNC_PROFILE_SCOPE("AshiatoSync_ClientPredictionDrainCues");
        client.cue_runtime_->drain_emitted_prediction(client, registry, settings, frame, true);
    }
    ++predicted_frame_run_count_;
    bool all_valid = true;
    {
        ASHIATO_SYNC_PROFILE_SCOPE("AshiatoSync_ClientQuantizePredicted");
        for (std::uint32_t entity_index : client.entity_store_->active_entity_indices()) {
            if (entity_index >= client.entity_store_->entity_count()) {
                continue;
            }
            EntityState& state = client.entity_store_->state_unchecked(entity_index);
            if (state.mode.current != ReplicationClientMode::Predict ||
                state.identity.client_entity_network_id == invalid_client_entity_network_id) {
                continue;
            }
            if (!client.quantize_predicted_entity(
                    registry,
                    settings,
                    state,
                    frame,
                    FrameWriteSource::PredictedFrame)) {
                all_valid = false;
            }
        }
    }

    last_predicted_frame_ = frame;
    has_predicted_frame_ = true;
#ifdef ASHIATO_SYNC_ENABLE_TRACING
    client.trace_frame_components(
        registry,
        settings,
        frame,
        false,
        false,
        ReplicationClient::TraceFrameComponentScope::Predicted);
#endif
    return all_valid;
}

bool ClientPredictionRuntime::run_catchup(
    ReplicationClient& client,
    ashiato::Registry& registry,
    std::uint32_t predicted_steps_this_tick,
    const ashiato::RunJobsOptions& options) {
    ASHIATO_SYNC_PROFILE_SCOPE("AshiatoSync_ClientPredictionCatchup");

    if (client.options_.clock.auto_timing_fast_recovery &&
        client.has_predicted_entities() &&
        has_predicted_frame_ &&
        pending_prediction_catchup_frame_ > client.clock_.predicted_frame()) {
        const SyncFrame first = client.clock_.predicted_frame() + 1U;
        const std::uint32_t max_steps = client.options_.clock.max_fixed_steps_per_tick;
        const std::uint32_t remaining_steps =
            max_steps == 0U || predicted_steps_this_tick >= max_steps ? 0U : max_steps - predicted_steps_this_tick;
        const SyncFrame last = max_steps == 0U
            ? pending_prediction_catchup_frame_
            : std::min(
                  pending_prediction_catchup_frame_,
                  static_cast<SyncFrame>(client.clock_.predicted_frame() + remaining_steps));
        const SyncSettings& settings = registry.get<SyncSettings>();
        for (SyncFrame frame = first; frame <= last; ++frame) {
            (void)client.record_input_frame(registry, settings, frame);
            if (frame > last_predicted_frame_ && !run_frame(client, registry, frame, options)) {
                return false;
            }
            client.clock_.advance_predicted_frame_to(frame);
        }
        if (pending_prediction_catchup_server_frame_ != 0U) {
#ifdef ASHIATO_SYNC_ENABLE_TRACING
            const SyncFrame catchup_server_frame = pending_prediction_catchup_server_frame_;
#endif
            client.clock_.record_prediction_lead(pending_prediction_catchup_server_frame_, client.clock_.predicted_frame());
#ifdef ASHIATO_SYNC_ENABLE_TRACING
            client.trace_clock_skew(
                "prediction_catchup_applied",
                static_cast<SyncFrame>(std::max(0.0, client.clock_.estimated_server_frame())),
                catchup_server_frame,
                client.clock_.estimated_server_frame(),
                client.clock_.buffered_frame(),
                client.input_->last_recorded_frame(),
                client.clock_.predicted_frame());
#endif
        }
        if (max_steps != 0U && pending_prediction_catchup_frame_ > client.clock_.predicted_frame()) {
            const std::uint64_t dropped =
                static_cast<std::uint64_t>(pending_prediction_catchup_frame_ - client.clock_.predicted_frame());
            client.clock_.mutable_stats().dropped_input_frames += dropped;
            pending_prediction_catchup_frame_ = client.clock_.predicted_frame();
        }
    }
    if (pending_prediction_catchup_frame_ <= client.clock_.predicted_frame()) {
        pending_prediction_catchup_frame_ = 0;
        pending_prediction_catchup_server_frame_ = 0;
    }
    return true;
}

void ClientPredictionRuntime::collect_resimulated_entities(
    ReplicationClient& client,
    std::vector<std::uint32_t>& out) const {
    out.clear();
    // PredictedOnly resets and replays every predicted entity, exactly as All does.
    const bool all = client.options_.prediction.rollback_policy != ReplicationRollbackPolicy::OnlyAffected;
    if (!all) {
        out.reserve(client.entity_store_->prediction_rollback_entity_indices().size());
        for (const std::uint32_t entity_index : client.entity_store_->prediction_rollback_entity_indices()) {
            if (entity_index < client.entity_store_->entity_count()) {
                const EntityState& state = client.entity_store_->state_unchecked(entity_index);
                if (state.mode.current == ReplicationClientMode::Predict && state.prediction.rollback_pending) {
                    out.push_back(entity_index);
                }
            }
        }
        return;
    }

    out.reserve(client.entity_store_->active_entity_indices().size());
    for (const std::uint32_t entity_index : client.entity_store_->active_entity_indices()) {
        if (entity_index >= client.entity_store_->entity_count()) {
            continue;
        }
        const EntityState& state = client.entity_store_->state_unchecked(entity_index);
        if (state.mode.current == ReplicationClientMode::Predict && (all || state.prediction.rollback_pending)) {
            out.push_back(entity_index);
        }
    }
}

void ClientPredictionRuntime::capture_original_current(
    ReplicationClient& client,
    SyncFrame current_frame,
    const std::vector<std::uint32_t>& entity_indices,
    std::vector<OriginalPredictionCapture>& out) const {
    out.clear();
    out.reserve(entity_indices.size());
    for (const std::uint32_t entity_index : entity_indices) {
        if (entity_index >= client.entity_store_->entity_count()) {
            continue;
        }
        const EntityState& state = client.entity_store_->state_unchecked(entity_index);
        if (state.mode.current != ReplicationClientMode::Predict || predicted_frames_.empty(entity_index)) {
            continue;
        }
        EntityBufferedFrame sample;
        if (predicted_frames_.read(entity_index, current_frame, sample) && sample.entity_present) {
            out.push_back(OriginalPredictionCapture{entity_index, sample.baseline});
        }
    }
}

bool ClientPredictionRuntime::blend_resim_errors(
    ReplicationClient& client,
    const ashiato::Registry& registry,
    const SyncSettings& settings,
    SyncFrame current_frame,
    const std::vector<OriginalPredictionCapture>& original) {
    for (const OriginalPredictionCapture& capture : original) {
        const std::uint32_t entity_index = capture.entity_index;
        if (entity_index >= client.entity_store_->entity_count()) {
            continue;
        }
        EntityState& state = client.entity_store_->state_unchecked(entity_index);
        if (state.mode.current != ReplicationClientMode::Predict || predicted_frames_.empty(entity_index) ||
            state.identity.archetype.value >= settings.archetypes.size()) {
            continue;
        }
        EntityFrameView resimmed;
        if (!predicted_frames_.view(entity_index, current_frame, resimmed) || !resimmed.entity_present ||
            capture.baseline.bytes.empty()) {
            continue;
        }

        const SyncArchetype& archetype = settings.archetypes[state.identity.archetype.value];
        if (resimmed.baseline.byte_count < archetype.total_quantized_bytes ||
            capture.baseline.bytes.size() < archetype.total_quantized_bytes) {
            return false;
        }
        state.visual.snap_errors.clear();
        for (std::size_t component_index = 0; component_index < archetype.components.size(); ++component_index) {
            const ComponentReplication& replication = archetype.components[component_index];
            if (!registry.has<FractionalTickSampled>(replication.component) ||
                component_index >= archetype.component_ops.size()) {
                continue;
            }
            const SyncComponentOps& ops = archetype.component_ops[component_index];
            if (ops.compute_error == nullptr || ops.apply_error == nullptr || ops.blend_out_error == nullptr ||
                !frame_has_component(capture.baseline, component_index) ||
                !frame_has_component(resimmed.baseline, component_index)) {
                continue;
            }
            const std::uint8_t* resimmed_bytes =
                unchecked_frame_component_data(archetype, resimmed.baseline, component_index);
            const std::uint8_t* original_bytes =
                unchecked_frame_component_data(archetype, capture.baseline, component_index);
            SyncComponentOps::QuantizedBytes error;
            if (!ops.compute_error(resimmed_bytes, original_bytes, error)) {
                return false;
            }
            if (error.size() != ops.error_size) {
                return false;
            }
            if (!all_zero(error)) {
                state.visual.snap_errors.push_back(
                    EntityComponentError{replication.component, replication.serializer, std::move(error)});
            }
        }
        client.sync_entity_memberships(state);
    }
    return true;
}

bool ClientPredictionRuntime::apply_pending_rollback(
    ReplicationClient& client,
    ashiato::Registry& registry,
    const ashiato::RunJobsOptions& options) {
    ASHIATO_SYNC_PROFILE_SCOPE("AshiatoSync_ClientApplyPendingRollback");

    if (!has_pending_prediction_rollback_) {
        return true;
    }
    ++rollback_applied_count_;
    const SyncFrame begin_frame = pending_prediction_rollback_frame_;
    const SyncFrame current_frame = has_predicted_frame_ ? last_predicted_frame_ : begin_frame;
    {
        ASHIATO_SYNC_PROFILE_SCOPE("AshiatoSync_ClientCollectResimulatedEntities");
        collect_resimulated_entities(client, rollback_entity_indices_scratch_);
    }
    {
        ASHIATO_SYNC_PROFILE_SCOPE("AshiatoSync_ClientCaptureRollbackOriginal");
        capture_original_current(
            client,
            current_frame,
            rollback_entity_indices_scratch_,
            rollback_original_current_scratch_);
    }
    client.cue_runtime_->store().begin_resimulation();

    bool resimmed = false;
    switch (client.options_.prediction.rollback_policy) {
    case ReplicationRollbackPolicy::OnlyAffected:
        resimmed = resimulate_affected(client, registry, begin_frame, current_frame, options);
        break;
    case ReplicationRollbackPolicy::PredictedOnly:
        resimmed = resimulate(client, registry, begin_frame, current_frame, options, ResimScope::Predicted);
        break;
    case ReplicationRollbackPolicy::All:
    default:
        resimmed = resimulate_all(client, registry, begin_frame, current_frame, options);
        break;
    }
    if (!resimmed) {
        return false;
    }

    const SyncSettings& settings = registry.get<SyncSettings>();
    {
        ASHIATO_SYNC_PROFILE_SCOPE("AshiatoSync_ClientFinishCueResimulation");
        if (!client.cue_runtime_->finish_resimulation(
                client, registry, settings, begin_frame, current_frame, rollback_entity_indices_scratch_)) {
            return false;
        }
    }
    {
        ASHIATO_SYNC_PROFILE_SCOPE("AshiatoSync_ClientBlendResimErrors");
        if (!blend_resim_errors(
                client,
                registry,
                settings,
                current_frame,
                rollback_original_current_scratch_)) {
            return false;
        }
    }

    has_pending_prediction_rollback_ = false;
    pending_prediction_rollback_frame_ = 0;
    client.entity_store_->clear_prediction_rollback_memberships();
    return true;
}

bool ClientPredictionRuntime::resimulate_all(
    ReplicationClient& client,
    ashiato::Registry& registry,
    SyncFrame begin_frame,
    SyncFrame current_frame,
    const ashiato::RunJobsOptions& options) {
    return resimulate(client, registry, begin_frame, current_frame, options, ResimScope::All);
}

bool ClientPredictionRuntime::resimulate_affected(
    ReplicationClient& client,
    ashiato::Registry& registry,
    SyncFrame begin_frame,
    SyncFrame current_frame,
    const ashiato::RunJobsOptions& options) {
    return resimulate(client, registry, begin_frame, current_frame, options, ResimScope::Affected);
}

bool ClientPredictionRuntime::resimulate(
    ReplicationClient& client,
    ashiato::Registry& registry,
    SyncFrame begin_frame,
    SyncFrame current_frame,
    const ashiato::RunJobsOptions& options,
    ResimScope scope) {
    ASHIATO_SYNC_PROFILE_SCOPE("AshiatoSync_ClientResimulate");

    const SyncSettings& settings = registry.get<SyncSettings>();
    bool has_entities_to_resimulate = true;
    {
        ASHIATO_SYNC_PROFILE_SCOPE("AshiatoSync_ClientPrepareResimulation");
        if (!prepare_resimulation(client, registry, settings, begin_frame, scope, has_entities_to_resimulate)) {
            return false;
        }
    }
    if (!has_entities_to_resimulate) {
        return true;
    }
    {
        ASHIATO_SYNC_PROFILE_SCOPE("AshiatoSync_ClientRollbackPreparedCallback");
        notify_rollback_prepared(client, registry, begin_frame, current_frame);
    }
    if (current_frame <= begin_frame) {
        return quantize_resimulated(client, registry, settings, current_frame, scope);
    }

    for (SyncFrame frame = begin_frame + 1U; frame <= current_frame; ++frame) {
        if (!run_resimulation_frame(client, registry, settings, frame, options, scope)) {
            return false;
        }
        if (frame == current_frame) {
            break;
        }
    }
    return true;
}

bool ClientPredictionRuntime::prepare_resimulation(
    ReplicationClient& client,
    ashiato::Registry& registry,
    const SyncSettings& settings,
    SyncFrame begin_frame,
    ResimScope scope,
    bool& has_entities_to_resimulate) {
    has_entities_to_resimulate = true;
    rollback_event_entities_scratch_.clear();
    if (scope == ResimScope::Affected) {
        rollback_affected_entities_scratch_.clear();
        rollback_affected_entities_scratch_.reserve(client.entity_store_->prediction_rollback_entity_indices().size());
    } else if (scope == ResimScope::Predicted) {
        rollback_affected_entities_scratch_.clear();
    }

    for (std::uint32_t entity_index : client.entity_store_->active_entity_indices()) {
        if (entity_index >= client.entity_store_->entity_count()) {
            continue;
        }
        EntityState& state = client.entity_store_->state_unchecked(entity_index);
        if (state.mode.current != ReplicationClientMode::Predict) {
            continue;
        }
        const bool affected_scope = scope == ResimScope::Affected;
        if (affected_scope && !state.prediction.rollback_pending) {
            continue;
        }
        const SyncFrame reset_frame = affected_scope ? state.prediction.rollback_frame : begin_frame;
        const QuantizedFrameData* baseline = client.find_baseline(state, reset_frame);
        if (affected_scope && baseline == nullptr) {
            baseline = client.find_baseline(state, begin_frame);
        }
        if (baseline != nullptr) {
            if (!client.apply_frame_data(registry, settings, state, reset_frame, true, *baseline)) {
                return false;
            }
        } else if (!affected_scope) {
            EntityFrameView predicted;
            if (predicted_frames_.view(entity_index, reset_frame, predicted) &&
                !client.apply_buffered_sample(registry, settings, state, predicted)) {
                return false;
            }
        }
        if (state.identity.local && registry.alive(state.identity.local)) {
            rollback_event_entities_scratch_.push_back(state.identity.local);
            if (scope != ResimScope::All) {
                rollback_affected_entities_scratch_.push_back(state.identity.local);
            }
        }
    }
    if (scope != ResimScope::All && rollback_affected_entities_scratch_.empty()) {
        has_entities_to_resimulate = false;
    }
    rollback_companion_indices_scratch_.clear();
    if (scope == ResimScope::Predicted && has_entities_to_resimulate &&
        client.options_.predicted_only_companions_handler) {
        rollback_companion_entities_scratch_.clear();
        client.options_.predicted_only_companions_handler(
            registry, rollback_affected_entities_scratch_, rollback_companion_entities_scratch_);
        for (const ashiato::Entity companion : rollback_companion_entities_scratch_) {
            EntityState* state = client.entity_store_->find_by_local_entity(companion);
            if (state == nullptr || state->mode.current != ReplicationClientMode::BufferedInterpolation ||
                !registry.alive(companion)) {
                continue;
            }
            const std::uint32_t entity_index = client.entity_store_->index_of(*state);
            if (std::find(rollback_companion_indices_scratch_.begin(), rollback_companion_indices_scratch_.end(),
                    entity_index) != rollback_companion_indices_scratch_.end()) {
                continue;
            }
            rollback_companion_indices_scratch_.push_back(entity_index);
            rollback_affected_entities_scratch_.push_back(companion);
        }
    }
    return true;
}

void ClientPredictionRuntime::notify_rollback_prepared(
    ReplicationClient& client,
    ashiato::Registry& registry,
    SyncFrame rollback_frame,
    SyncFrame resim_end_frame) const {
    if (!client.options_.rollback_prepared_handler) {
        return;
    }

    client.options_.rollback_prepared_handler(
        registry,
        ReplicationClientRollbackPreparedEvent{
            rollback_frame,
            resim_end_frame,
            client.options_.prediction.rollback_policy,
            rollback_event_entities_scratch_});
}

bool ClientPredictionRuntime::run_resimulation_frame(
    ReplicationClient& client,
    ashiato::Registry& registry,
    const SyncSettings& settings,
    SyncFrame frame,
    const ashiato::RunJobsOptions& options,
    ResimScope scope) {
    ASHIATO_SYNC_PROFILE_SCOPE("AshiatoSync_ClientRunResimulationFrame");

    if (scope != ResimScope::Affected) {
        ASHIATO_SYNC_PROFILE_SCOPE("AshiatoSync_ClientResimApplySnap");
        if (!apply_snap_resimulation_frame(client, registry, settings, frame)) {
            return false;
        }
    }
    // PredictedOnly holds every buffered entity where the last tick left it, but its companions.
    if (scope == ResimScope::Predicted && !rollback_companion_indices_scratch_.empty()) {
        ASHIATO_SYNC_PROFILE_SCOPE("AshiatoSync_ClientResimApplyCompanions");
        if (!client.buffered_runtime_->apply_frame_for(
                client,
                registry,
                buffered_resimulation_frame(client, frame),
                rollback_companion_indices_scratch_)) {
            return false;
        }
    }
    if (scope == ResimScope::All) {
        const SyncFrame buffered_frame = buffered_resimulation_frame(client, frame);
        {
            ASHIATO_SYNC_PROFILE_SCOPE("AshiatoSync_ClientResimApplyBuffered");
            if (!client.buffered_runtime_->apply_frame(
                    client,
                    registry,
                    buffered_frame,
                    MissingBufferedFramePolicy::AllowMissing)) {
                return false;
            }
        }
    }
    if (!client.apply_input_frame(registry, settings, frame)) {
        return false;
    }
    registry.write<FrameInfo>().frame = frame;
    {
        ASHIATO_SYNC_PROFILE_SCOPE("AshiatoSync_ClientResimJobGraph");
        if (scope != ResimScope::All) {
            client.resim_job_graph(registry).tick_for_entities(registry, rollback_affected_entities_scratch_, options);
        } else {
            client.resim_job_graph(registry).tick(registry, options);
        }
    }
    {
        ASHIATO_SYNC_PROFILE_SCOPE("AshiatoSync_ClientResimDrainCues");
        client.cue_runtime_->drain_emitted_prediction(client, registry, settings, frame, true);
    }
    ++resimulated_frame_count_;
    {
        ASHIATO_SYNC_PROFILE_SCOPE("AshiatoSync_ClientQuantizeResimulated");
        if (!quantize_resimulated(client, registry, settings, frame, scope)) {
            return false;
        }
    }
#ifdef ASHIATO_SYNC_ENABLE_TRACING
    client.trace_frame_components(
        registry,
        settings,
        frame,
        true,
        scope == ResimScope::Affected,
        ReplicationClient::TraceFrameComponentScope::Predicted);
#endif
    return true;
}

SyncFrame ClientPredictionRuntime::buffered_resimulation_frame(
    const ReplicationClient& client,
    SyncFrame predicted_frame) const noexcept {
    const SyncFrame predicted_now = client.clock_.predicted_frame();
    const SyncFrame buffered_now = client.clock_.buffered_frame();
    const SyncFrame prediction_to_buffered_offset = predicted_now > buffered_now
        ? predicted_now - buffered_now
        : 0U;
    return predicted_frame > prediction_to_buffered_offset ? predicted_frame - prediction_to_buffered_offset : 0U;
}

bool ClientPredictionRuntime::apply_snap_resimulation_frame(
    ReplicationClient& client,
    ashiato::Registry& registry,
    const SyncSettings& settings,
    SyncFrame frame) {
    for (std::uint32_t entity_index : client.entity_store_->active_entity_indices()) {
        if (entity_index >= client.entity_store_->entity_count()) {
            continue;
        }
        EntityState& state = client.entity_store_->state_unchecked(entity_index);
        if (state.mode.current != ReplicationClientMode::Snap) {
            continue;
        }
        const QuantizedFrameData* baseline = client.find_baseline(state, frame);
        if (baseline == nullptr) {
            continue;
        }
        if (!client.apply_frame_data(registry, settings, state, frame, true, *baseline)) {
            return false;
        }
    }
    return true;
}

bool ClientPredictionRuntime::quantize_resimulated(
    ReplicationClient& client,
    ashiato::Registry& registry,
    const SyncSettings& settings,
    SyncFrame frame,
    ResimScope scope) {
    for (std::uint32_t entity_index : client.entity_store_->active_entity_indices()) {
        if (entity_index >= client.entity_store_->entity_count()) {
            continue;
        }
        EntityState& state = client.entity_store_->state_unchecked(entity_index);
        if (state.mode.current != ReplicationClientMode::Predict) {
            continue;
        }
        if (scope == ResimScope::Affected && !state.prediction.rollback_pending) {
            continue;
        }
        if (!client.quantize_predicted_entity(
                registry,
                settings,
                state,
                frame,
                FrameWriteSource::ResimulatedFrame)) {
            return false;
        }
    }
    return true;
}

}  // namespace ashiato::sync::client_detail
