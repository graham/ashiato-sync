#pragma once

#include "client/state.hpp"
#include "client/store/frame_ring_store.hpp"

#include "ashiato/ashiato.hpp"

#include <cstddef>
#include <cstdint>
#include <vector>

namespace ashiato::sync {

class ReplicationClient;

namespace client_detail {

class ClientPredictionRuntime {
public:
    explicit ClientPredictionRuntime(std::size_t frame_capacity = 64);

    ClientFrameRingStore& frames() noexcept {
        return predicted_frames_;
    }

    const ClientFrameRingStore& frames() const noexcept {
        return predicted_frames_;
    }

    ClientFrameRingStore& presentation_frames() noexcept {
        return presentation_frames_;
    }

    const ClientFrameRingStore& presentation_frames() const noexcept {
        return presentation_frames_;
    }

    void reset_entity(std::uint32_t entity_index) noexcept;
    void ensure_entity(std::uint32_t entity_index);
    void clear_entity(std::uint32_t entity_index) noexcept;

    bool has_predicted_frame() const noexcept {
        return has_predicted_frame_;
    }

    SyncFrame last_predicted_frame() const noexcept {
        return last_predicted_frame_;
    }

    bool should_run_prediction_frame(SyncFrame frame) const noexcept {
        return has_predicted_frame_ && frame > last_predicted_frame_;
    }

    SyncFrame active_snap_lead() const noexcept {
        return active_prediction_snap_lead_frames_;
    }

    SyncFrame pending_catchup_frame() const noexcept {
        return pending_prediction_catchup_frame_;
    }

    SyncFrame pending_catchup_server_frame() const noexcept {
        return pending_prediction_catchup_server_frame_;
    }

    std::uint64_t predicted_frame_run_count() const noexcept {
        return predicted_frame_run_count_;
    }

    std::uint64_t rollback_queued_count() const noexcept {
        return rollback_queued_count_;
    }

    std::uint64_t rollback_applied_count() const noexcept {
        return rollback_applied_count_;
    }

    std::uint64_t resimulated_frame_count() const noexcept {
        return resimulated_frame_count_;
    }

    std::uint64_t prediction_seed_count() const noexcept {
        return prediction_seed_count_;
    }

    SyncFrame pending_rollback_frame() const noexcept {
        return pending_prediction_rollback_frame_;
    }

    bool has_pending_rollback() const noexcept {
        return has_pending_prediction_rollback_;
    }

    SyncFrame late_frames_since(SyncFrame frame) const noexcept {
        return last_predicted_frame_ > frame ? last_predicted_frame_ - frame : 0U;
    }

    void reset_predicted_frame() noexcept;
    void set_active_snap_lead(SyncFrame frames) noexcept;
    SyncFrame update_active_snap_lead_from_server_update(
        SyncFrame server_frame,
        bool clock_requested_prefill,
        SyncFrame target_prediction_lead_frames,
        SyncFrame current_prediction_lead_frames) noexcept;
    void schedule_catchup(SyncFrame server_frame, SyncFrame target_input_frame) noexcept;
    bool seed_first_authoritative_frame(
        ReplicationClient& client,
        ashiato::Registry& registry,
        const SyncSettings& settings,
        SyncFrame frame);
    bool seed_existing_authoritative_frame(
        ReplicationClient& client,
        ashiato::Registry& registry,
        const SyncSettings& settings,
        SyncFrame frame);
    void refresh_pending_rollback_frame(ReplicationClient& client) noexcept;
    void queue_rollback(ReplicationClient& client, EntityState& state, SyncFrame frame);
    bool run_frame(
        ReplicationClient& client,
        ashiato::Registry& registry,
        SyncFrame frame,
        const ashiato::RunJobsOptions& options);
    bool run_catchup(
        ReplicationClient& client,
        ashiato::Registry& registry,
        std::uint32_t predicted_steps_this_tick,
        const ashiato::RunJobsOptions& options);
    bool apply_pending_rollback(
        ReplicationClient& client,
        ashiato::Registry& registry,
        const ashiato::RunJobsOptions& options);

private:
    enum class ResimScope {
        All,
        Affected,
        Predicted,
    };

    bool resimulate_all(
        ReplicationClient& client,
        ashiato::Registry& registry,
        SyncFrame begin_frame,
        SyncFrame current_frame,
        const ashiato::RunJobsOptions& options);
    bool resimulate_affected(
        ReplicationClient& client,
        ashiato::Registry& registry,
        SyncFrame begin_frame,
        SyncFrame current_frame,
        const ashiato::RunJobsOptions& options);
    bool resimulate(
        ReplicationClient& client,
        ashiato::Registry& registry,
        SyncFrame begin_frame,
        SyncFrame current_frame,
        const ashiato::RunJobsOptions& options,
        ResimScope scope);
    bool prepare_resimulation(
        ReplicationClient& client,
        ashiato::Registry& registry,
        const SyncSettings& settings,
        SyncFrame begin_frame,
        ResimScope scope,
        bool& has_entities_to_resimulate);
    void notify_rollback_prepared(
        ReplicationClient& client,
        ashiato::Registry& registry,
        SyncFrame rollback_frame,
        SyncFrame resim_end_frame) const;
    bool run_resimulation_frame(
        ReplicationClient& client,
        ashiato::Registry& registry,
        const SyncSettings& settings,
        SyncFrame frame,
        const ashiato::RunJobsOptions& options,
        ResimScope scope);
    SyncFrame buffered_resimulation_frame(const ReplicationClient& client, SyncFrame predicted_frame) const noexcept;
    bool apply_snap_resimulation_frame(
        ReplicationClient& client,
        ashiato::Registry& registry,
        const SyncSettings& settings,
        SyncFrame frame);
    bool quantize_resimulated(
        ReplicationClient& client,
        ashiato::Registry& registry,
        const SyncSettings& settings,
        SyncFrame frame,
        ResimScope scope);
    void collect_resimulated_entities(ReplicationClient& client, std::vector<std::uint32_t>& out) const;
    void capture_original_current(
        ReplicationClient& client,
        SyncFrame current_frame,
        const std::vector<std::uint32_t>& entity_indices,
        std::vector<OriginalPredictionCapture>& out) const;
    bool blend_resim_errors(
        ReplicationClient& client,
        const ashiato::Registry& registry,
        const SyncSettings& settings,
        SyncFrame current_frame,
        const std::vector<OriginalPredictionCapture>& original);

    std::vector<std::uint32_t> rollback_entity_indices_scratch_;
    std::vector<ashiato::Entity> rollback_affected_entities_scratch_;
    // PredictedOnly: the buffered entities replayed with the predicted ones (predicted_only_companions_handler).
    std::vector<ashiato::Entity> rollback_companion_entities_scratch_;
    std::vector<std::uint32_t> rollback_companion_indices_scratch_;
    std::vector<ashiato::Entity> rollback_event_entities_scratch_;
    std::vector<OriginalPredictionCapture> rollback_original_current_scratch_;
    ClientFrameRingStore predicted_frames_;
    ClientFrameRingStore presentation_frames_;
    SyncFrame last_predicted_frame_ = 0;
    bool has_predicted_frame_ = false;
    SyncFrame active_prediction_snap_lead_frames_ = 0;
    SyncFrame pending_prediction_catchup_frame_ = 0;
    SyncFrame pending_prediction_catchup_server_frame_ = 0;
    SyncFrame pending_prediction_rollback_frame_ = 0;
    bool has_pending_prediction_rollback_ = false;
    std::uint64_t predicted_frame_run_count_ = 0;
    std::uint64_t rollback_queued_count_ = 0;
    std::uint64_t rollback_applied_count_ = 0;
    std::uint64_t resimulated_frame_count_ = 0;
    std::uint64_t prediction_seed_count_ = 0;
};

}  // namespace client_detail
}  // namespace ashiato::sync
