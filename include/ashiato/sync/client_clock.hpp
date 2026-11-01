#pragma once

#include "ashiato/sync/types.hpp"

#include <cstddef>
#include <cstdint>

namespace ashiato::sync {

struct ReplicationClientTimingStats {
    std::uint64_t sample_count = 0;
    float latency_frames = 0.0f;
    float jitter_frames = 0.0f;
    float measured_buffered_frame_lag = 0.0f;
    SyncFrame desired_buffered_frame_lag = 0;
    SyncFrame target_buffered_frame_lag = 0;
    SyncFrame current_buffered_frame_lag = 0;
    float measured_prediction_lead_frames = 0.0f;
    SyncFrame desired_prediction_lead_frames = 0;
    SyncFrame target_prediction_lead_frames = 0;
    SyncFrame current_prediction_lead_frames = 0;
    float buffered_time_dilation = 1.0f;
    float predicted_time_dilation = 1.0f;
    std::uint64_t server_update_packets_received = 0;
    std::uint64_t server_update_packets_missing = 0;
    std::uint64_t server_update_packets_reordered_or_duplicate = 0;
    std::uint64_t dropped_receive_frames = 0;
    std::uint64_t dropped_playback_frames = 0;
    std::uint64_t dropped_input_frames = 0;
    float server_update_packet_loss = 0.0f;
};

struct ReplicationClientClockConfig {
    double fixed_dt_seconds = 1.0 / 60.0;
    SyncFrame buffered_frame_lag = 2;
    std::size_t buffered_frame_lag_capacity = 64;
    bool auto_buffered_frame_lag = true;
    SyncFrame auto_buffered_frame_lag_min = 2;
    float auto_buffered_frame_lag_jitter_multiplier = 2.0f;
    float auto_buffered_frame_lag_smoothing = 0.1f;
    float auto_buffered_time_dilation_min = 0.95f;
    float auto_buffered_time_dilation_max = 1.05f;
    float auto_buffered_time_dilation_gain = 0.05f;
    bool auto_prediction_lead_frames = true;
    SyncFrame prediction_lead_frames = 2;
    std::size_t input_buffer_capacity_frames = 64;
    SyncFrame auto_prediction_min_frames = 1;
    SyncFrame auto_prediction_safety_frames = 1;
    float auto_prediction_jitter_multiplier = 2.0f;
    float auto_predicted_time_dilation_min = 0.95f;
    float auto_predicted_time_dilation_max = 1.10f;
    float auto_predicted_time_dilation_gain = 0.05f;
    std::uint32_t auto_timing_warmup_samples = 3;
    bool auto_timing_fast_recovery = true;
    SyncFrame auto_timing_fast_recovery_min_frame_gap = 4;
    std::uint32_t max_fixed_steps_per_tick = 0;
};

class ReplicationClientClock {
public:
    struct FrameRange {
        SyncFrame first = 0;
        SyncFrame last = 0;

        bool empty() const noexcept {
            return first == 0 && last == 0;
        }
    };

    struct AdvanceResult {
        FrameRange buffered;
        FrameRange predicted;
    };

    explicit ReplicationClientClock(const ReplicationClientClockConfig& config = {});

    const ReplicationClientClockConfig& config() const noexcept {
        return config_;
    }
    const ReplicationClientTimingStats& stats() const noexcept {
        return stats_;
    }
    ReplicationClientTimingStats& mutable_stats() noexcept {
        return stats_;
    }
    /// Sets the prediction safety margin the auto targets add. A whole frame of safety is a whole frame of target, so the desired and
    /// target prediction leads move by the same delta (clamped as every target is); the next ping sample recomputes them from the new value.
    void set_auto_prediction_safety_frames(SyncFrame frames) noexcept;

    bool bootstrapped() const noexcept {
        return bootstrapped_;
    }
    SyncFrame buffered_frame() const noexcept {
        return buffered_frame_;
    }
    SyncFrame predicted_frame() const noexcept {
        return predicted_frame_;
    }
    SyncFrame buffered_frame_lag() const noexcept {
        return buffered_frame_lag_;
    }
    float buffered_time_dilation() const noexcept {
        return stats_.buffered_time_dilation;
    }
    float predicted_time_dilation() const noexcept {
        return stats_.predicted_time_dilation;
    }
    double display_accumulator_seconds() const noexcept {
        return display_accumulator_seconds_;
    }
    double consume_display_accumulator_seconds() noexcept;
    double predicted_accumulator_seconds() const noexcept {
        return predicted_accumulator_seconds_;
    }
    double local_time_seconds() const noexcept {
        return local_time_seconds_;
    }
    double estimated_server_time_seconds() const noexcept;
    double estimated_server_frame() const noexcept;
    double buffered_frame_for_estimated_server_frame(double estimated_server_frame) const noexcept;
    double buffered_subframe() const noexcept {
        return buffered_accumulator_seconds_ / config_.fixed_dt_seconds;
    }
    double predicted_subframe() const noexcept {
        return predicted_accumulator_seconds_ / config_.fixed_dt_seconds;
    }
    double continuous_buffered_frame() const noexcept {
        return static_cast<double>(buffered_frame_) + buffered_subframe();
    }
    double continuous_predicted_frame() const noexcept {
        return static_cast<double>(predicted_frame_) + predicted_subframe();
    }
    double display_target_frame() const noexcept {
        return display_target_frame_;
    }

    AdvanceResult advance(double dt_seconds) noexcept;
    void advance_local_time(double dt_seconds) noexcept;
    AdvanceResult advance_client_frame_numbers(double dt_seconds) noexcept;
    void advance_predicted_frame_to(SyncFrame predicted_frame) noexcept;
    bool maybe_bootstrap_from_first_server_update(
        SyncFrame server_frame,
        bool seed_prediction_lead = true,
        bool complete_warmup = false) noexcept;
    void record_time_sync_sample(
        double client_send_time_seconds,
        double server_receive_time_seconds,
        double server_send_time_seconds,
        double client_receive_time_seconds) noexcept;
    SyncFrame record_server_update(
        SyncFrame server_frame,
        bool has_buffered_entities,
        SyncFrame last_recorded_input_frame) noexcept;
    SyncFrame record_server_update(
        SyncFrame server_frame,
        SyncFrame observed_server_frame,
        SyncFrame observed_buffered_frame,
        bool has_buffered_entities,
        SyncFrame last_recorded_input_frame) noexcept;
    SyncFrame record_server_update(
        SyncFrame server_frame,
        double observed_server_frame,
        double observed_buffered_frame,
        double observed_predicted_frame,
        bool has_buffered_entities) noexcept;
    SyncFrame record_server_update(
        SyncFrame server_frame,
        double observed_server_frame,
        double observed_buffered_frame,
        double observed_predicted_frame,
        bool has_buffered_entities,
        SyncFrame last_recorded_input_frame) noexcept;
    void record_prediction_lead(SyncFrame server_frame, SyncFrame predicted_frame) noexcept;
    bool set_buffered_frame_lag(SyncFrame frames) noexcept;
    void update_display_target(double dt_seconds) noexcept;

private:
    void record_ping_sample(float sample) noexcept;
    void compute_auto_targets() noexcept;
    double time_sync_reconfigure_threshold_frames() const noexcept;
    void reconfigure_frames_from_estimated_server_time() noexcept;
    double observed_downstream_frames(SyncFrame server_frame, double observed_server_frame) const noexcept;
    SyncFrame fast_recovery_step(SyncFrame current, SyncFrame target) const noexcept;
    SyncFrame prediction_target_from_observed(double observed_downstream) const noexcept;
    void record_buffered_timing(
        SyncFrame server_frame,
        double observed_server_frame,
        double observed_buffered_frame,
        bool has_buffered_entities) noexcept;
    void record_prediction_timing(
        SyncFrame server_frame,
        double observed_downstream,
        double observed_predicted_frame,
        SyncFrame last_recorded_input_frame) noexcept;
    SyncFrame clamp_buffered_lag_target(SyncFrame frames) const noexcept;
    SyncFrame clamp_prediction_target(SyncFrame frames) const noexcept;

    ReplicationClientClockConfig config_;
    ReplicationClientTimingStats stats_;
    double local_time_seconds_ = 0.0;
    double server_time_offset_seconds_ = 0.0;
    double buffered_accumulator_seconds_ = 0.0;
    double predicted_accumulator_seconds_ = 0.0;
    double display_accumulator_seconds_ = 0.0;
    double display_target_frame_ = 0.0;
    SyncFrame buffered_frame_ = 0;
    SyncFrame predicted_frame_ = 0;
    SyncFrame buffered_frame_lag_ = 0;
    std::uint32_t warmup_samples_ = 0;
    bool bootstrapped_ = false;
    bool has_display_target_frame_ = false;
    bool has_time_sync_sample_ = false;
};

}  // namespace ashiato::sync
