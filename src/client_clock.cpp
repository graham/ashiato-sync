#include "ashiato/sync/client_clock.hpp"

#include "detail/options_validation.hpp"

#include <algorithm>
#include <cmath>
#include <limits>

namespace ashiato::sync {

namespace {

struct ClientFixedStepAdvance {
    SyncFrame steps = 0;
    std::uint64_t dropped_steps = 0;
};

float dilation_from_error(float error, float gain, float min_dilation, float max_dilation) noexcept {
    constexpr float deadband_frames = 0.25f;
    if (std::fabs(error) <= deadband_frames) {
        return 1.0f;
    }
    const float unclamped = 1.0f + error * gain;
    return std::min(max_dilation, std::max(min_dilation, unclamped));
}

SyncFrame rounded_frame_count(double frames) noexcept {
    if (!std::isfinite(frames) || frames <= 0.0) {
        return 0;
    }
    return static_cast<SyncFrame>(std::round(frames));
}

ClientFixedStepAdvance consume_client_fixed_steps(
    double& accumulator_seconds,
    double dt_seconds,
    double fixed_dt_seconds,
    std::uint32_t max_fixed_steps_per_tick) noexcept {
    accumulator_seconds += dt_seconds;
    if (accumulator_seconds < fixed_dt_seconds) {
        return {};
    }

    const double whole_step_count = std::floor(accumulator_seconds / fixed_dt_seconds);
    if (!std::isfinite(whole_step_count) || whole_step_count <= 0.0) {
        accumulator_seconds = 0.0;
        return {};
    }

    const double remainder = accumulator_seconds - whole_step_count * fixed_dt_seconds;
    const double clamped_remainder = remainder > 0.0 && std::isfinite(remainder) ? remainder : 0.0;
    const std::uint64_t whole_steps = whole_step_count >= static_cast<double>(std::numeric_limits<std::uint64_t>::max())
        ? std::numeric_limits<std::uint64_t>::max()
        : static_cast<std::uint64_t>(whole_step_count);
    const std::uint64_t max_steps = max_fixed_steps_per_tick == 0U
        ? whole_steps
        : std::min<std::uint64_t>(whole_steps, max_fixed_steps_per_tick);
    accumulator_seconds = clamped_remainder;
    return ClientFixedStepAdvance{
        static_cast<SyncFrame>(std::min<std::uint64_t>(max_steps, std::numeric_limits<SyncFrame>::max())),
        whole_steps - max_steps};
}

}  // namespace

ReplicationClientClock::ReplicationClientClock(const ReplicationClientClockConfig& config)
    : config_(detail::validate_client_clock_config(config)),
      buffered_frame_lag_(config.buffered_frame_lag) {
    stats_.desired_buffered_frame_lag = config_.buffered_frame_lag;
    stats_.target_buffered_frame_lag = config_.buffered_frame_lag;
    stats_.current_buffered_frame_lag = config_.buffered_frame_lag;
    stats_.desired_prediction_lead_frames = config_.prediction_lead_frames;
    stats_.target_prediction_lead_frames = config_.prediction_lead_frames;
    stats_.current_prediction_lead_frames = config_.prediction_lead_frames;
}

ReplicationClientClock::AdvanceResult ReplicationClientClock::advance(double dt_seconds) noexcept {
    AdvanceResult result;
    advance_local_time(dt_seconds);
    const AdvanceResult client_frames = advance_client_frame_numbers(dt_seconds);
    result.buffered = client_frames.buffered;
    result.predicted = client_frames.predicted;
    return result;
}

void ReplicationClientClock::advance_local_time(double dt_seconds) noexcept {
    if (dt_seconds < 0.0 || !std::isfinite(dt_seconds)) {
        return;
    }
    local_time_seconds_ += dt_seconds;
    display_accumulator_seconds_ += dt_seconds;
}

double ReplicationClientClock::estimated_server_time_seconds() const noexcept {
    return local_time_seconds_ + server_time_offset_seconds_;
}

double ReplicationClientClock::estimated_server_frame() const noexcept {
    return estimated_server_time_seconds() / config_.fixed_dt_seconds;
}

double ReplicationClientClock::buffered_frame_for_estimated_server_frame(double estimated_server_frame) const noexcept {
    if (!std::isfinite(estimated_server_frame)) {
        return 0.0;
    }
    return std::max(0.0, estimated_server_frame - static_cast<double>(buffered_frame_lag_));
}

ReplicationClientClock::AdvanceResult ReplicationClientClock::advance_client_frame_numbers(double dt_seconds) noexcept {
    AdvanceResult result;
    if (dt_seconds < 0.0 || !std::isfinite(dt_seconds)) {
        return result;
    }

    if (!bootstrapped_) {
        return result;
    }

    const SyncFrame previous_buffered_frame = buffered_frame_;
    const double continuous_buffered_frame = buffered_frame_for_estimated_server_frame(estimated_server_frame());
    const auto next_buffered_frame = static_cast<SyncFrame>(std::floor(continuous_buffered_frame));
    buffered_frame_ = next_buffered_frame;
    buffered_accumulator_seconds_ =
        (continuous_buffered_frame - static_cast<double>(buffered_frame_)) * config_.fixed_dt_seconds;
    if (buffered_frame_ > previous_buffered_frame) {
        result.buffered.first = previous_buffered_frame + 1U;
        result.buffered.last = buffered_frame_;
    }

    const SyncFrame previous_predicted_frame = predicted_frame_;
    const ClientFixedStepAdvance predicted_advance = consume_client_fixed_steps(
        predicted_accumulator_seconds_,
        dt_seconds * static_cast<double>(stats_.predicted_time_dilation),
        config_.fixed_dt_seconds,
        config_.max_fixed_steps_per_tick);
    predicted_frame_ += predicted_advance.steps;
    stats_.dropped_input_frames += predicted_advance.dropped_steps;
    if (predicted_frame_ != previous_predicted_frame) {
        result.predicted.first = previous_predicted_frame + 1U;
        result.predicted.last = predicted_frame_;
    }
    return result;
}

void ReplicationClientClock::advance_predicted_frame_to(SyncFrame predicted_frame) noexcept {
    if (predicted_frame > predicted_frame_) {
        predicted_frame_ = predicted_frame;
        predicted_accumulator_seconds_ = 0.0;
    }
}

bool ReplicationClientClock::maybe_bootstrap_from_first_server_update(
    SyncFrame server_frame,
    bool seed_prediction_lead,
    bool complete_warmup) noexcept {
    if (bootstrapped_) {
        return false;
    }
    bool anchor_to_update = !has_time_sync_sample_;
    if (has_time_sync_sample_) {
        const double estimated_frame = estimated_server_frame();
        const double frame_delta = std::fabs(estimated_frame - static_cast<double>(server_frame));
        anchor_to_update = !std::isfinite(estimated_frame) ||
            frame_delta >= time_sync_reconfigure_threshold_frames();
    }
    if (anchor_to_update) {
        server_time_offset_seconds_ =
            static_cast<double>(server_frame) * config_.fixed_dt_seconds - local_time_seconds_;
    }
    const double continuous_buffered_frame = buffered_frame_for_estimated_server_frame(estimated_server_frame());
    buffered_frame_ = static_cast<SyncFrame>(std::floor(continuous_buffered_frame));
    predicted_frame_ = seed_prediction_lead ? server_frame + config_.prediction_lead_frames : server_frame;
    buffered_accumulator_seconds_ =
        (continuous_buffered_frame - static_cast<double>(buffered_frame_)) * config_.fixed_dt_seconds;
    predicted_accumulator_seconds_ = 0.0;
    display_accumulator_seconds_ = 0.0;
    display_target_frame_ = continuous_buffered_frame;
    has_display_target_frame_ = true;
    stats_.current_prediction_lead_frames = seed_prediction_lead && predicted_frame_ >= server_frame
        ? predicted_frame_ - server_frame
        : config_.prediction_lead_frames;
    if (complete_warmup || !seed_prediction_lead) {
        warmup_samples_ = config_.auto_timing_warmup_samples;
    }
    bootstrapped_ = true;
    return true;
}

void ReplicationClientClock::record_time_sync_sample(
    double client_send_time_seconds,
    double server_receive_time_seconds,
    double server_send_time_seconds,
    double client_receive_time_seconds) noexcept {
    if (!std::isfinite(client_send_time_seconds) ||
        !std::isfinite(server_receive_time_seconds) ||
        !std::isfinite(server_send_time_seconds) ||
        !std::isfinite(client_receive_time_seconds) ||
        client_receive_time_seconds < client_send_time_seconds ||
        server_send_time_seconds < server_receive_time_seconds) {
        return;
    }
    const double round_trip_seconds =
        (client_receive_time_seconds - client_send_time_seconds) -
        (server_send_time_seconds - server_receive_time_seconds);
    if (round_trip_seconds < 0.0) {
        return;
    }
    const double sample_offset_seconds =
        ((server_receive_time_seconds - client_send_time_seconds) +
         (server_send_time_seconds - client_receive_time_seconds)) *
        0.5;
    const double previous_estimated_frame = estimated_server_frame();
    const double sample_estimated_frame =
        (client_receive_time_seconds + sample_offset_seconds) / config_.fixed_dt_seconds;
    const double frame_delta = std::fabs(sample_estimated_frame - previous_estimated_frame);
    const float sample_latency_frames = static_cast<float>(round_trip_seconds * 0.5 / config_.fixed_dt_seconds);
    record_ping_sample(sample_latency_frames);
    const bool reconfigure_from_sample = bootstrapped_ &&
        (!std::isfinite(previous_estimated_frame) ||
         frame_delta >= time_sync_reconfigure_threshold_frames());
    if (!has_time_sync_sample_) {
        server_time_offset_seconds_ = sample_offset_seconds;
        has_time_sync_sample_ = true;
    } else if (reconfigure_from_sample) {
        server_time_offset_seconds_ = sample_offset_seconds;
    } else {
        const double smoothing = static_cast<double>(config_.auto_buffered_frame_lag_smoothing);
        server_time_offset_seconds_ += (sample_offset_seconds - server_time_offset_seconds_) * smoothing;
    }
    if (reconfigure_from_sample) {
        reconfigure_frames_from_estimated_server_time();
    }
}

SyncFrame ReplicationClientClock::record_server_update(
    SyncFrame server_frame,
    bool has_buffered_entities,
    SyncFrame last_recorded_input_frame) noexcept {
    return record_server_update(
        server_frame,
        estimated_server_frame(),
        continuous_buffered_frame(),
        static_cast<double>(last_recorded_input_frame),
        has_buffered_entities);
}

SyncFrame ReplicationClientClock::record_server_update(
    SyncFrame server_frame,
    SyncFrame observed_server_frame,
    SyncFrame observed_buffered_frame,
    bool has_buffered_entities,
    SyncFrame last_recorded_input_frame) noexcept {
    if (!bootstrapped_) {
        return 0;
    }
    return record_server_update(
        server_frame,
        static_cast<double>(observed_server_frame),
        static_cast<double>(observed_buffered_frame),
        static_cast<double>(last_recorded_input_frame),
        has_buffered_entities);
}

SyncFrame ReplicationClientClock::record_server_update(
    SyncFrame server_frame,
    double observed_server_frame,
    double observed_buffered_frame,
    double observed_predicted_frame,
    bool has_buffered_entities) noexcept {
    return record_server_update(
        server_frame,
        observed_server_frame,
        observed_buffered_frame,
        observed_predicted_frame,
        has_buffered_entities,
        rounded_frame_count(observed_predicted_frame));
}

SyncFrame ReplicationClientClock::record_server_update(
    SyncFrame server_frame,
    double observed_server_frame,
    double observed_buffered_frame,
    double observed_predicted_frame,
    bool has_buffered_entities,
    SyncFrame last_recorded_input_frame) noexcept {
    if (!bootstrapped_ ||
        !std::isfinite(observed_server_frame) ||
        !std::isfinite(observed_buffered_frame) ||
        !std::isfinite(observed_predicted_frame)) {
        return 0;
    }
    const double observed_downstream = observed_downstream_frames(server_frame, observed_server_frame);
    record_buffered_timing(server_frame, observed_server_frame, observed_buffered_frame, has_buffered_entities);
    record_prediction_timing(server_frame, observed_downstream, observed_predicted_frame, last_recorded_input_frame);
    if (!config_.auto_timing_fast_recovery ||
        !config_.auto_prediction_lead_frames ||
        observed_downstream <= 0.0) {
        return 0;
    }
    const SyncFrame target = prediction_target_from_observed(observed_downstream);
    const SyncFrame min_gap = config_.auto_timing_fast_recovery_min_frame_gap;
    const SyncFrame available_lead = last_recorded_input_frame >= server_frame
        ? last_recorded_input_frame - server_frame
        : 0U;
    if (observed_downstream < static_cast<double>(min_gap) ||
        target <= available_lead) {
        return 0;
    }
    return server_frame + target;
}

void ReplicationClientClock::record_prediction_lead(SyncFrame server_frame, SyncFrame predicted_frame) noexcept {
    const SyncFrame measured = predicted_frame >= server_frame ? predicted_frame - server_frame : 0U;
    stats_.measured_prediction_lead_frames = static_cast<float>(measured);
    stats_.current_prediction_lead_frames = measured;
    const SyncFrame target = config_.auto_prediction_lead_frames
        ? stats_.target_prediction_lead_frames
        : config_.prediction_lead_frames;
    if (!config_.auto_prediction_lead_frames || warmup_samples_ < config_.auto_timing_warmup_samples) {
        stats_.predicted_time_dilation = measured == target ? 1.0f : stats_.predicted_time_dilation;
        return;
    }
    const float error = static_cast<float>(target) - static_cast<float>(measured);
    const float unclamped = 1.0f + error * config_.auto_predicted_time_dilation_gain;
    stats_.predicted_time_dilation = std::min(
        config_.auto_predicted_time_dilation_max,
        std::max(config_.auto_predicted_time_dilation_min, unclamped));
}

bool ReplicationClientClock::set_buffered_frame_lag(SyncFrame frames) noexcept {
    if (frames >= config_.buffered_frame_lag_capacity) {
        return false;
    }
    config_.buffered_frame_lag = frames;
    buffered_frame_lag_ = frames;
    stats_.desired_buffered_frame_lag = frames;
    stats_.target_buffered_frame_lag = frames;
    stats_.current_buffered_frame_lag = frames;
    stats_.buffered_time_dilation = 1.0f;
    return true;
}

void ReplicationClientClock::update_display_target(double dt_seconds) noexcept {
    (void)dt_seconds;
    if (!bootstrapped_) {
        return;
    }
    display_target_frame_ = continuous_buffered_frame();
    has_display_target_frame_ = true;
}

double ReplicationClientClock::consume_display_accumulator_seconds() noexcept {
    const double value = display_accumulator_seconds_;
    display_accumulator_seconds_ = 0.0;
    return value;
}

void ReplicationClientClock::record_ping_sample(float sample) noexcept {
    if (stats_.sample_count == 0) {
        stats_.latency_frames = sample;
        stats_.jitter_frames = 0.0f;
    } else {
        const float smoothing = config_.auto_buffered_frame_lag_smoothing;
        const float previous_latency = stats_.latency_frames;
        stats_.latency_frames += (sample - stats_.latency_frames) * smoothing;
        const float deviation = std::fabs(sample - previous_latency);
        stats_.jitter_frames += (deviation - stats_.jitter_frames) * smoothing;
    }
    ++stats_.sample_count;
    if (warmup_samples_ < config_.auto_timing_warmup_samples) {
        ++warmup_samples_;
    }
    if (warmup_samples_ >= config_.auto_timing_warmup_samples) {
        compute_auto_targets();
    }
}

void ReplicationClientClock::compute_auto_targets() noexcept {
    const float wanted = stats_.latency_frames +
        config_.auto_buffered_frame_lag_jitter_multiplier * stats_.jitter_frames;
    const SyncFrame target = clamp_buffered_lag_target(
        static_cast<SyncFrame>(std::ceil(std::max(0.0f, wanted))));
    stats_.desired_buffered_frame_lag = target;
    stats_.target_buffered_frame_lag = target;

    const float prediction_wanted = 2.0f * (
        stats_.latency_frames + config_.auto_prediction_jitter_multiplier * stats_.jitter_frames) +
        static_cast<float>(config_.auto_prediction_safety_frames);
    const SyncFrame prediction_target = clamp_prediction_target(
        static_cast<SyncFrame>(std::ceil(std::max(0.0f, prediction_wanted))));
    stats_.desired_prediction_lead_frames = prediction_target;
    stats_.target_prediction_lead_frames = prediction_target;
}

void ReplicationClientClock::set_auto_prediction_safety_frames(SyncFrame frames) noexcept {
    const auto delta = static_cast<std::int64_t>(frames) - static_cast<std::int64_t>(config_.auto_prediction_safety_frames);
    config_.auto_prediction_safety_frames = frames;
    const auto shifted = [&](SyncFrame target) {
        const std::int64_t moved = std::max<std::int64_t>(0, static_cast<std::int64_t>(target) + delta);
        return clamp_prediction_target(static_cast<SyncFrame>(moved));
    };
    stats_.desired_prediction_lead_frames = shifted(stats_.desired_prediction_lead_frames);
    stats_.target_prediction_lead_frames = shifted(stats_.target_prediction_lead_frames);
}

double ReplicationClientClock::time_sync_reconfigure_threshold_frames() const noexcept {
    return static_cast<double>(
        stats_.latency_frames +
        stats_.jitter_frames * 2.0f +
        static_cast<float>(config_.auto_timing_fast_recovery_min_frame_gap));
}

void ReplicationClientClock::reconfigure_frames_from_estimated_server_time() noexcept {
    const double estimated_frame = estimated_server_frame();
    if (!std::isfinite(estimated_frame)) {
        return;
    }
    const double continuous_buffered = buffered_frame_for_estimated_server_frame(estimated_frame);
    buffered_frame_ = static_cast<SyncFrame>(std::floor(continuous_buffered));
    buffered_accumulator_seconds_ =
        (continuous_buffered - static_cast<double>(buffered_frame_)) * config_.fixed_dt_seconds;

    const SyncFrame server_frame = rounded_frame_count(std::max(0.0, estimated_frame));
    const SyncFrame prediction_lead = config_.auto_prediction_lead_frames
        ? stats_.target_prediction_lead_frames
        : config_.prediction_lead_frames;
    predicted_frame_ = server_frame + prediction_lead;
    predicted_accumulator_seconds_ = 0.0;
    display_accumulator_seconds_ = 0.0;
    display_target_frame_ = continuous_buffered;
    has_display_target_frame_ = true;
    stats_.current_prediction_lead_frames = prediction_lead;
}

double ReplicationClientClock::observed_downstream_frames(
    SyncFrame server_frame,
    double observed_server_frame) const noexcept {
    return observed_server_frame > static_cast<double>(server_frame)
        ? observed_server_frame - static_cast<double>(server_frame)
        : 0.0;
}

SyncFrame ReplicationClientClock::fast_recovery_step(SyncFrame current, SyncFrame target) const noexcept {
    if (current == target) {
        return current;
    }
    if (!config_.auto_timing_fast_recovery) {
        return current < target ? current + 1U : current - 1U;
    }
    const SyncFrame delta = current < target ? target - current : current - target;
    if (delta < config_.auto_timing_fast_recovery_min_frame_gap) {
        return current < target ? current + 1U : current - 1U;
    }
    return target;
}

SyncFrame ReplicationClientClock::prediction_target_from_observed(double observed_downstream) const noexcept {
    const float ping_upstream = warmup_samples_ >= config_.auto_timing_warmup_samples
        ? stats_.latency_frames + config_.auto_prediction_jitter_multiplier * stats_.jitter_frames
        : static_cast<float>(config_.prediction_lead_frames);
    const float downstream = static_cast<float>(std::max(0.0, observed_downstream));
    const float upstream = std::max(downstream, ping_upstream);
    const float wanted = downstream + upstream +
        static_cast<float>(config_.auto_prediction_safety_frames);
    return clamp_prediction_target(static_cast<SyncFrame>(std::ceil(std::max(0.0f, wanted))));
}

void ReplicationClientClock::record_buffered_timing(
    SyncFrame server_frame,
    double observed_server_frame,
    double observed_buffered_frame,
    bool has_buffered_entities) noexcept {
    const double observed_downstream = observed_downstream_frames(server_frame, observed_server_frame);
    if (config_.auto_buffered_frame_lag) {
        SyncFrame target = stats_.desired_buffered_frame_lag;
        if (config_.auto_timing_fast_recovery && observed_downstream > 0.0) {
            target = std::max(target, clamp_buffered_lag_target(rounded_frame_count(observed_downstream)));
        } else if (warmup_samples_ >= config_.auto_timing_warmup_samples && observed_downstream > 0.0) {
            target = std::max(
                stats_.target_buffered_frame_lag,
                clamp_buffered_lag_target(rounded_frame_count(observed_downstream)));
        }
        stats_.target_buffered_frame_lag = target;
    }
    const SyncFrame target = stats_.target_buffered_frame_lag;
    const SyncFrame current = buffered_frame_lag_;
    const float measured = static_cast<float>(
        std::max(0.0, static_cast<double>(server_frame) - observed_buffered_frame));
    stats_.measured_buffered_frame_lag = measured;

    if (has_buffered_entities &&
        config_.auto_buffered_frame_lag &&
        (warmup_samples_ >= config_.auto_timing_warmup_samples ||
         (config_.auto_timing_fast_recovery && observed_downstream > 0.0))) {
        const float error = measured - static_cast<float>(target);
        stats_.buffered_time_dilation = dilation_from_error(
            error,
            config_.auto_buffered_time_dilation_gain,
            config_.auto_buffered_time_dilation_min,
            config_.auto_buffered_time_dilation_max);

        const SyncFrame min_gap = config_.auto_timing_fast_recovery_min_frame_gap;
        const bool large_fast_recovery_gap = config_.auto_timing_fast_recovery &&
            ((current < target && target - current >= min_gap) ||
             (current > target && current - target >= min_gap));
        const bool should_increase_lag = current < target &&
            (large_fast_recovery_gap || observed_downstream > 0.0 || measured >= static_cast<float>(current));
        const bool should_decrease_lag =
            current > target && (large_fast_recovery_gap || measured <= static_cast<float>(current));
        if (should_increase_lag || should_decrease_lag) {
            buffered_frame_lag_ = fast_recovery_step(current, target);
        }
        stats_.current_buffered_frame_lag = buffered_frame_lag_;
    } else {
        stats_.current_buffered_frame_lag = buffered_frame_lag_;
        stats_.buffered_time_dilation = 1.0f;
    }
}

void ReplicationClientClock::record_prediction_timing(
    SyncFrame server_frame,
    double observed_downstream,
    double observed_predicted_frame,
    SyncFrame last_recorded_input_frame) noexcept {
    const double measured_double = std::max(0.0, observed_predicted_frame - static_cast<double>(server_frame));
    const float measured = static_cast<float>(measured_double);
    const SyncFrame available_lead = last_recorded_input_frame >= server_frame
        ? last_recorded_input_frame - server_frame
        : 0U;
    stats_.measured_prediction_lead_frames = measured;
    stats_.current_prediction_lead_frames = std::max(rounded_frame_count(measured_double), available_lead);
    if (config_.auto_prediction_lead_frames &&
        config_.auto_timing_fast_recovery) {
        const SyncFrame observed_target = observed_downstream > 0.0
            ? prediction_target_from_observed(observed_downstream)
            : stats_.desired_prediction_lead_frames;
        stats_.target_prediction_lead_frames = std::max(stats_.desired_prediction_lead_frames, observed_target);
    }
    const SyncFrame target = config_.auto_prediction_lead_frames
        ? stats_.target_prediction_lead_frames
        : config_.prediction_lead_frames;
    if (warmup_samples_ < config_.auto_timing_warmup_samples) {
        stats_.predicted_time_dilation = 1.0f;
        return;
    }
    if (config_.auto_prediction_lead_frames) {
        const float error = static_cast<float>(target) - static_cast<float>(measured);
        stats_.predicted_time_dilation = dilation_from_error(
            error,
            config_.auto_predicted_time_dilation_gain,
            config_.auto_predicted_time_dilation_min,
            config_.auto_predicted_time_dilation_max);
    } else {
        stats_.desired_prediction_lead_frames = config_.prediction_lead_frames;
        stats_.target_prediction_lead_frames = config_.prediction_lead_frames;
        stats_.predicted_time_dilation = 1.0f;
    }
}

SyncFrame ReplicationClientClock::clamp_buffered_lag_target(SyncFrame frames) const noexcept {
    const SyncFrame min_frames = config_.auto_buffered_frame_lag_min;
    const SyncFrame max_frames = static_cast<SyncFrame>(config_.buffered_frame_lag_capacity - 1U);
    return std::min(max_frames, std::max(min_frames, frames));
}

SyncFrame ReplicationClientClock::clamp_prediction_target(SyncFrame frames) const noexcept {
    const SyncFrame min_frames = config_.auto_prediction_min_frames;
    const SyncFrame max_frames = static_cast<SyncFrame>(config_.input_buffer_capacity_frames - 1U);
    return std::min(max_frames, std::max(min_frames, frames));
}

}  // namespace ashiato::sync
