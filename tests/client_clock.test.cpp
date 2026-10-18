#include "ashiato/sync/client_clock.hpp"

#include <catch2/catch_approx.hpp>
#include <catch2/catch_test_macros.hpp>

#include <cmath>
#include <cstdio>
#include <stdexcept>

namespace {

ashiato::sync::ReplicationClientClockConfig test_config() {
    ashiato::sync::ReplicationClientClockConfig config;
    config.fixed_dt_seconds = 1.0 / 60.0;
    config.buffered_frame_lag = 2;
    config.buffered_frame_lag_capacity = 8;
    config.prediction_lead_frames = 2;
    config.input_buffer_capacity_frames = 16;
    config.auto_buffered_frame_lag_smoothing = 1.0f;
    config.auto_buffered_time_dilation_min = 0.90f;
    config.auto_buffered_time_dilation_max = 1.10f;
    config.auto_buffered_time_dilation_gain = 0.10f;
    config.auto_predicted_time_dilation_min = 0.90f;
    config.auto_predicted_time_dilation_max = 1.10f;
    config.auto_predicted_time_dilation_gain = 0.10f;
    config.auto_timing_warmup_samples = 3;
    return config;
}

void record_pong(
    ashiato::sync::ReplicationClientClock& clock,
    ashiato::sync::SyncFrame send_frame,
    ashiato::sync::SyncFrame local_frame) {
    const double dt = clock.config().fixed_dt_seconds;
    const double client_send_time = static_cast<double>(send_frame) * dt;
    const double client_receive_time = static_cast<double>(local_frame) * dt;
    const double server_time = (static_cast<double>(send_frame) + static_cast<double>(local_frame)) * 0.5 * dt;
    clock.record_time_sync_sample(client_send_time, server_time, server_time, client_receive_time);
}

void advance_local_frame_to(ashiato::sync::ReplicationClientClock& clock, ashiato::sync::SyncFrame frame) {
    const double target_time = static_cast<double>(frame) * clock.config().fixed_dt_seconds;
    if (target_time > clock.local_time_seconds()) {
        clock.advance_local_time(target_time - clock.local_time_seconds());
    }
}

}  // namespace

TEST_CASE("client clock initializes stats from startup timing config") {
    const ashiato::sync::ReplicationClientClock clock(test_config());

    REQUIRE_FALSE(clock.bootstrapped());
    REQUIRE(clock.local_time_seconds() == Catch::Approx(0.0));
    REQUIRE(clock.buffered_frame() == 0);
    REQUIRE(clock.predicted_frame() == 0);
    REQUIRE(clock.buffered_frame_lag() == 2);
    REQUIRE(clock.stats().desired_buffered_frame_lag == 2);
    REQUIRE(clock.stats().target_buffered_frame_lag == 2);
    REQUIRE(clock.stats().current_buffered_frame_lag == 2);
    REQUIRE(clock.stats().desired_prediction_lead_frames == 2);
    REQUIRE(clock.stats().target_prediction_lead_frames == 2);
    REQUIRE(clock.stats().current_prediction_lead_frames == 2);
    REQUIRE(clock.stats().buffered_time_dilation == Catch::Approx(1.0f));
    REQUIRE(clock.stats().predicted_time_dilation == Catch::Approx(1.0f));
}

TEST_CASE("client clock rejects invalid timing config") {
    ashiato::sync::ReplicationClientClockConfig config = test_config();
    config.fixed_dt_seconds = 0.0;
    REQUIRE_THROWS_AS(ashiato::sync::ReplicationClientClock(config), std::invalid_argument);

    config = test_config();
    config.buffered_frame_lag_capacity = 7;
    REQUIRE_THROWS_AS(ashiato::sync::ReplicationClientClock(config), std::invalid_argument);

    config = test_config();
    config.buffered_frame_lag = 8;
    REQUIRE_THROWS_AS(ashiato::sync::ReplicationClientClock(config), std::invalid_argument);

    config = test_config();
    config.auto_buffered_frame_lag_smoothing = 0.0f;
    REQUIRE_THROWS_AS(ashiato::sync::ReplicationClientClock(config), std::invalid_argument);

    config = test_config();
    config.prediction_lead_frames = 16;
    REQUIRE_THROWS_AS(ashiato::sync::ReplicationClientClock(config), std::invalid_argument);

    config = test_config();
    config.auto_timing_fast_recovery_min_frame_gap = 0;
    REQUIRE_THROWS_AS(ashiato::sync::ReplicationClientClock(config), std::invalid_argument);
}

TEST_CASE("client clock advances only local time before bootstrap") {
    ashiato::sync::ReplicationClientClock clock(test_config());

    const ashiato::sync::ReplicationClientClock::AdvanceResult advanced =
        clock.advance(3.0 * clock.config().fixed_dt_seconds);

    REQUIRE(advanced.buffered.empty());
    REQUIRE(advanced.predicted.empty());
    REQUIRE(clock.local_time_seconds() == Catch::Approx(3.0 * clock.config().fixed_dt_seconds));
    REQUIRE(clock.buffered_frame() == 0);
    REQUIRE(clock.predicted_frame() == 0);
}

TEST_CASE("client clock bootstrap anchors buffered and predicted without catchup burst") {
    ashiato::sync::ReplicationClientClock clock(test_config());
    (void)clock.advance(20.0 * clock.config().fixed_dt_seconds);

    REQUIRE(clock.maybe_bootstrap_from_first_server_update(10));
    REQUIRE(clock.bootstrapped());
    REQUIRE(clock.buffered_frame() == 8);
    REQUIRE(clock.predicted_frame() == 12);
    REQUIRE(clock.stats().current_prediction_lead_frames == 2);

    const ashiato::sync::ReplicationClientClock::AdvanceResult none = clock.advance(0.0);
    REQUIRE(none.buffered.empty());
    REQUIRE(none.predicted.empty());

    const ashiato::sync::ReplicationClientClock::AdvanceResult one =
        clock.advance(clock.config().fixed_dt_seconds);
    REQUIRE(one.buffered.first == 9);
    REQUIRE(one.buffered.last == 9);
    REQUIRE(one.predicted.first == 13);
    REQUIRE(one.predicted.last == 13);
}

TEST_CASE("client clock caps predicted accumulator backlog when configured") {
    ashiato::sync::ReplicationClientClockConfig config = test_config();
    config.max_fixed_steps_per_tick = 3;
    ashiato::sync::ReplicationClientClock clock(config);
    REQUIRE(clock.maybe_bootstrap_from_first_server_update(10));

    const ashiato::sync::ReplicationClientClock::AdvanceResult advanced =
        clock.advance_client_frame_numbers(4.0 * clock.config().fixed_dt_seconds);

    REQUIRE(advanced.buffered.empty());
    REQUIRE(advanced.predicted.first == 13);
    REQUIRE(advanced.predicted.last == 15);
    REQUIRE(clock.buffered_frame() == 8);
    REQUIRE(clock.predicted_frame() == 15);
    REQUIRE(clock.buffered_subframe() == Catch::Approx(0.0));
    REQUIRE(clock.predicted_subframe() == Catch::Approx(0.0));
    REQUIRE(clock.stats().dropped_playback_frames == 0);
    REQUIRE(clock.stats().dropped_input_frames == 1);
}

TEST_CASE("client clock clamps bootstrap buffered at zero") {
    ashiato::sync::ReplicationClientClock clock(test_config());

    REQUIRE(clock.maybe_bootstrap_from_first_server_update(1));
    REQUIRE(clock.buffered_frame() == 0);
    REQUIRE(clock.predicted_frame() == 3);
}

TEST_CASE("client clock records pre-bootstrap pongs for startup timing") {
    ashiato::sync::ReplicationClientClock clock(test_config());

    record_pong(clock, 0, 8);
    record_pong(clock, 8, 16);
    record_pong(clock, 16, 24);

    REQUIRE(clock.stats().sample_count == 3);
    REQUIRE(clock.stats().latency_frames == Catch::Approx(4.0f));
    REQUIRE(clock.stats().desired_buffered_frame_lag == 4);
    REQUIRE(clock.stats().target_buffered_frame_lag == 4);
    REQUIRE(clock.stats().desired_prediction_lead_frames == 9);
}

TEST_CASE("client clock holds startup targets during warmup samples") {
    ashiato::sync::ReplicationClientClock clock(test_config());
    REQUIRE(clock.maybe_bootstrap_from_first_server_update(10));

    record_pong(clock, 10, 18);
    record_pong(clock, 18, 26);

    REQUIRE(clock.stats().sample_count == 2);
    REQUIRE(clock.stats().latency_frames == Catch::Approx(4.0f));
    REQUIRE(clock.stats().desired_buffered_frame_lag == 2);
    REQUIRE(clock.stats().target_buffered_frame_lag == 2);
    REQUIRE(clock.stats().desired_prediction_lead_frames == 2);
    REQUIRE(clock.stats().target_prediction_lead_frames == 2);
}

TEST_CASE("client clock computes auto targets after warmup") {
    ashiato::sync::ReplicationClientClock clock(test_config());
    REQUIRE(clock.maybe_bootstrap_from_first_server_update(10));

    record_pong(clock, 10, 18);
    record_pong(clock, 18, 26);
    record_pong(clock, 26, 34);

    REQUIRE(clock.stats().sample_count == 3);
    REQUIRE(clock.stats().latency_frames == Catch::Approx(4.0f));
    REQUIRE(clock.stats().jitter_frames == Catch::Approx(0.0f));
    REQUIRE(clock.stats().desired_buffered_frame_lag == 4);
    REQUIRE(clock.stats().target_buffered_frame_lag == 4);
    REQUIRE(clock.stats().desired_prediction_lead_frames == 9);
    REQUIRE(clock.stats().target_prediction_lead_frames == 9);
}

TEST_CASE("client clock re-estimates server frame after server clock hitch") {
    ashiato::sync::ReplicationClientClock clock(test_config());
    const double dt = clock.config().fixed_dt_seconds;

    clock.record_time_sync_sample(0.0 * dt, 0.5 * dt, 0.5 * dt, 1.0 * dt);
    advance_local_frame_to(clock, 10);
    REQUIRE(clock.estimated_server_frame() == Catch::Approx(10.0));

    clock.record_time_sync_sample(10.0 * dt, 8.0 * dt, 8.0 * dt, 12.0 * dt);
    REQUIRE(clock.local_time_seconds() == Catch::Approx(10.0 * dt));
    REQUIRE(clock.estimated_server_frame() == Catch::Approx(7.0));

    advance_local_frame_to(clock, 14);
    REQUIRE(clock.estimated_server_frame() == Catch::Approx(11.0));

    clock.record_time_sync_sample(14.0 * dt, 15.0 * dt, 15.0 * dt, 16.0 * dt);
    REQUIRE(clock.estimated_server_frame() == Catch::Approx(14.0));
}

TEST_CASE("client clock fast recovery uses downstream lag before warmup") {
    ashiato::sync::ReplicationClientClock clock(test_config());
    advance_local_frame_to(clock, 20);
    REQUIRE(clock.maybe_bootstrap_from_first_server_update(10));

    const ashiato::sync::SyncFrame prefill = clock.record_server_update(10, 20, 18, true, 12);
    REQUIRE(clock.stats().desired_buffered_frame_lag == 2);
    REQUIRE(clock.stats().target_buffered_frame_lag == 7);
    REQUIRE(clock.buffered_frame_lag() == 7);
    REQUIRE(clock.stats().target_prediction_lead_frames == 15);
    REQUIRE(prefill == 25);
    REQUIRE(clock.stats().buffered_time_dilation == Catch::Approx(0.90f));
}

TEST_CASE("client clock handles divergent observed receive and buffered frames") {
    ashiato::sync::ReplicationClientClock clock(test_config());
    REQUIRE(clock.maybe_bootstrap_from_first_server_update(1));

    (void)clock.record_server_update(
        1,
        10.0,
        7.0,
        10.0,
        true,
        10);

    REQUIRE(std::isfinite(clock.stats().measured_buffered_frame_lag));
    REQUIRE(std::isfinite(clock.stats().measured_prediction_lead_frames));
    REQUIRE(std::isfinite(clock.stats().buffered_time_dilation));
    REQUIRE(std::isfinite(clock.stats().predicted_time_dilation));
}

TEST_CASE("client clock fast recovery snaps large buffered lag gaps to target") {
    ashiato::sync::ReplicationClientClockConfig config = test_config();
    config.buffered_frame_lag_capacity = 64;
    config.input_buffer_capacity_frames = 64;
    ashiato::sync::ReplicationClientClock clock(config);
    advance_local_frame_to(clock, 40);
    REQUIRE(clock.maybe_bootstrap_from_first_server_update(10));

    const ashiato::sync::SyncFrame prefill = clock.record_server_update(10, 40, 38, true, 12);

    REQUIRE(clock.stats().target_buffered_frame_lag == 30);
    REQUIRE(clock.buffered_frame_lag() == 30);
    REQUIRE(clock.stats().target_prediction_lead_frames == 61);
    REQUIRE(prefill == 71);
}

TEST_CASE("client clock downstream lag floor can use legacy warmup behavior") {
    ashiato::sync::ReplicationClientClockConfig config = test_config();
    config.auto_timing_fast_recovery = false;
    ashiato::sync::ReplicationClientClock clock(config);
    advance_local_frame_to(clock, 20);
    REQUIRE(clock.maybe_bootstrap_from_first_server_update(10));

    REQUIRE(clock.record_server_update(10, true, 12) == 0);
    REQUIRE(clock.stats().target_buffered_frame_lag == 2);
    REQUIRE(clock.stats().buffered_time_dilation == Catch::Approx(1.0f));

    record_pong(clock, 10, 18);
    record_pong(clock, 18, 26);
    record_pong(clock, 26, 34);
    clock.record_server_update(11, true, 13);

    REQUIRE(clock.stats().target_buffered_frame_lag == 7);
    REQUIRE(clock.stats().buffered_time_dilation == Catch::Approx(0.90f));
    REQUIRE(clock.buffered_frame_lag() == 3);
}

TEST_CASE("client clock buffered frame lag shrinks one frame at a time") {
    ashiato::sync::ReplicationClientClockConfig config = test_config();
    config.buffered_frame_lag = 4;
    config.auto_buffered_frame_lag_min = 1;
    config.auto_buffered_frame_lag_jitter_multiplier = 0.0f;
    config.auto_timing_warmup_samples = 1;
    config.auto_timing_fast_recovery = false;
    ashiato::sync::ReplicationClientClock clock(config);

    REQUIRE(clock.maybe_bootstrap_from_first_server_update(4));
    record_pong(clock, 10, 10);
    clock.record_server_update(4, true, 6);
    REQUIRE(clock.stats().desired_buffered_frame_lag == 1);
    REQUIRE(clock.buffered_frame_lag() == 3);

    clock.record_server_update(3, true, 7);
    REQUIRE(clock.buffered_frame_lag() == 2);

    clock.record_server_update(2, true, 8);
    REQUIRE(clock.buffered_frame_lag() == 1);
}

TEST_CASE("client clock manual buffered lag override resets target and dilation") {
    ashiato::sync::ReplicationClientClockConfig config = test_config();
    config.auto_timing_warmup_samples = 1;
    ashiato::sync::ReplicationClientClock clock(config);

    REQUIRE(clock.maybe_bootstrap_from_first_server_update(10));
    record_pong(clock, 10, 18);
    clock.record_server_update(11, true, 13);
    REQUIRE(clock.stats().desired_buffered_frame_lag == 4);
    REQUIRE(clock.stats().buffered_time_dilation != Catch::Approx(1.0f));

    REQUIRE(clock.set_buffered_frame_lag(3));
    REQUIRE(clock.buffered_frame_lag() == 3);
    REQUIRE(clock.stats().desired_buffered_frame_lag == 3);
    REQUIRE(clock.stats().target_buffered_frame_lag == 3);
    REQUIRE(clock.stats().current_buffered_frame_lag == 3);
    REQUIRE(clock.stats().buffered_time_dilation == Catch::Approx(1.0f));
}

TEST_CASE("client clock leaves buffered neutral when auto buffered frame lag is disabled") {
    ashiato::sync::ReplicationClientClockConfig config = test_config();
    config.auto_buffered_frame_lag = false;
    config.auto_timing_warmup_samples = 1;
    ashiato::sync::ReplicationClientClock clock(config);

    REQUIRE(clock.maybe_bootstrap_from_first_server_update(10));
    record_pong(clock, 10, 18);
    clock.record_server_update(10, true, 12);

    REQUIRE(clock.stats().desired_buffered_frame_lag == 4);
    REQUIRE(clock.stats().target_buffered_frame_lag == 4);
    REQUIRE(clock.buffered_frame_lag() == 2);
    REQUIRE(clock.stats().buffered_time_dilation == Catch::Approx(1.0f));
}

TEST_CASE("client clock zero buffered dilation gain tracks target with neutral speed") {
    ashiato::sync::ReplicationClientClockConfig config = test_config();
    config.auto_timing_warmup_samples = 1;
    config.auto_buffered_time_dilation_gain = 0.0f;
    ashiato::sync::ReplicationClientClock clock(config);

    REQUIRE(clock.maybe_bootstrap_from_first_server_update(10));
    record_pong(clock, 10, 18);
    clock.record_server_update(11, true, 13);

    REQUIRE(clock.stats().desired_buffered_frame_lag == 4);
    REQUIRE(clock.stats().target_buffered_frame_lag == 4);
    REQUIRE(clock.stats().buffered_time_dilation == Catch::Approx(1.0f));
}

TEST_CASE("client clock predicted dilation follows measured input lead") {
    ashiato::sync::ReplicationClientClockConfig config = test_config();
    config.auto_timing_warmup_samples = 1;
    ashiato::sync::ReplicationClientClock clock(config);

    REQUIRE(clock.maybe_bootstrap_from_first_server_update(10));
    record_pong(clock, 10, 18);
    clock.record_server_update(10, false, 10);

    REQUIRE(clock.stats().desired_prediction_lead_frames == 9);
    REQUIRE(clock.stats().measured_prediction_lead_frames == Catch::Approx(0.0f));
    REQUIRE(clock.stats().predicted_time_dilation == Catch::Approx(1.10f));

    clock.record_server_update(11, false, 21);
    REQUIRE(clock.stats().measured_prediction_lead_frames == Catch::Approx(10.0f));
    REQUIRE(clock.stats().predicted_time_dilation == Catch::Approx(0.90f));
}

TEST_CASE("client clock small prediction lead changes keep time dilation nudging") {
    ashiato::sync::ReplicationClientClockConfig config = test_config();
    config.auto_timing_warmup_samples = 1;
    ashiato::sync::ReplicationClientClock clock(config);

    REQUIRE(clock.maybe_bootstrap_from_first_server_update(10));
    record_pong(clock, 10, 18);
    const ashiato::sync::SyncFrame prefill = clock.record_server_update(11, 14, 9, false, 18);

    REQUIRE(prefill == 0);
    REQUIRE(clock.stats().target_prediction_lead_frames == 9);
    REQUIRE(clock.stats().measured_prediction_lead_frames == Catch::Approx(7.0f));
    REQUIRE(clock.stats().predicted_time_dilation == Catch::Approx(1.10f));
}

TEST_CASE("client clock predicted dilation uses continuous input lead") {
    ashiato::sync::ReplicationClientClockConfig config = test_config();
    config.auto_timing_warmup_samples = 1;
    ashiato::sync::ReplicationClientClock clock(config);

    REQUIRE(clock.maybe_bootstrap_from_first_server_update(10));
    record_pong(clock, 10, 18);
    const ashiato::sync::SyncFrame prefill = clock.record_server_update(
        11,
        14.0,
        9.0,
        19.5,
        false);

    REQUIRE(prefill == 0);
    REQUIRE(clock.stats().target_prediction_lead_frames == 9);
    REQUIRE(clock.stats().measured_prediction_lead_frames == Catch::Approx(8.5f));
    REQUIRE(clock.stats().predicted_time_dilation == Catch::Approx(1.05f));
}

TEST_CASE("client clock predicted dilation has a subframe deadband") {
    ashiato::sync::ReplicationClientClockConfig config = test_config();
    config.auto_timing_warmup_samples = 1;
    ashiato::sync::ReplicationClientClock clock(config);

    REQUIRE(clock.maybe_bootstrap_from_first_server_update(10));
    record_pong(clock, 10, 18);
    (void)clock.record_server_update(
        11,
        14.0,
        9.0,
        19.8,
        false);

    REQUIRE(clock.stats().target_prediction_lead_frames == 9);
    REQUIRE(clock.stats().measured_prediction_lead_frames == Catch::Approx(8.8f));
    REQUIRE(clock.stats().predicted_time_dilation == Catch::Approx(1.0f));
}

TEST_CASE("client clock small buffered lag changes keep one-frame movement") {
    ashiato::sync::ReplicationClientClockConfig config = test_config();
    config.buffered_frame_lag = 2;
    config.auto_buffered_frame_lag_jitter_multiplier = 0.0f;
    config.auto_timing_warmup_samples = 1;
    ashiato::sync::ReplicationClientClock clock(config);

    REQUIRE(clock.maybe_bootstrap_from_first_server_update(10));
    record_pong(clock, 10, 16);
    clock.record_server_update(11, 14, 11, true, 13);

    REQUIRE(clock.stats().desired_buffered_frame_lag == 3);
    REQUIRE(clock.stats().target_buffered_frame_lag == 3);
    REQUIRE(clock.buffered_frame_lag() == 3);
    REQUIRE(clock.stats().buffered_time_dilation == Catch::Approx(0.90f));

    record_pong(clock, 16, 20);
    clock.record_server_update(12, 12, 9, true, 14);

    REQUIRE(clock.stats().desired_buffered_frame_lag == 2);
    REQUIRE(clock.buffered_frame_lag() == 2);
    REQUIRE(clock.stats().buffered_time_dilation == Catch::Approx(1.10f));
}

TEST_CASE("client clock buffered dilation uses continuous buffered delay") {
    ashiato::sync::ReplicationClientClockConfig config = test_config();
    config.auto_timing_warmup_samples = 1;
    ashiato::sync::ReplicationClientClock clock(config);

    REQUIRE(clock.maybe_bootstrap_from_first_server_update(10));
    record_pong(clock, 10, 16);
    clock.record_server_update(
        11,
        14.0,
        8.5,
        13.0,
        true);

    REQUIRE(clock.stats().target_buffered_frame_lag == 3);
    REQUIRE(clock.stats().measured_buffered_frame_lag == Catch::Approx(2.5f));
    REQUIRE(clock.stats().buffered_time_dilation == Catch::Approx(0.95f));
}

TEST_CASE("client clock buffered dilation has a subframe deadband") {
    ashiato::sync::ReplicationClientClockConfig config = test_config();
    config.auto_timing_warmup_samples = 1;
    ashiato::sync::ReplicationClientClock clock(config);

    REQUIRE(clock.maybe_bootstrap_from_first_server_update(10));
    record_pong(clock, 10, 16);
    clock.record_server_update(
        11,
        14.0,
        7.8,
        13.0,
        true);

    REQUIRE(clock.stats().target_buffered_frame_lag == 3);
    REQUIRE(clock.stats().measured_buffered_frame_lag == Catch::Approx(3.2f));
    REQUIRE(clock.stats().buffered_time_dilation == Catch::Approx(1.0f));
}

TEST_CASE("client clock prediction lead budgets for both network legs") {
    ashiato::sync::ReplicationClientClockConfig config = test_config();
    config.input_buffer_capacity_frames = 32;
    config.auto_timing_warmup_samples = 1;
    ashiato::sync::ReplicationClientClock clock(config);

    REQUIRE(clock.maybe_bootstrap_from_first_server_update(10));
    record_pong(clock, 10, 22);

    REQUIRE(clock.stats().latency_frames == Catch::Approx(6.0f));
    REQUIRE(clock.stats().desired_buffered_frame_lag == 6);
    REQUIRE(clock.stats().desired_prediction_lead_frames == 13);
    REQUIRE(clock.stats().target_prediction_lead_frames == 13);
}

TEST_CASE("client clock keeps prediction neutral when auto prediction is disabled") {
    ashiato::sync::ReplicationClientClockConfig config = test_config();
    config.auto_prediction_lead_frames = false;
    config.auto_timing_warmup_samples = 1;
    ashiato::sync::ReplicationClientClock clock(config);

    REQUIRE(clock.maybe_bootstrap_from_first_server_update(10));
    record_pong(clock, 10, 18);
    clock.record_server_update(10, false, 10);

    REQUIRE(clock.stats().target_prediction_lead_frames == 2);
    REQUIRE(clock.stats().predicted_time_dilation == Catch::Approx(1.0f));
}

TEST_CASE("client clock buffered frame advances one frame every tick when the estimate lands on a whole frame") {
    ashiato::sync::ReplicationClientClock clock(test_config());
    const double dt = clock.config().fixed_dt_seconds;

    // Zero latency: the pong's server time is the client's own time, so the estimated server frame is the client's
    // local frame and the buffered frame (estimate minus the lag) is a whole number on every tick.
    clock.record_time_sync_sample(0.0, 0.0, 0.0, 0.0);
    REQUIRE(clock.maybe_bootstrap_from_first_server_update(0));

    ashiato::sync::SyncFrame previous = clock.buffered_frame();
    int bad_ticks = 0;
    for (int tick = 1; tick <= 600; ++tick) {
        (void)clock.advance(dt);
        const ashiato::sync::SyncFrame now = clock.buffered_frame();
        if (tick > 3 && now != previous + 1U) {
            ++bad_ticks;
            std::fprintf(stderr, "tick %d buffered %u previous %u continuous %.17g\n", tick, static_cast<unsigned>(now),
                static_cast<unsigned>(previous), clock.continuous_buffered_frame());
        }
        previous = now;
        // The time-sync pong that arrives every two seconds reports the same zero-latency answer.
        if (tick % 120 == 0) {
            const double t = clock.local_time_seconds();
            clock.record_time_sync_sample(t, t, t, t);
        }
    }
    REQUIRE(bad_ticks == 0);
}
