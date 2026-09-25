#include "ashiato/sync/client.hpp"
#include "ashiato/sync/profiling.hpp"

#include "client/store/ack_queue.hpp"
#include "client/runtime/buffered_runtime.hpp"
#include "client/runtime/cue_runtime.hpp"
#include "client/runtime/display_sampler.hpp"
#include "client/store/entity_store.hpp"
#include "client/frame_components.hpp"
#include "client/store/frame_ring_store.hpp"
#include "client/frame_data.hpp"
#include "client/store/input_buffer.hpp"
#include "client/runtime/prediction_runtime.hpp"
#include "client/session_transport.hpp"
#include "client/state.hpp"
#include "client/timing_stats.hpp"
#include "client/tracing.hpp"
#include "client/runtime/update_runtime.hpp"
#include "detail/logging.hpp"
#include "detail/options_validation.hpp"

#include "ashiato/sync/protocol.hpp"

#ifndef ASHIATO_ENABLE_DEBUG_SERVER
#define ASHIATO_ENABLE_DEBUG_SERVER 0
#endif

#if ASHIATO_ENABLE_DEBUG_SERVER
#include "ashiato/debug_server.hpp"
#endif

#include <spdlog/logger.h>

#include <algorithm>
#include <cmath>
#include <cstddef>
#include <cstdint>
#include <cstring>
#include <limits>
#include <mutex>
#include <sstream>
#include <stdexcept>
#include <utility>

namespace ashiato::sync {
namespace {

using client_detail::apply_archetype_tags;
using client_detail::EntityFrameView;
using client_detail::frame_component_data;
using client_detail::FrameWriteSource;
using client_detail::frame_has_component;
using client_detail::has_tag_slot;
using client_detail::init_frame_data;
using client_detail::mutable_frame_component_data;
using client_detail::MutableEntityFrameView;
using client_detail::remove_archetype_tags;
using client_detail::sync_slot_bit;
using client_detail::sync_slot_count;
using client_detail::tag_bit_set;
using client_detail::unchecked_frame_component_data;
using client_detail::unchecked_mutable_frame_component_data;

#if ASHIATO_ENABLE_DEBUG_SERVER
void set_sync_debug_name(ashiato::Registry& registry, ashiato::Entity entity, const SyncArchetype& archetype) {
    if (!registry.alive(entity)) {
        return;
    }
    (void)registry.register_component<ashiato::DebugName>("DebugName");
    if (archetype.name.empty()) {
        registry.remove<ashiato::DebugName>(entity);
        return;
    }
    registry.add<ashiato::DebugName>(entity, ashiato::DebugName{archetype.name});
}
#else
void set_sync_debug_name(ashiato::Registry&, ashiato::Entity, const SyncArchetype&) {}
#endif

std::string missing_prediction_rollback_trait_message(
    SyncArchetypeId archetype_id,
    const SyncArchetype& archetype,
    std::size_t component_index) {
    std::ostringstream message;
    message << "predicted replicated component must define SyncComponentTraits<T>::should_roll_back"
            << " archetype=" << archetype_id.value;
    if (!archetype.name.empty()) {
        message << " archetype_name=" << archetype.name;
    }
    message << " component_index=" << component_index;
    if (component_index < archetype.components.size()) {
        message << " component=" << archetype.components[component_index].component.value;
    }
    if (component_index < archetype.component_ops.size() &&
        !archetype.component_ops[component_index].serialization.name.empty()) {
        message << " component_name=" << archetype.component_ops[component_index].serialization.name;
    }
    return message.str();
}

ClientError missing_prediction_rollback_trait_error(
    SyncArchetypeId archetype_id,
    const SyncArchetype& archetype,
    std::size_t component_index) {
    const std::string message =
        missing_prediction_rollback_trait_message(archetype_id, archetype, component_index);
    return ClientError(ClientStatus::MissingPredictionRollbackTrait, message.c_str());
}

ReplicationClientClockConfig make_clock_config(
    const ReplicationClientOptions& options,
    std::size_t buffered_frame_capacity) noexcept {
    ReplicationClientClockConfig config;
    config.fixed_dt_seconds = options.clock.fixed_dt_seconds;
    config.buffered_frame_lag = options.buffered.buffered_frame_lag;
    config.buffered_frame_lag_capacity = buffered_frame_capacity;
    config.auto_buffered_frame_lag = options.buffered.auto_buffered_frame_lag;
    config.auto_buffered_frame_lag_min = options.buffered.auto_buffered_frame_lag_min;
    config.auto_buffered_frame_lag_jitter_multiplier = options.buffered.auto_buffered_frame_lag_jitter_multiplier;
    config.auto_buffered_frame_lag_smoothing = options.buffered.auto_buffered_frame_lag_smoothing;
    config.auto_buffered_time_dilation_min = options.buffered.auto_buffered_time_dilation_min;
    config.auto_buffered_time_dilation_max = options.buffered.auto_buffered_time_dilation_max;
    config.auto_buffered_time_dilation_gain = options.buffered.auto_buffered_time_dilation_gain;
    config.auto_prediction_lead_frames = options.prediction.auto_lead_frames;
    config.prediction_lead_frames = options.prediction.lead_frames;
    config.input_buffer_capacity_frames = options.prediction.input_buffer_capacity_frames;
    config.auto_prediction_min_frames = options.prediction.auto_min_frames;
    config.auto_prediction_safety_frames = options.prediction.auto_safety_frames;
    config.auto_prediction_jitter_multiplier = options.prediction.auto_jitter_multiplier;
    config.auto_predicted_time_dilation_min = options.prediction.auto_predicted_time_dilation_min;
    config.auto_predicted_time_dilation_max = options.prediction.auto_predicted_time_dilation_max;
    config.auto_predicted_time_dilation_gain = options.prediction.auto_predicted_time_dilation_gain;
    config.auto_timing_warmup_samples = options.clock.auto_timing_warmup_samples;
    config.auto_timing_fast_recovery = options.clock.auto_timing_fast_recovery;
    config.auto_timing_fast_recovery_min_frame_gap = options.clock.auto_timing_fast_recovery_min_frame_gap;
    config.max_fixed_steps_per_tick = options.clock.max_fixed_steps_per_tick;
    return config;
}

bool all_zero(const SyncComponentOps::QuantizedBytes& bytes) {
    return std::all_of(bytes.begin(), bytes.end(), [](std::uint8_t byte) {
        return byte == 0U;
    });
}

std::uint32_t frame_range_count(const ReplicationClientClock::FrameRange& range) noexcept {
    return range.empty() || range.last < range.first ? 0U : range.last - range.first + 1U;
}

const SyncComponentOps* find_update_component_ops(
    const SyncSettings& settings,
    SyncArchetypeId archetype,
    ashiato::Entity component,
    SyncComponentSerializerId serializer) {
    if (archetype.value < settings.archetypes.size()) {
        const SyncArchetype& definition = settings.archetypes[archetype.value];
        for (std::size_t component_index = 0; component_index < definition.components.size(); ++component_index) {
            if (component_index < definition.component_ops.size() &&
                definition.components[component_index].component == component) {
                return &definition.component_ops[component_index];
            }
        }
    }
    if (serializer.value < settings.component_serializers.size() &&
        settings.component_serializers[serializer.value].serialization.component == component) {
        return &settings.component_serializers[serializer.value];
    }
    const auto found_ops = settings.component_ops.find(component.value);
    return found_ops != settings.component_ops.end() ? &found_ops->second : nullptr;
}

bool dequantize_component_update(
    const SyncSettings& settings,
    SyncArchetypeId archetype,
    const ReplicatedComponentUpdate& update,
    void* out) {
    const SyncComponentOps* ops =
        find_update_component_ops(settings, archetype, update.component, update.serializer);
    if (ops == nullptr || ops->serialization.dequantize == nullptr) {
        return false;
    }
    if (update.bytes.size() != ops->serialization.quantized_size) {
        return false;
    }
    ops->serialization.dequantize(update.bytes.data(), out);
    return true;
}

}  // namespace

struct ReplicationClient::RegistryFrameApplyInfo {
    SyncFrame frame = 0;
    detail::FrameDataView frame_data;
    std::uint64_t previous_present_mask = 0;
    std::uint64_t next_present_mask = 0;
    bool remove_missing_components = false;
    bool verify_tag_apply = false;
};

bool ReplicatedEntityUpdateView::try_get(
    const ashiato::Registry& registry,
    ashiato::Entity component,
    void* out) const {
    if (components == nullptr || out == nullptr) {
        return false;
    }

    const auto found = std::find_if(
        components->begin(),
        components->end(),
        [component](const ReplicatedComponentUpdate& update) {
            return update.component == component;
        });
    if (found == components->end()) {
        return false;
    }

    const SyncSettings& settings = registry.get<SyncSettings>();
    return dequantize_component_update(settings, archetype, *found, out);
}

bool ReplicatedEntityUpdateView::has_tag(const ashiato::Registry& registry, ashiato::Entity tag) const {
    const SyncSettings& settings = registry.get<SyncSettings>();
    if (archetype.value >= settings.archetypes.size()) {
        return false;
    }
    const SyncArchetype& definition = settings.archetypes[archetype.value];
    for (std::size_t tag_index = 0; tag_index < definition.tags.size(); ++tag_index) {
        if (definition.tags[tag_index].tag == tag) {
            return tag_bit_set(tag_mask, tag_index);
        }
    }
    return false;
}

bool FractionalTickSample::try_get_sampled_value(
    const ashiato::Registry& registry,
    ashiato::Entity component,
    void* out) const {
    if (out == nullptr) {
        return false;
    }
    if (!registry.has<FractionalTickSampled>(component)) {
        throw std::logic_error("fractional tick sample component is not marked fractional-tick-sampled");
    }

    const auto found = std::find_if(
        components_.begin(),
        components_.end(),
        [component](const ReplicatedComponentUpdate& update) {
            return update.component == component;
        });
    if (found == components_.end()) {
        return false;
    }

    const SyncSettings& settings = registry.get<SyncSettings>();
    return dequantize_component_update(settings, invalid_sync_archetype_id, *found, out);
}

ReplicationClient::ReplicationClient(ashiato::Registry& registry, ReplicationClientOptions options)
    : ReplicationClient(
          registry,
          std::move(options),
          FrameHistoryCapacities{buffered_frame_capacity, prediction_frame_capacity}) {}

ReplicationClient::ReplicationClient(
    ashiato::Registry& registry,
    ReplicationClientOptions options,
    FrameHistoryCapacities capacities)
    : options_(detail::validate_client_options(std::move(options), capacities.buffered, capacities.predicted)),
      fixed_dt_seconds_(options_.clock.fixed_dt_seconds),
      clock_(make_clock_config(options_, capacities.buffered)),
      entity_store_(std::make_unique<client_detail::ClientEntityStore>()),
      prediction_(std::make_unique<client_detail::ClientPredictionRuntime>(capacities.predicted)),
      session_transport_(std::make_unique<client_detail::ClientSessionTransport>()),
      cue_runtime_(std::make_unique<client_detail::ClientCueRuntime>()),
      buffered_runtime_(std::make_unique<client_detail::ClientBufferedRuntime>(capacities.buffered)),
      ack_queue_(std::make_unique<client_detail::ClientAckQueue>()),
      timing_stats_(std::make_unique<client_detail::ClientTimingStatsCalculator>(
          options_.network.protocol.max_pending_packet_acks_per_client)),
      update_runtime_(std::make_unique<client_detail::ClientUpdateRuntime>()),
      input_(std::make_unique<client_detail::ClientInputBuffer>()),
      logger_(detail::make_logger(options_.logging, "ashiato.sync.client")) {
    register_components(registry);
    SyncSettings& settings = registry.write<SyncSettings>();
    settings.role = SyncRole::Client;
    settings.fixed_dt_seconds = fixed_dt_seconds_;
    registry.write<SyncAuthority>().authoritative = false;
    set_client_id(registry, options_.session.local_client);
#ifdef ASHIATO_SYNC_ENABLE_TRACING
    set_trace_options(options_.trace);
#endif
    session_transport_->adaptive_ping_active = true;
    if (options_.session.connect_token.empty()) {
        set_connection_state(ReplicationClientConnectionState::Ready, client_id_);
        (void)clock_.maybe_bootstrap_from_first_server_update(0, false, true);
    }
}

ReplicationClient::~ReplicationClient() = default;

ReplicationClient::ReplicationClient(ReplicationClient&& other) noexcept = default;

ReplicationClient& ReplicationClient::operator=(ReplicationClient&& other) noexcept = default;

void ReplicationClient::set_logger(std::shared_ptr<spdlog::logger> logger) {
    options_.logging.logger = std::move(logger);
    logger_ = detail::make_logger(options_.logging, "ashiato.sync.client");
}

void ReplicationClient::set_log_level(LogLevel level) {
    options_.logging.level = level;
    if (logger_ == nullptr) {
        logger_ = detail::make_logger(options_.logging, "ashiato.sync.client");
    } else {
        logger_->set_level(detail::to_spdlog_level(level));
    }
}

void ReplicationClient::set_client_id(ashiato::Registry& registry, ClientId client) {
    client_id_ = client;
    SyncSettings& settings = registry.write<SyncSettings>();
    settings.local_client = client;
}

void ReplicationClient::set_connection_state(
    ReplicationClientConnectionState state,
    ClientId client,
    std::string reason) {
    const ReplicationClientConnectionState previous = session_transport_->connection_state;
    if (previous == state) {
        return;
    }
    session_transport_->connection_state = state;
    if (options_.connection_event_handler) {
        options_.connection_event_handler(ReplicationClientConnectionEvent{
            previous,
            state,
            client == invalid_client_id ? client_id_ : client,
            std::move(reason)});
    }
}

ReplicationClientConnectionState ReplicationClient::connection_state() const noexcept {
    return session_transport_->connection_state;
}

const std::string& ReplicationClient::connect_error() const noexcept {
    return session_transport_->connect_error;
}

bool ReplicationClient::receive(ashiato::Registry& registry, ashiato::BitBuffer packet) {
    const ReceiveContext context{
        clock_.local_time_seconds(),
        clock_.estimated_server_time_seconds(),
        clock_.estimated_server_frame(),
        clock_.buffered_frame(),
        clock_.continuous_buffered_frame(),
        clock_.continuous_predicted_frame()};

    try {
        detail::BitReader reader(packet);
        std::uint8_t message = 0;
        if (!reader.read_bits(protocol::message_bits, message)) {
            log_server_packet_warning(0, "missing_message_id", "packet_missing_message_id");
            return false;
        }
        if (message == protocol::server_connect_response_message) {
            const bool received = receive_connect_response(registry, packet);
            if (!received) {
                log_server_packet_warning(message, "malformed_connect_response", "malformed_connect_response");
            }
            return received;
        }
        if (message == protocol::server_pong_message) {
            const bool received = receive_pong(registry, packet, context);
            if (!received) {
                log_server_packet_warning(message, "malformed_pong", "malformed_pong");
            }
            return received;
        }
        if (message == protocol::server_update_message) {
            const bool received = receive_entity_update(registry, packet, context);
            if (!received) {
                log_server_packet_warning(message, "malformed_server_update", "malformed_server_update");
            }
            return received;
        }
        log_server_packet_warning(message, "unknown_message", "unknown_server_message");
        return false;
    } catch (const std::out_of_range& ex) {
        log_server_packet_warning(0, "decode_exception", ex.what());
        return false;
    }
}

void ReplicationClient::log_info(const char* event, const std::string& fields) const {
    if (logger_ != nullptr && logger_->should_log(spdlog::level::info)) {
        logger_->info("event={} client={} {}", event, client_id_, fields);
    }
}

void ReplicationClient::log_server_packet_warning(
    std::uint8_t message,
    const char* reason_code,
    const char* reason_detail) {
    ++observability_stats_.server_packet_warnings;
    const std::uint32_t max_logs = options_.logging.max_warning_logs_per_source;
    std::uint32_t& logged_count = warning_logs_by_message_[message];
    if (max_logs != 0U && logged_count >= max_logs) {
        ++observability_stats_.suppressed_server_packet_warnings;
        if (logged_count == max_logs) {
            ++logged_count;
            if (logger_ != nullptr && logger_->should_log(spdlog::level::warn)) {
                logger_->warn(
                    "event=server_packet_warnings_suppressed client={} buffered_frame={} predicted_frame={} message_id={} max_logs={}",
                    client_id_,
                    clock_.buffered_frame(),
                    clock_.predicted_frame(),
                    message,
                    max_logs);
            }
        }
        return;
    }
    ++logged_count;
    if (logger_ != nullptr && logger_->should_log(spdlog::level::warn)) {
        logger_->warn(
            "event=server_packet_rejected client={} buffered_frame={} predicted_frame={} message_id={} reason={} detail={}",
            client_id_,
            clock_.buffered_frame(),
            clock_.predicted_frame(),
            message,
            reason_code,
            detail::log_token(reason_detail));
    }
}

void ReplicationClient::report_input_truncation(std::uint64_t truncated_packets_before) {
    observability_stats_.input_packets_truncated = input_->truncated_packets();
    observability_stats_.input_frames_truncated = input_->truncated_frames();
    if (input_->truncated_packets() == truncated_packets_before) {
        return;
    }
    // The same budget as a rejected server packet, under a message id no server message uses.
    constexpr std::uint8_t input_truncation_source = 0xFFU;
    const std::uint32_t max_logs = options_.logging.max_warning_logs_per_source;
    std::uint32_t& logged_count = warning_logs_by_message_[input_truncation_source];
    if (max_logs != 0U && logged_count >= max_logs) {
        return;
    }
    ++logged_count;
    if (logger_ != nullptr && logger_->should_log(spdlog::level::warn)) {
        logger_->warn(
            "event=input_packet_truncated client={} predicted_frame={} mtu_bytes={} packets_truncated={} "
            "frames_truncated={}",
            client_id_,
            clock_.predicted_frame(),
            options_.network.mtu_bytes,
            input_->truncated_packets(),
            input_->truncated_frames());
    }
}

void ReplicationClient::log_client_error(std::uint8_t message, const char* event, const char* reason) {
    ++observability_stats_.client_errors;
    if (logger_ != nullptr && logger_->should_log(spdlog::level::err)) {
        logger_->error(
            "event={} client={} buffered_frame={} predicted_frame={} message_id={} reason={}",
            event,
            client_id_,
            clock_.buffered_frame(),
            clock_.predicted_frame(),
            message,
            detail::log_token(reason));
    }
}

bool ReplicationClient::set_default_entity_mode(ReplicationClientMode mode) noexcept {
    options_.entities.default_mode = mode;
    return true;
}

void ReplicationClient::set_entity_mode(
    ashiato::Registry& registry,
    ClientEntityNetworkId client_entity_network_id,
    ReplicationClientMode mode) {
    EntityState* state = find_entity_state(client_entity_network_id);
    if (state == nullptr) {
        throw ClientError(ClientStatus::EntityNotFound, "client entity network id is not alive");
    }

    if (!state->replication.entity_present && !state->identity.local) {
        throw ClientError(ClientStatus::EntityUnavailable, "client entity is not currently present");
    }
    if (state->mode.current == mode) {
        return;
    }

    const SyncSettings& settings = registry.get<SyncSettings>();
    if (mode == ReplicationClientMode::Predict) {
        (void)validate_predicted_archetype(settings, state->identity.archetype);
    }
    if (mode == ReplicationClientMode::Predict && !has_predicted_entities()) {
        prediction_->reset_predicted_frame();
    }
#ifdef ASHIATO_SYNC_ENABLE_TRACING
    const ReplicationClientMode previous_mode = state->mode.current;
#endif
    const bool switched = switch_entity_mode(registry, settings, *state, mode);
    if (switched) {
        sync_entity_memberships(*state);
#ifdef ASHIATO_SYNC_ENABLE_TRACING
        if (tracer_ != nullptr && tracer_->enabled() && previous_mode != state->mode.current) {
            SyncTraceEvent event = make_client_trace_event(SyncTraceEventType::ModeChanged, client_id_, state->replication.frame);
            event.server_entity = ashiato::Entity{state->identity.client_entity_network_id};
            event.local_entity = state->identity.local;
            event.client_network_id = state->identity.client_entity_network_id;
            event.wire_network_id = state->identity.wire_network_id;
            event.network_version = client_entity_network_id_version(state->identity.client_entity_network_id);
            event.archetype = state->identity.archetype;
            event.previous_mode = previous_mode;
            event.mode = state->mode.current;
            tracer_->trace(event);
        }
#endif
    }
    if (switched && mode != ReplicationClientMode::Predict && !has_predicted_entities()) {
        prediction_->reset_predicted_frame();
    }
    if (!switched) {
        throw ClientError(ClientStatus::ModeSwitchFailed, "client entity mode switch failed");
    }
}

ReplicationClientMode ReplicationClient::entity_mode(ClientEntityNetworkId client_entity_network_id) const noexcept {
    const EntityState* state = find_entity_state(client_entity_network_id);
    return state != nullptr ? state->mode.current : options_.entities.default_mode;
}

bool ReplicationClient::has_entity(ClientEntityNetworkId client_entity_network_id) const noexcept {
    return find_entity_state(client_entity_network_id) != nullptr;
}

bool ReplicationClient::set_buffered_frame_lag(SyncFrame frames) noexcept {
    if (!clock_.set_buffered_frame_lag(frames)) {
        return false;
    }
    options_.buffered.buffered_frame_lag = frames;
    return true;
}

SyncFrame ReplicationClient::current_buffered_frame_lag() const noexcept {
    return clock_.buffered_frame_lag();
}

double ReplicationClient::estimated_server_frame() const noexcept {
    if (!has_received_server_update_) {
        return 0.0;
    }
    return clock_.estimated_server_frame();
}

double ReplicationClient::continuous_prediction_frames_ahead() const noexcept {
    if (!has_received_server_update_) {
        return 0.0;
    }
    return clock_.continuous_predicted_frame() - estimated_server_frame();
}

double ReplicationClient::continuous_buffered_frames_behind() const noexcept {
    if (!has_received_server_update_) {
        return 0.0;
    }
    return clock_.estimated_server_frame() - clock_.continuous_buffered_frame();
}

bool ReplicationClient::has_applied_buffered_frame() const noexcept {
    return buffered_runtime_->has_applied_frame();
}

SyncFrame ReplicationClient::last_applied_buffered_frame() const noexcept {
    return buffered_runtime_->last_applied_frame();
}

void ReplicationClient::set_packet_sender(std::function<void(const ashiato::BitBuffer&)> sender) {
    session_transport_->packet_sender = std::move(sender);
}

void ReplicationClient::receive_packet(ashiato::BitBuffer packet) {
    session_transport_->inbound_packets.push_back(std::move(packet));
}

bool ReplicationClient::tick(
    ashiato::Registry& registry,
    double dt_seconds,
    const ashiato::RunJobsOptions& prediction_options) {
    ASHIATO_SYNC_PROFILE_SCOPE("AshiatoSync_ClientTick");

    if (dt_seconds < 0.0 || !std::isfinite(dt_seconds)) {
        return false;
    }

#ifdef ASHIATO_SYNC_ENABLE_TRACING
    const auto previous_local_frame = static_cast<SyncFrame>(
        std::floor(clock_.local_time_seconds() / fixed_dt_seconds_));
#endif

    {
        ASHIATO_SYNC_PROFILE_SCOPE("AshiatoSync_ClientAdvanceLocalTime");
        clock_.advance_local_time(dt_seconds);
    }

#ifdef ASHIATO_SYNC_ENABLE_TRACING
    ReplicationClientClock::FrameRange local_time_frames;
    const auto current_local_frame = static_cast<SyncFrame>(
        std::floor(clock_.local_time_seconds() / fixed_dt_seconds_));
    if (current_local_frame > previous_local_frame) {
        local_time_frames.first = previous_local_frame + 1U;
        local_time_frames.last = current_local_frame;
    }
#endif

    session_transport_->connect_resend_accumulator_seconds += dt_seconds;
    session_transport_->ping_accumulator_seconds += dt_seconds;

    process_inbound_packets(registry);

    ReplicationClientClock::AdvanceResult current_frame_numbers;
    {
        ASHIATO_SYNC_PROFILE_SCOPE("AshiatoSync_ClientAdvanceFrameNumbers");
        current_frame_numbers = clock_.advance_client_frame_numbers(dt_seconds);
    }
    apply_buffered_frames_to_ashiato(registry, current_frame_numbers.buffered);
    if (!run_predicted_frames(registry, current_frame_numbers.predicted, prediction_options)) {
        return false;
    }
    if (!prediction_->run_catchup(
            *this,
            registry,
            frame_range_count(current_frame_numbers.predicted),
            prediction_options)) {
        return false;
    }

    {
        ASHIATO_SYNC_PROFILE_SCOPE("AshiatoSync_ClientUpdateDisplayTarget");
        clock_.update_display_target(dt_seconds);
    }
#ifdef ASHIATO_SYNC_ENABLE_TRACING
    trace_local_time_frames(registry, local_time_frames);
#endif
    send_pending_packets();
    return true;
}

void ReplicationClient::apply_buffered_frames_to_ashiato(
    ashiato::Registry& registry,
    const ReplicationClientClock::FrameRange& frames) {
    ASHIATO_SYNC_PROFILE_SCOPE("AshiatoSync_ClientApplyBufferedFrames");

    (void)buffered_runtime_->apply_frames(*this, registry, frames);
}

bool ReplicationClient::run_predicted_frames(
    ashiato::Registry& registry,
    const ReplicationClientClock::FrameRange& frames,
    const ashiato::RunJobsOptions& options) {
    ASHIATO_SYNC_PROFILE_SCOPE("AshiatoSync_ClientRunPredictedFrames");

    for (SyncFrame frame = frames.first; !frames.empty() && frame <= frames.last; ++frame) {
        const SyncSettings& settings = registry.get<SyncSettings>();
        (void)record_input_frame(registry, settings, frame);
        const bool has_predicted = has_predicted_entities();
        const bool should_run = prediction_->should_run_prediction_frame(frame);
        if (has_predicted && should_run && !prediction_->run_frame(*this, registry, frame, options)) {
            return false;
        }
    }
    return true;
}

#ifdef ASHIATO_SYNC_ENABLE_TRACING
void ReplicationClient::trace_local_time_frames(
    ashiato::Registry& registry,
    const ReplicationClientClock::FrameRange& frames) {
    if (!frames.empty()) {
        const SyncSettings& settings = registry.get<SyncSettings>();
        for (SyncFrame frame = frames.first; frame <= frames.last; ++frame) {
            trace_frame_components(
                registry,
                settings,
                frame,
                false,
                false,
                TraceFrameComponentScope::NonPredicted);
            if (frame == frames.last) {
                break;
            }
        }
    }
}
#endif

bool ReplicationClient::apply_frame(ashiato::Registry& registry, SyncFrame buffered_frame) {
    return buffered_runtime_->apply_frame(*this, registry, buffered_frame);
}

#ifdef ASHIATO_SYNC_ENABLE_INTERPOLATION_DIAGNOSTICS
void ReplicationClient::record_interpolation_frame(std::uint64_t checks, std::uint64_t starvations) noexcept {
    interpolation_diagnostics_.record_frame(checks, starvations);
}
#endif

const FractionalTickSampleBuffer& ReplicationClient::fractional_tick_frame(const ashiato::Registry& registry) {
    ASHIATO_SYNC_PROFILE_SCOPE("AshiatoSync_ClientFractionalTickFrame");

    const double display_accumulator_seconds = clock_.consume_display_accumulator_seconds();
    const float display_dt = display_accumulator_seconds > 0.0 && std::isfinite(display_accumulator_seconds)
        ? static_cast<float>(display_accumulator_seconds)
        : 0.0f;
    if (display_dt > 0.0f) {
        ASHIATO_SYNC_PROFILE_SCOPE("AshiatoSync_ClientBlendSnapErrors");
        blend_snap_errors(registry.get<SyncSettings>(), display_dt);
    }
    bool sampled_fractional_tick = false;
    {
        ASHIATO_SYNC_PROFILE_SCOPE("AshiatoSync_ClientSampleFractionalTickFrame");
        sampled_fractional_tick = sample_fractional_tick_frame(registry, clock_.display_target_frame(), fractional_tick_scratch_);
    }
    if (sampled_fractional_tick || fractional_tick_frame_.entities.empty()) {
        fractional_tick_frame_.entities.swap(fractional_tick_scratch_.entities);
    }
    return fractional_tick_frame_;
}

bool ReplicationClient::ensure_local_entity(ashiato::Registry& registry, EntityState& state) {
    if (!state.identity.local || !registry.alive(state.identity.local)) {
        unregister_local_entity_index(state);
        state.identity.local = registry.create();
    }
    register_local_entity_index(state);
    const SyncSettings& settings = registry.get<SyncSettings>();
    if (state.identity.archetype.value < settings.archetypes.size()) {
        set_sync_debug_name(registry, state.identity.local, settings.archetypes[state.identity.archetype.value]);
    }
    return state.identity.local && registry.alive(state.identity.local);
}

std::uint64_t ReplicationClient::registry_tag_mask(
    const ashiato::Registry& registry,
    ashiato::Entity entity,
    const SyncArchetype& archetype) const {
    std::uint64_t mask = 0;
    if (!entity || !registry.alive(entity)) {
        return mask;
    }
    for (std::size_t tag_index = 0; tag_index < archetype.tags.size(); ++tag_index) {
        if (registry.has(entity, archetype.tags[tag_index].tag)) {
            mask |= std::uint64_t{1} << tag_index;
        }
    }
    return mask;
}

void ReplicationClient::trace_applied_tag_delta(
    const SyncArchetype& archetype,
    const EntityState& state,
    SyncFrame frame,
    std::uint64_t previous_tag_mask,
    std::uint64_t next_tag_mask) {
#ifdef ASHIATO_SYNC_ENABLE_TRACING
    if (tracer_ == nullptr || !tracer_->enabled() || previous_tag_mask == next_tag_mask) {
        return;
    }
    for (std::size_t tag_index = 0; tag_index < archetype.tags.size(); ++tag_index) {
        const std::uint64_t bit = std::uint64_t{1} << tag_index;
        if ((previous_tag_mask & bit) == (next_tag_mask & bit)) {
            continue;
        }
        SyncTraceEvent event = make_client_trace_event(SyncTraceEventType::TagApplied, client_id_, frame);
        event.server_entity = ashiato::Entity{state.identity.client_entity_network_id};
        event.local_entity = state.identity.local;
        event.client_network_id = state.identity.client_entity_network_id;
        event.wire_network_id = state.identity.wire_network_id;
        event.network_version = client_entity_network_id_version(state.identity.client_entity_network_id);
        event.archetype = state.identity.archetype;
        event.tag = archetype.tags[tag_index].tag;
        event.remove = (next_tag_mask & bit) == 0U;
        tracer_->trace(event);
    }
#else
    (void)archetype;
    (void)state;
    (void)frame;
    (void)previous_tag_mask;
    (void)next_tag_mask;
#endif
}

bool ReplicationClient::apply_registry_tags(
    ashiato::Registry& registry,
    const SyncArchetype& archetype,
    EntityState& state,
    SyncFrame frame,
    std::uint64_t tag_mask,
    bool verify_tag_apply) {
    std::uint64_t previous_tag_mask = 0;
#ifdef ASHIATO_SYNC_ENABLE_TRACING
    if (tracer_ != nullptr && tracer_->enabled()) {
        previous_tag_mask = registry_tag_mask(registry, state.identity.local, archetype);
    }
#endif
    if (!apply_archetype_tags(registry, state.identity.local, archetype, tag_mask)) {
        return false;
    }
    if (verify_tag_apply && !archetype.tags.empty()) {
        const std::uint64_t after_tag_mask = registry_tag_mask(registry, state.identity.local, archetype);
        if (after_tag_mask != tag_mask) {
            std::ostringstream fields;
            fields << "frame=" << frame
                   << " client=" << client_id_
                   << " network=" << state.identity.client_entity_network_id
                   << " local=" << state.identity.local.value
                   << " archetype=" << state.identity.archetype.value
                   << " sample_mask=0x" << std::hex << tag_mask
                   << " registry_mask=0x" << after_tag_mask << std::dec
                   << " tag_count=" << archetype.tags.size();
            ++observability_stats_.client_errors;
            if (logger_ != nullptr && logger_->should_log(spdlog::level::err)) {
                logger_->error("event=tag_apply_mismatch {}", fields.str());
            }
        }
    }
    trace_applied_tag_delta(archetype, state, frame, previous_tag_mask, tag_mask);
    return true;
}

bool ReplicationClient::remove_missing_registry_components(
    ashiato::Registry& registry,
    const SyncArchetype& archetype,
    const EntityState& state,
    SyncFrame frame,
    std::uint64_t previous_present_mask,
    std::uint64_t next_present_mask) {
#ifndef ASHIATO_SYNC_ENABLE_TRACING
    (void)frame;
#endif
    for (std::size_t component_index = 0; component_index < archetype.components.size(); ++component_index) {
        const std::uint64_t bit = std::uint64_t{1} << component_index;
        if ((previous_present_mask & bit) == 0U || (next_present_mask & bit) != 0U) {
            continue;
        }
        registry.remove(state.identity.local, archetype.components[component_index].component);
#ifdef ASHIATO_SYNC_ENABLE_TRACING
        if (tracer_ != nullptr && tracer_->enabled()) {
            SyncTraceEvent event = make_client_trace_event(SyncTraceEventType::ComponentRemoved, client_id_, frame);
            event.server_entity = ashiato::Entity{state.identity.client_entity_network_id};
            event.local_entity = state.identity.local;
            event.client_network_id = state.identity.client_entity_network_id;
            event.wire_network_id = state.identity.wire_network_id;
            event.network_version = client_entity_network_id_version(state.identity.client_entity_network_id);
            event.archetype = state.identity.archetype;
            event.component = archetype.components[component_index].component;
            event.remove = true;
            append_trace_component_name(archetype, component_index, event);
            tracer_->trace(event);
        }
#endif
    }
    return true;
}

bool ReplicationClient::apply_registry_frame(
    ashiato::Registry& registry,
    const SyncArchetype& archetype,
    EntityState& state,
    const RegistryFrameApplyInfo& input) {
    if (!ensure_local_entity(registry, state)) {
        return false;
    }
    if (!apply_registry_tags(
            registry,
            archetype,
            state,
            input.frame,
            input.frame_data.tag_mask,
            input.verify_tag_apply)) {
        return false;
    }
    if (input.remove_missing_components &&
        !remove_missing_registry_components(
            registry,
            archetype,
            state,
            input.frame,
            input.previous_present_mask,
            input.next_present_mask)) {
        return false;
    }

    return client_detail::for_each_present_component(
        archetype,
        input.frame_data,
        [&](std::size_t, const ComponentReplication&, const SyncComponentOps& ops, const std::uint8_t* bytes) {
            if (ops.serialization.push_to_registry == nullptr ||
                !ops.serialization.push_to_registry(registry, state.identity.local, bytes)) {
                return false;
            }
            return true;
        });
}

bool ReplicationClient::validate_buffered_archetype(const SyncSettings& settings, SyncArchetypeId archetype) const {
    if (archetype.value >= settings.archetypes.size()) {
        return false;
    }
    const SyncArchetype& definition = settings.archetypes[archetype.value];
    for (std::size_t index = 0; index < definition.components.size(); ++index) {
        const ComponentReplication& replication = definition.components[index];
        if (replication.interpolation != ComponentInterpolation::Interpolate) {
            continue;
        }
        if (index >= definition.component_ops.size() || definition.component_ops[index].interpolate == nullptr) {
            return false;
        }
    }
    return true;
}

bool ReplicationClient::validate_predicted_archetype(const SyncSettings& settings, SyncArchetypeId archetype) const {
    if (archetype.value >= settings.archetypes.size()) {
        return false;
    }
    const SyncArchetype& definition = settings.archetypes[archetype.value];
    for (std::size_t index = 0; index < definition.components.size(); ++index) {
        if (index >= definition.component_ops.size() ||
            definition.component_ops[index].should_roll_back == nullptr) {
            throw missing_prediction_rollback_trait_error(archetype, definition, index);
        }
    }
    return true;
}

bool ReplicationClient::fill_buffered_frames(
    const SyncSettings& settings,
    EntityState& state,
    SyncFrame frame,
    bool entity_present,
    QuantizedFrameData& decoded) {
    if (state.replication.frame != 0 && frame <= state.replication.frame) {
        return false;
    }

    const QuantizedFrameData* from = state.replication.frame != 0 ? &state.replication.baseline : nullptr;
    const bool from_entity_present = state.identity.local || (from != nullptr && from->present_mask != 0U);
    const SyncFrame from_frame = state.replication.frame;
    const SyncFrame begin = state.replication.frame != 0 ? state.replication.frame + 1U : frame;
    for (SyncFrame current = begin; current <= frame; ++current) {
        const bool current_present = current == frame ? entity_present : from_entity_present;
        const QuantizedFrameData* to = entity_present ? &decoded : nullptr;
        const bool final_absent = current == frame && !entity_present;
        if (!write_buffered_frame(
                settings,
                state,
                current,
                final_absent ? false : current_present,
                from,
                to,
                from_frame,
                frame)) {
            return false;
        }
        if (current == frame) {
            break;
        }
    }
    return true;
}

bool ReplicationClient::write_buffered_frame(
    const SyncSettings& settings,
    EntityState& state,
    SyncFrame frame,
    bool entity_present,
    const QuantizedFrameData* from,
    const QuantizedFrameData* to,
    SyncFrame from_frame,
    SyncFrame to_frame) {
    const std::uint32_t entity_index = entity_store_->index_of(state);
    const SyncArchetype& archetype = settings.archetypes[state.identity.archetype.value];
    if (!client_detail::valid_frame_archetype(archetype)) {
        return false;
    }
    MutableEntityFrameView sample =
        buffered_runtime_->frames().begin_write(
            entity_index,
            frame,
            archetype.total_quantized_bytes,
            FrameWriteSource::BufferedFrame);
    *sample.valid = true;
    *sample.entity_present = entity_present;

    const auto finish_buffered_frame = [&]() {
        const detail::FrameDataView current{
            *sample.baseline.tag_mask,
            *sample.baseline.present_mask,
            sample.baseline.bytes,
            sample.baseline.byte_count};
        EntityFrameView previous;
        if (frame != 0U &&
            buffered_runtime_->frames().view(entity_index, frame - 1U, previous) &&
            previous.entity_present) {
            *sample.component_apply_mask = detail::changed_present_component_mask(
                archetype,
                previous.baseline,
                current);
        } else {
            *sample.component_apply_mask = current.present_mask;
        }
    };

    if (!entity_present) {
        return true;
    }

    const bool final_frame = frame == to_frame;
    if (final_frame && to != nullptr) {
        *sample.baseline.tag_mask = to->tag_mask;
        *sample.baseline.present_mask = to->present_mask;
        if (to->bytes.size() != sample.baseline.byte_count) {
            return false;
        }
        if (!to->bytes.empty()) {
            std::memcpy(sample.baseline.bytes, to->bytes.data(), to->bytes.size());
        }
        finish_buffered_frame();
        return true;
    }
    if (from == nullptr) {
        finish_buffered_frame();
        return true;
    }
    *sample.baseline.tag_mask = from->tag_mask;

    for (std::size_t component_index = 0; component_index < archetype.components.size(); ++component_index) {
        if (!frame_has_component(*from, component_index)) {
            continue;
        }
        const SyncComponentOps& ops = archetype.component_ops[component_index];
        const std::uint8_t* previous = frame_component_data(archetype, *from, component_index);
        std::uint8_t* value = mutable_frame_component_data(archetype, sample.baseline, component_index);
        if (previous == nullptr || value == nullptr) {
            return false;
        }
        std::memcpy(value, previous, ops.serialization.quantized_size);
        if (to != nullptr &&
            frame_has_component(*to, component_index) &&
            archetype.components[component_index].interpolation == ComponentInterpolation::Interpolate) {
            if (component_index >= archetype.component_ops.size() ||
                ops.interpolate == nullptr ||
                to_frame == from_frame) {
                return false;
            }
            const std::uint8_t* next = frame_component_data(archetype, *to, component_index);
            if (next == nullptr) {
                return false;
            }
            const float alpha =
                static_cast<float>(frame - from_frame) / static_cast<float>(to_frame - from_frame);
            if (!ops.interpolate(previous, next, alpha, value)) {
                return false;
            }
        }
    }

    finish_buffered_frame();
    return true;
}

bool ReplicationClient::apply_buffered_sample(
    ashiato::Registry& registry,
    const SyncSettings& settings,
    EntityState& state,
    const EntityFrameView& sample) {
    if (!sample.entity_present) {
        if (state.identity.local) {
            unregister_local_entity_index(state);
            if (registry.alive(state.identity.local)) {
                registry.destroy(state.identity.local);
            }
        }
        state.identity.local = ashiato::Entity{};
        state.replication.applied_present_mask = 0;
        state.visual.snap_errors.clear();
        if (sample.write_source == FrameWriteSource::BufferedFrame) {
            state.mode.last_applied_buffered_frame = sample.frame;
        }
        return true;
    }

    const SyncArchetype& archetype = settings.archetypes[state.identity.archetype.value];
    std::uint64_t component_apply_mask = sample.baseline.present_mask;
    if (sample.write_source == FrameWriteSource::BufferedFrame &&
        state.mode.last_applied_buffered_frame + 1U == sample.frame) {
        component_apply_mask = sample.component_apply_mask;
    }
    const detail::FrameDataView frame{
        sample.baseline.tag_mask,
        sample.baseline.present_mask & component_apply_mask,
        sample.baseline.bytes,
        sample.baseline.byte_count};
    const RegistryFrameApplyInfo input{
        sample.frame,
        frame,
        state.replication.applied_present_mask,
        sample.baseline.present_mask,
        true,
        true};
    if (!apply_registry_frame(registry, archetype, state, input)) {
        return false;
    }
    state.replication.applied_present_mask = sample.baseline.present_mask;
    if (sample.write_source == FrameWriteSource::BufferedFrame) {
        state.mode.last_applied_buffered_frame = sample.frame;
    }
    return true;
}

bool ReplicationClient::apply_frame_data(
    ashiato::Registry& registry,
    const SyncSettings& settings,
    EntityState& state,
    SyncFrame frame,
    bool entity_present,
    const QuantizedFrameData& baseline) {
    EntityFrameView sample;
    sample.frame = frame;
    sample.valid = true;
    sample.entity_present = entity_present;
    sample.write_generation = 0U;
    sample.write_source = FrameWriteSource::Unknown;
    sample.baseline = detail::FrameDataView{
        baseline.tag_mask,
        baseline.present_mask,
        baseline.bytes.empty() ? nullptr : baseline.bytes.data(),
        baseline.bytes.size()};
    return apply_buffered_sample(registry, settings, state, sample);
}

bool ReplicationClient::quantize_predicted_entity(
    const ashiato::Registry& registry,
    const SyncSettings& settings,
    EntityState& state,
    SyncFrame frame,
    FrameWriteSource source) {
    if (state.identity.archetype.value >= settings.archetypes.size()) {
        return false;
    }
    const std::uint32_t entity_index = entity_store_->index_of(state);
    prediction_->ensure_entity(entity_index);

    const SyncArchetype& archetype = settings.archetypes[state.identity.archetype.value];
    if (!client_detail::valid_frame_archetype(archetype)) {
        return false;
    }
    MutableEntityFrameView sample =
        prediction_->frames().begin_write(
            entity_index,
            frame,
            archetype.total_quantized_bytes,
            source);
    *sample.valid = true;
    *sample.entity_present = state.identity.local && registry.alive(state.identity.local);
    if (!*sample.entity_present) {
        return true;
    }

    for (std::size_t tag_index = 0; tag_index < archetype.tags.size(); ++tag_index) {
        if (registry.has(state.identity.local, archetype.tags[tag_index].tag)) {
            *sample.baseline.tag_mask |= std::uint64_t{1} << tag_index;
        }
    }

    for (std::size_t component_index = 0; component_index < archetype.components.size(); ++component_index) {
        const ComponentReplication& replication = archetype.components[component_index];
        const void* value = registry.get(state.identity.local, replication.component);
        if (value == nullptr) {
            continue;
        }
        if (component_index >= archetype.component_ops.size()) {
            return false;
        }
        const SyncComponentOps& ops = archetype.component_ops[component_index];
        if (ops.serialization.quantize == nullptr) {
            return false;
        }
        std::uint8_t* out = unchecked_mutable_frame_component_data(archetype, sample.baseline, component_index);
        ops.serialization.quantize(value, out);
    }
    return true;
}

bool ReplicationClient::compare_predicted_frame(
    const SyncSettings& settings,
    EntityState& state,
    SyncFrame frame,
    const QuantizedFrameData& authoritative) const {
    const std::uint32_t entity_index = entity_store_->index_of(state);
    if (state.identity.archetype.value >= settings.archetypes.size() || prediction_->frames().empty(entity_index)) {
        return false;
    }
    EntityFrameView predicted;
    if (!prediction_->frames().view(entity_index, frame, predicted)) {
        return false;
    }
    if (!predicted.entity_present) {
        return true;
    }
    const SyncArchetype& archetype = settings.archetypes[state.identity.archetype.value];
    constexpr std::size_t invalid_component_index = static_cast<std::size_t>(-1);
    auto rollback_conflict = [&](bool condition,
                                 std::size_t component_index,
                                 ashiato::Entity component,
                                 const std::uint8_t* component_bytes) {
        if (!condition) {
            return false;
        }
#ifdef ASHIATO_SYNC_ENABLE_TRACING
        if (tracer_ != nullptr && tracer_->enabled()) {
            SyncTraceEvent event = make_client_trace_event(
                SyncTraceEventType::PredictionRollbackConflict,
                client_id_,
                frame);
            event.server_entity = ashiato::Entity{state.identity.client_entity_network_id};
            event.local_entity = state.identity.local;
            event.client_network_id = state.identity.client_entity_network_id;
            event.wire_network_id = state.identity.wire_network_id;
            event.network_version = client_entity_network_id_version(state.identity.client_entity_network_id);
            event.archetype = state.identity.archetype;
            event.component = component;
            if (component_index != invalid_component_index && component_bytes != nullptr) {
                append_trace_component_data(tracer_, archetype, component_index, component_bytes, event);
            }
            tracer_->trace(event);
        }
#else
        (void)component_index;
        (void)component;
        (void)component_bytes;
#endif
        return true;
    };
    if (rollback_conflict(predicted.baseline.tag_mask != authoritative.tag_mask, invalid_component_index, {}, nullptr) ||
        rollback_conflict(predicted.baseline.present_mask != authoritative.present_mask, invalid_component_index, {}, nullptr) ||
        rollback_conflict(
            predicted.baseline.byte_count < archetype.total_quantized_bytes,
            invalid_component_index,
            {},
            nullptr) ||
        rollback_conflict(
            authoritative.bytes.size() < archetype.total_quantized_bytes,
            invalid_component_index,
            {},
            nullptr)) {
        return true;
    }
    for (std::size_t component_index = 0; component_index < archetype.components.size(); ++component_index) {
        if (!frame_has_component(authoritative, component_index)) {
            continue;
        }
        if (rollback_conflict(component_index >= archetype.component_ops.size(), invalid_component_index, {}, nullptr)) {
            return true;
        }
        const SyncComponentOps& ops = archetype.component_ops[component_index];
        if (ops.should_roll_back == nullptr) {
            throw missing_prediction_rollback_trait_error(state.identity.archetype, archetype, component_index);
        }
        const std::uint8_t* predicted_bytes =
            unchecked_frame_component_data(archetype, predicted.baseline, component_index);
        const std::uint8_t* authoritative_bytes =
            unchecked_frame_component_data(archetype, authoritative, component_index);
#ifdef ASHIATO_SYNC_ENABLE_TRACING
        RollbackReasonTraceContext rollback_reason_context;
        rollback_reason_context.tracer = tracer_;
        rollback_reason_context.client = client_id_;
        rollback_reason_context.frame = frame;
        rollback_reason_context.server_entity = ashiato::Entity{state.identity.client_entity_network_id};
        rollback_reason_context.local_entity = state.identity.local;
        rollback_reason_context.client_network_id = state.identity.client_entity_network_id;
        rollback_reason_context.wire_network_id = state.identity.wire_network_id;
        rollback_reason_context.network_version = client_entity_network_id_version(state.identity.client_entity_network_id);
        rollback_reason_context.archetype = state.identity.archetype;
        rollback_reason_context.component = archetype.components[component_index].component;
        rollback_reason_context.component_name = component_index < archetype.component_ops.size()
            ? archetype.component_ops[component_index].serialization.name
            : std::string{};
        ScopedRollbackReasonTraceContext scoped_rollback_reason_context(rollback_reason_context);
#endif
        if (rollback_conflict(
                ops.should_roll_back(predicted_bytes, authoritative_bytes),
                component_index,
                archetype.components[component_index].component,
                authoritative_bytes)) {
            return true;
        }
    }
    return false;
}

bool ReplicationClient::apply_snap_sample(
    ashiato::Registry& registry,
    const SyncSettings& settings,
    EntityState& state,
    const QuantizedFrameData& authoritative,
    std::uint64_t component_apply_mask) {
    const SyncArchetype& archetype = settings.archetypes[state.identity.archetype.value];
    if (state.replication.baseline.bytes.size() != archetype.total_quantized_bytes &&
        !init_frame_data(archetype, state.replication.baseline)) {
        return false;
    }
    const detail::FrameDataView frame{
        authoritative.tag_mask,
        authoritative.present_mask & component_apply_mask,
        authoritative.bytes.empty() ? nullptr : authoritative.bytes.data(),
        authoritative.bytes.size()};
    const RegistryFrameApplyInfo input{
        state.replication.frame,
        frame,
        state.replication.applied_present_mask,
        authoritative.present_mask,
        true,
        false};
    if (!apply_registry_frame(registry, archetype, state, input)) {
        return false;
    }

    state.visual.snap_errors.erase(
        std::remove_if(
            state.visual.snap_errors.begin(),
            state.visual.snap_errors.end(),
            [&](const client_detail::EntityComponentError& existing) {
                const auto found_component = std::find_if(
                    archetype.components.begin(),
                    archetype.components.end(),
                    [&](const ComponentReplication& replication) {
                        return replication.component == existing.component;
                    });
                if (found_component == archetype.components.end()) {
                    return true;
                }
                const std::size_t component_index =
                    static_cast<std::size_t>(found_component - archetype.components.begin());
                return (authoritative.present_mask & (std::uint64_t{1} << component_index)) == 0U;
            }),
        state.visual.snap_errors.end());

    if (!client_detail::for_each_present_component(
            archetype,
            frame,
            [&](std::size_t component_index,
                const ComponentReplication& replication,
                const SyncComponentOps& ops,
                const std::uint8_t* bytes) {
                const std::uint8_t* previous_bytes =
                    frame_component_data(archetype, state.replication.baseline, component_index);
                const bool had_baseline = previous_bytes != nullptr;
                if (had_baseline &&
                    ops.compute_error != nullptr &&
                    ops.apply_error != nullptr &&
                    ops.blend_out_error != nullptr) {
                    SyncComponentOps::QuantizedBytes error;
                    if (!ops.compute_error(bytes, previous_bytes, error)) {
                        return false;
                    }

                    auto found_error = client_detail::find_snap_error(state, replication.component);
                    if (all_zero(error)) {
                        if (found_error != state.visual.snap_errors.end()) {
                            state.visual.snap_errors.erase(found_error);
                        }
                    } else if (found_error == state.visual.snap_errors.end()) {
                        state.visual.snap_errors.push_back(
                            client_detail::EntityComponentError{
                                replication.component,
                                replication.serializer,
                                std::move(error)});
                    } else {
                        found_error->serializer = replication.serializer;
                        found_error->bytes = std::move(error);
                    }
                }
                std::memcpy(
                    unchecked_mutable_frame_component_data(archetype, state.replication.baseline, component_index),
                    bytes,
                    ops.serialization.quantized_size);
                return true;
            })) {
        return false;
    }

    state.replication.entity_present = true;
    state.replication.baseline.tag_mask = authoritative.tag_mask;
    state.replication.baseline.present_mask = authoritative.present_mask;
    state.replication.applied_present_mask = authoritative.present_mask;
    sync_entity_memberships(state);
    return true;
}

bool ReplicationClient::apply_latest_snap(
    ashiato::Registry& registry,
    const SyncSettings& settings,
    EntityState& state) {
    if (!state.replication.entity_present) {
        if (state.identity.local) {
            unregister_local_entity_index(state);
            if (registry.alive(state.identity.local)) {
                registry.destroy(state.identity.local);
            }
        }
        state.identity.local = ashiato::Entity{};
        state.replication.applied_present_mask = 0;
        state.visual.snap_errors.clear();
        sync_entity_memberships(state);
        return true;
    }

    return apply_snap_sample(
        registry,
        settings,
        state,
        state.replication.baseline,
        state.replication.baseline.present_mask);
}

bool ReplicationClient::switch_entity_mode(
    ashiato::Registry& registry,
    const SyncSettings& settings,
    EntityState& state,
    ReplicationClientMode mode) {
    if (state.mode.current == mode) {
        return true;
    }

    if (mode == ReplicationClientMode::BufferedInterpolation) {
        return transition_to_buffered(settings, state);
    }
    if (mode == ReplicationClientMode::Predict) {
        return transition_to_predict(registry, settings, state);
    }
    return transition_to_snap(registry, settings, state);
}

void ReplicationClient::mark_mode_auto_selected(EntityState& state, ReplicationClientMode mode) noexcept {
    state.mode.current = mode;
    state.mode.selected = true;
}

void ReplicationClient::mark_mode_user_selected(EntityState& state, ReplicationClientMode mode) noexcept {
    state.mode.current = mode;
    state.mode.selected = true;
}

void ReplicationClient::reset_absent_entity_state(EntityState& state, SyncArchetypeId archetype) noexcept {
    state.identity.archetype = archetype;
    state.mode.current = ReplicationClientMode::Snap;
    state.mode.selected = false;
    state.mode.last_applied_buffered_frame = 0;
    state.replication.entity_present = false;
    state.replication.baseline.clear();
    state.replication.history.clear();
    state.replication.history_next = 0;
    state.replication.pinned_wire_baseline = {};
    state.replication.referenced_baseline_frame = 0;
    state.replication.has_referenced_baseline = false;
    state.replication.applied_present_mask = 0;
    sync_entity_memberships(state);
}

void ReplicationClient::record_authoritative_present(
    EntityState& state,
    SyncFrame frame,
    SyncArchetypeId archetype,
    QuantizedFrameData baseline,
    BaselineUpdate baseline_update) {
    state.identity.archetype = archetype;
    if (baseline_update == BaselineUpdate::ReplaceAndApplyMask) {
        state.replication.baseline = std::move(baseline);
        state.replication.applied_present_mask = state.replication.baseline.present_mask;
    }
    state.replication.frame = frame;
    state.replication.entity_present = true;
    remember_baseline(state);
    sync_entity_memberships(state);
}

void ReplicationClient::record_authoritative_absent(EntityState& state, SyncFrame frame) {
    state.replication.baseline.clear();
    state.replication.applied_present_mask = 0;
    state.replication.frame = frame;
    state.replication.entity_present = false;
    remember_baseline(state);
    sync_entity_memberships(state);
}

void ReplicationClient::protect_referenced_baseline(EntityState& state, SyncFrame frame) noexcept {
    state.replication.referenced_baseline_frame = frame;
    state.replication.has_referenced_baseline = true;
}

bool ReplicationClient::transition_to_buffered(const SyncSettings& settings, EntityState& state) {
    if (!validate_buffered_archetype(settings, state.identity.archetype)) {
        return false;
    }
    mark_mode_user_selected(state, ReplicationClientMode::BufferedInterpolation);
    state.mode.last_applied_buffered_frame = 0;
    state.visual.snap_errors.clear();
    const std::uint32_t entity_index = entity_store_->index_of(state);
    buffered_runtime_->ensure_entity(entity_index);
    if (state.replication.frame != 0) {
        if (!write_buffered_frame(
                settings,
                state,
                state.replication.frame,
                state.replication.entity_present,
                nullptr,
                state.replication.entity_present ? &state.replication.baseline : nullptr,
                state.replication.frame,
                state.replication.frame)) {
            return false;
        }
    }
    state.replication.applied_present_mask = state.replication.baseline.present_mask;
    sync_entity_memberships(state);
    return true;
}

bool ReplicationClient::transition_to_predict(
    ashiato::Registry& registry,
    const SyncSettings& settings,
    EntityState& state) {
    if (!validate_predicted_archetype(settings, state.identity.archetype)) {
        return false;
    }
    const ReplicationClientMode previous = state.mode.current;
    mark_mode_user_selected(state, ReplicationClientMode::Predict);
    state.visual.snap_errors.clear();
    const std::uint32_t entity_index = entity_store_->index_of(state);
    prediction_->ensure_entity(entity_index);
    if (state.replication.frame != 0) {
        if (!apply_frame_data(
                registry,
                settings,
                state,
                state.replication.frame,
                state.replication.entity_present,
                state.replication.baseline)) {
            return false;
        }
        MutableEntityFrameView sample =
            prediction_->frames().begin_write(
                entity_index,
                state.replication.frame,
                state.replication.baseline.bytes.size(),
                FrameWriteSource::AuthoritativeSeed);
        *sample.valid = true;
        *sample.entity_present = state.replication.entity_present;
        *sample.baseline.tag_mask = state.replication.baseline.tag_mask;
        *sample.baseline.present_mask = state.replication.baseline.present_mask;
        if (!state.replication.baseline.bytes.empty()) {
            std::memcpy(sample.baseline.bytes, state.replication.baseline.bytes.data(), state.replication.baseline.bytes.size());
        }
        if (state.replication.entity_present && !prediction_->has_predicted_frame()) {
            if (!prediction_->seed_existing_authoritative_frame(*this, registry, settings, state.replication.frame)) {
                return false;
            }
        } else if (state.replication.entity_present && state.replication.frame < prediction_->last_predicted_frame()) {
            // SETTLE IT FORWARD, as a newly predicted entity is (update_runtime.cpp, apply_predicted_upsert): its
            // latest authoritative frame is older than the prediction, so replay it from there.
            prediction_->queue_rollback(*this, state, state.replication.frame);
        }
    }
    finish_immediate_mode_transition(registry, settings, state, previous);
    sync_entity_memberships(state);
    return true;
}

bool ReplicationClient::transition_to_snap(ashiato::Registry& registry, const SyncSettings& settings, EntityState& state) {
    const ReplicationClientMode previous = state.mode.current;
    mark_mode_user_selected(state, ReplicationClientMode::Snap);
    if (!apply_latest_snap(registry, settings, state)) {
        state.mode.current = previous;
        return false;
    }
    finish_immediate_mode_transition(registry, settings, state, previous);
    sync_entity_memberships(state);
    return true;
}

void ReplicationClient::finish_immediate_mode_transition(
    ashiato::Registry& registry,
    const SyncSettings& settings,
    EntityState& state,
    ReplicationClientMode previous_mode) {
    const std::uint32_t entity_index = entity_store_->index_of(state);
    if (previous_mode == ReplicationClientMode::BufferedInterpolation) {
        cue_runtime_->play_buffered_on_mode_transition(
            *this,
            registry,
            settings,
            entity_index,
            state);
    }
    buffered_runtime_->clear_entity(entity_index);
    cue_runtime_->erase_buffered_for_entity(entity_index);
}

bool ReplicationClient::has_buffered_entities() const noexcept {
    return !entity_store_->buffered_entity_indices().empty();
}

bool ReplicationClient::has_predicted_entities() const noexcept {
    return !entity_store_->predicted_entity_indices().empty();
}

const ashiato::JobGraph& ReplicationClient::resim_job_graph(ashiato::Registry& registry) {
    register_components(registry);
    detail::register_predictive_simulation_job_tag(registry);

    std::vector<ashiato::Entity> predictive_jobs;
    registry.view<>()
        .with_tags({registry.job_tag(), registry.component<PredictiveSimulationJob>()})
        .each([&predictive_jobs](ashiato::Entity job) {
            predictive_jobs.push_back(job);
        });

    if (!resim_job_graph_valid_ || predictive_jobs != resim_job_graph_jobs_) {
        resim_job_graph_jobs_ = std::move(predictive_jobs);
        resim_job_graph_ = registry.compile_job_graph(resim_job_graph_jobs_);
        resim_job_graph_valid_ = true;
    }
    return resim_job_graph_;
}

void ReplicationClient::blend_snap_errors(const SyncSettings& settings, float dt_seconds) {
    for (std::size_t list_index = 0; list_index < entity_store_->snap_error_entity_indices().size();) {
        const std::uint32_t entity_index = entity_store_->snap_error_entity_indices()[list_index];
        if (entity_index >= entity_store_->entity_count()) {
            ++list_index;
            continue;
        }
        EntityState& state = entity_store_->state_unchecked(entity_index);

        for (client_detail::EntityComponentError& error : state.visual.snap_errors) {
            const SyncComponentOps* ops =
                find_update_component_ops(settings, state.identity.archetype, error.component, error.serializer);
            if (ops == nullptr || ops->blend_out_error == nullptr ||
                !ops->blend_out_error(error.bytes, dt_seconds, error.bytes)) {
                error.bytes.clear();
            }
        }

        state.visual.snap_errors.erase(
            std::remove_if(
                state.visual.snap_errors.begin(),
                state.visual.snap_errors.end(),
                [](const client_detail::EntityComponentError& error) {
                    return error.bytes.empty() || all_zero(error.bytes);
                }),
            state.visual.snap_errors.end());
        if ((state.mode.current != ReplicationClientMode::Snap && state.mode.current != ReplicationClientMode::Predict) ||
            state.visual.snap_errors.empty()) {
            entity_store_->set_snap_error_membership(entity_index, false);
        } else {
            ++list_index;
        }
    }
}

bool ReplicationClient::sample_fractional_tick_frame(
    const ashiato::Registry& registry,
    double target_frame,
    FractionalTickSampleBuffer& out) const {
    const client_detail::ClientDisplaySampler sampler(
        clock_,
        *entity_store_,
        *prediction_,
        buffered_runtime_->frames(),
        prediction_->frames(),
        prediction_->presentation_frames(),
        fractional_tick_frame_,
        fixed_dt_seconds_);
    return sampler.sample_fractional_tick_frame(registry, target_frame, out);
}

void ReplicationClient::remember_baseline(EntityState& state) {
    if (state.replication.history.size() != client_detail::max_baseline_history_per_entity) {
        state.replication.history.clear();
        state.replication.history.resize(client_detail::max_baseline_history_per_entity);
        state.replication.history_next = 0;
    }

    client_detail::EntityFrameBaseline& baseline =
        state.replication.history[state.replication.frame & (client_detail::max_baseline_history_per_entity - 1U)];
    if (state.replication.has_referenced_baseline &&
        baseline.valid &&
        baseline.frame == state.replication.referenced_baseline_frame) {
        std::swap(baseline, state.replication.pinned_wire_baseline);
    }
    baseline.frame = state.replication.frame;
    baseline.valid = true;
    baseline.baseline = state.replication.baseline;
}

}  // namespace ashiato::sync
