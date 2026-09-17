#include "ashiato/sync/server.hpp"

#include "ashiato/sync/assert.hpp"
#include "ashiato/sync/bandwidth_budget.hpp"
#include "detail/frame_data.hpp"
#include "detail/logging.hpp"
#include "detail/options_validation.hpp"
#include "server/detail.hpp"
#include "server/packet.hpp"
#include "server/state.hpp"

#include "ashiato/sync/protocol.hpp"
#include "ashiato/sync/tracing.hpp"

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
#include <memory>
#include <mutex>
#include <sstream>
#include <stdexcept>
#include <string>
#include <utility>

namespace ashiato::sync {
namespace {

struct ServerFixedStepAdvance {
    std::uint32_t steps = 0;
    std::uint64_t dropped_steps = 0;
};

class ScopedBroadcastFlag final {
public:
    explicit ScopedBroadcastFlag(bool& flag) noexcept
        : flag_(flag) {
        ASHIATO_SYNC_ASSERT(!flag_);
        flag_ = true;
    }

    ~ScopedBroadcastFlag() {
        flag_ = false;
    }

    ScopedBroadcastFlag(const ScopedBroadcastFlag&) = delete;
    ScopedBroadcastFlag& operator=(const ScopedBroadcastFlag&) = delete;

private:
    bool& flag_;
};

using detail::frame_component_data;
using detail::frame_has_component;
using detail::has_tag_slot;
using detail::init_frame_data;
using detail::mutable_frame_component_data;
using detail::sync_slot_bit;
using detail::sync_slot_count;
using server_detail::boosted_candidate_priority;
using server_detail::configured_packet_id_bits;
using server_detail::destroy_record_bits;
using server_detail::make_server_packet;
using server_detail::server_update_header_bits;

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

ServerFixedStepAdvance consume_server_fixed_steps(
    double& accumulator_seconds,
    double fixed_dt_seconds,
    std::uint32_t max_fixed_steps_per_tick) noexcept {
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
    const std::uint64_t capped_steps = max_fixed_steps_per_tick == 0U
        ? whole_steps
        : std::min<std::uint64_t>(whole_steps, max_fixed_steps_per_tick);
    accumulator_seconds = clamped_remainder;
    return ServerFixedStepAdvance{
        static_cast<std::uint32_t>(std::min<std::uint64_t>(capped_steps, std::numeric_limits<std::uint32_t>::max())),
        whole_steps - capped_steps};
}

struct OutboundPacket {
    ClientId client = invalid_client_id;
    ashiato::BitBuffer packet;
};

#ifdef ASHIATO_SYNC_ENABLE_TRACING
using server_detail::make_server_trace_event;

void append_trace_component_data(
    const SyncTracer* tracer,
    const SyncArchetype& archetype,
    std::size_t component_index,
    const std::uint8_t* bytes,
    SyncTraceEvent& event) {
    if (component_index < archetype.component_ops.size()) {
        event.component_name = archetype.component_ops[component_index].serialization.name;
    }
#ifdef ASHIATO_SYNC_TRACE_COMPONENT_DATA
    if (tracer == nullptr || !tracer->frame_data_enabled() || bytes == nullptr ||
        component_index >= archetype.component_ops.size()) {
        return;
    }
    const SyncComponentOps& ops = archetype.component_ops[component_index];
    if (ops.trace == nullptr) {
        return;
    }
    SyncTraceStringBuilder builder;
    ops.trace(bytes, builder);
    event.data = std::move(builder.value);
#else
    (void)tracer;
    (void)archetype;
    (void)component_index;
    (void)bytes;
    (void)event;
#endif
}

void append_trace_input_component_data(
    const SyncTracer* tracer,
    const SyncComponentOps& ops,
    const std::uint8_t* bytes,
    SyncTraceEvent& event) {
    event.component_name = ops.serialization.name;
#ifdef ASHIATO_SYNC_TRACE_COMPONENT_DATA
    if (tracer == nullptr || !tracer->frame_data_enabled() || bytes == nullptr || ops.trace == nullptr) {
        return;
    }
    SyncTraceStringBuilder builder;
    ops.trace(bytes, builder);
    event.data = std::move(builder.value);
#else
    (void)tracer;
    (void)ops;
    (void)bytes;
    (void)event;
#endif
}

void append_trace_cue_data(
    const SyncTracer* tracer,
    const SyncSettings& settings,
    SyncCueTypeId cue_type,
    const ashiato::BitBuffer& payload,
    SyncTraceEvent& event) {
#ifdef ASHIATO_SYNC_TRACE_COMPONENT_DATA
    if (tracer == nullptr || !tracer->frame_data_enabled() ||
        cue_type >= settings.cue_ops.size() || settings.cue_ops[cue_type].trace == nullptr) {
        return;
    }
    SyncTraceStringBuilder builder;
    if (settings.cue_ops[cue_type].trace(cue_type, settings.cue_ops[cue_type].user_data, payload, builder)) {
        event.data = std::move(builder.value);
    }
#else
    (void)tracer;
    (void)settings;
    (void)cue_type;
    (void)payload;
    (void)event;
#endif
}

#ifdef ASHIATO_SYNC_TRACE_COMPONENT_DATA
void push_trace_payload_bits_range(
    ashiato::BitBuffer& out,
    const ashiato::BitBuffer& source,
    std::uint64_t begin_bits,
    std::uint64_t end_bits) {
    const std::uint64_t source_bits = static_cast<std::uint64_t>(source.bit_size());
    begin_bits = std::min(begin_bits, source_bits);
    end_bits = std::min(end_bits, source_bits);
    if (end_bits <= begin_bits) {
        return;
    }

    ashiato::BitBuffer reader = source;
    reader.skip_bits(static_cast<std::size_t>(begin_bits));
    reader.read_buffer_bits(out, static_cast<std::size_t>(end_bits - begin_bits));
}

void push_trace_payload_scope_children(
    ScopedSerializationTraceCapture& capture,
    ashiato::BitBuffer& out,
    const ashiato::BitBuffer& source,
    const std::vector<ashiato::SerializationTraceScope>& scopes,
    std::uint32_t parent,
    std::uint64_t begin_bits,
    std::uint64_t end_bits) {
    std::uint64_t cursor_bits = begin_bits;
    for (const ashiato::SerializationTraceScope& scope : scopes) {
        if (scope.parent != parent) {
            continue;
        }

        const std::uint64_t scope_begin_bits = std::max(scope.begin_bits, begin_bits);
        const std::uint64_t scope_end_bits = std::min(scope.end_bits, end_bits);
        if (scope_end_bits <= scope_begin_bits) {
            continue;
        }
        if (scope_begin_bits > cursor_bits) {
            push_trace_payload_bits_range(out, source, cursor_bits, scope_begin_bits);
        }
        {
            ScopedSerializationTraceScope payload_scope(&capture, scope.name.c_str());
            push_trace_payload_scope_children(capture, out, source, scopes, scope.id, scope_begin_bits, scope_end_bits);
        }
        cursor_bits = std::max(cursor_bits, scope_end_bits);
    }

    if (end_bits > cursor_bits) {
        push_trace_payload_bits_range(out, source, cursor_bits, end_bits);
    }
}

void push_buffer_bits_with_replayed_trace(
    ScopedSerializationTraceCapture& capture,
    ashiato::BitBuffer& out,
    const ashiato::BitBuffer& source,
    const std::vector<ashiato::SerializationTraceScope>& scopes,
    const char* name) {
    ScopedSerializationTraceScope payload_scope(&capture, name);
    if (scopes.empty()) {
        out.write_buffer_bits(source);
        return;
    }

    const std::uint64_t source_bits = static_cast<std::uint64_t>(source.bit_size());
    std::uint64_t cursor_bits = 0;
    bool copied_root_scope = false;
    for (const ashiato::SerializationTraceScope& scope : scopes) {
        if (scope.parent != UINT32_MAX) {
            continue;
        }

        const std::uint64_t scope_begin_bits = std::min(scope.begin_bits, source_bits);
        const std::uint64_t scope_end_bits = std::min(scope.end_bits, source_bits);
        if (scope_end_bits <= scope_begin_bits) {
            continue;
        }
        if (scope_begin_bits > cursor_bits) {
            push_trace_payload_bits_range(out, source, cursor_bits, scope_begin_bits);
        }
        push_trace_payload_scope_children(capture, out, source, scopes, scope.id, scope_begin_bits, scope_end_bits);
        cursor_bits = std::max(cursor_bits, scope_end_bits);
        copied_root_scope = true;
    }

    if (!copied_root_scope) {
        push_trace_payload_scope_children(capture, out, source, scopes, UINT32_MAX, 0, source_bits);
        return;
    }
    if (source_bits > cursor_bits) {
        push_trace_payload_bits_range(out, source, cursor_bits, source_bits);
    }
}
#endif

void append_trace_data_field(SyncTraceEvent& event, const char* key, const char* value) {
    if (key == nullptr || value == nullptr || value[0] == '\0') {
        return;
    }
    if (!event.data.empty()) {
        event.data += ",";
    }
    event.data += key;
    event.data += "=";
    event.data += value;
}

void append_trace_data_field(SyncTraceEvent& event, const char* key, std::uint64_t value) {
    append_trace_data_field(event, key, std::to_string(value).c_str());
}

void append_trace_cue_name(const SyncSettings& settings, SyncCueTypeId cue_type, SyncTraceEvent& event) {
    if (cue_type < settings.cue_ops.size()) {
        event.component_name = settings.cue_ops[cue_type].name;
    }
}

#ifdef ASHIATO_SYNC_TRACE_PACKET_LOGS
std::string packet_ack_list(const std::vector<std::uint32_t>& acks) {
    std::ostringstream out;
    out << "[";
    for (std::size_t index = 0; index < acks.size(); ++index) {
        if (index != 0U) {
            out << ",";
        }
        out << acks[index];
    }
    out << "]";
    return out.str();
}
#endif

#endif

std::uint64_t visible_tag_mask(
    const ashiato::Registry& registry,
    const SyncArchetype& archetype,
    ashiato::Entity entity,
    ClientId client) {
    std::uint64_t mask = 0;
    const NetworkOwner* owner = registry.try_get<NetworkOwner>(entity);
    const ClientId owner_client = owner != nullptr ? owner->client : invalid_client_id;
    for (std::size_t tag_index = 0; tag_index < archetype.tags.size(); ++tag_index) {
        const SyncTagReplication& replication = archetype.tags[tag_index];
        if (!replication_audience_matches(replication.audience, owner_client, client)) {
            continue;
        }
        if (registry.has(entity, replication.tag)) {
            mask |= std::uint64_t{1} << tag_index;
        }
    }
    return mask;
}

}  // namespace

ReplicationServer::ReplicationServer(ashiato::Registry& registry, ReplicationServerOptions options)
    : options_(detail::validate_server_options(std::move(options))),
      registry_dirty_frame_listeners_(std::make_shared<ServerRegistryDirtyFrameSubscription::State>()),
      frame_batch_listeners_(std::make_shared<ServerFrameBatchListenerSubscription::State>()),
      logger_(detail::make_logger(options_.logging, "ashiato.sync.server")) {
    register_components(registry);
    SyncSettings& settings = registry.write<SyncSettings>();
    settings.role = SyncRole::Server;
    settings.fixed_dt_seconds = options_.fixed_dt_seconds;
    registry.write<SyncAuthority>().authoritative = true;
    set_local_client_id(registry, invalid_client_id);
    server_dirty_frame_subscription_ = registry_dirty_frame_broadcaster_.subscribe(*this);
#ifdef ASHIATO_SYNC_ENABLE_TRACING
    set_trace_options(options_.trace);
#endif
}

ReplicationServer::~ReplicationServer() {
    for (ClientState& client : clients_) {
        if (client.replication != nullptr) {
            detach_client_bandwidth_participant(*client.replication);
        }
    }
}

void ReplicationServer::set_transport(TransportFn transport) {
    options_.transport = std::move(transport);
}

void ReplicationServer::set_logger(std::shared_ptr<spdlog::logger> logger) {
    options_.logging.logger = std::move(logger);
    logger_ = detail::make_logger(options_.logging, "ashiato.sync.server");
}

void ReplicationServer::set_log_level(LogLevel level) {
    options_.logging.level = level;
    if (logger_ == nullptr) {
        logger_ = detail::make_logger(options_.logging, "ashiato.sync.server");
    } else {
        logger_->set_level(detail::to_spdlog_level(level));
    }
}

ServerRegistryDirtyFrameSubscription::ServerRegistryDirtyFrameSubscription(
    const std::shared_ptr<State>& state,
    std::uint64_t id)
    : state_(state),
      id_(id) {}

ServerRegistryDirtyFrameSubscription::ServerRegistryDirtyFrameSubscription(
    ServerRegistryDirtyFrameSubscription&& other) noexcept
    : state_(std::move(other.state_)),
      id_(other.id_) {
    other.id_ = 0;
}

ServerRegistryDirtyFrameSubscription& ServerRegistryDirtyFrameSubscription::operator=(
    ServerRegistryDirtyFrameSubscription&& other) noexcept {
    if (this != &other) {
        reset();
        state_ = std::move(other.state_);
        id_ = other.id_;
        other.id_ = 0;
    }
    return *this;
}

ServerRegistryDirtyFrameSubscription::~ServerRegistryDirtyFrameSubscription() {
    reset();
}

void ServerRegistryDirtyFrameSubscription::reset() {
    auto state = state_.lock();
    if (state == nullptr || id_ == 0U) {
        return;
    }
    for (Entry& entry : state->listeners) {
        if (entry.id == id_) {
            entry.listener = nullptr;
            break;
        }
    }
    id_ = 0;
    state_.reset();
}

bool ServerRegistryDirtyFrameSubscription::active() const noexcept {
    return id_ != 0U && !state_.expired();
}

ServerFrameBatchListenerSubscription::ServerFrameBatchListenerSubscription(
    const std::shared_ptr<State>& state,
    std::uint64_t id)
    : state_(state),
      id_(id) {}

ServerFrameBatchListenerSubscription::ServerFrameBatchListenerSubscription(
    ServerFrameBatchListenerSubscription&& other) noexcept
    : state_(std::move(other.state_)),
      id_(other.id_) {
    other.id_ = 0;
}

ServerFrameBatchListenerSubscription& ServerFrameBatchListenerSubscription::operator=(
    ServerFrameBatchListenerSubscription&& other) noexcept {
    if (this != &other) {
        reset();
        state_ = std::move(other.state_);
        id_ = other.id_;
        other.id_ = 0;
    }
    return *this;
}

ServerFrameBatchListenerSubscription::~ServerFrameBatchListenerSubscription() {
    reset();
}

void ServerFrameBatchListenerSubscription::reset() {
    auto state = state_.lock();
    if (state == nullptr || id_ == 0U) {
        return;
    }
    for (Entry& entry : state->listeners) {
        if (entry.id == id_) {
            entry.listener = nullptr;
            break;
        }
    }
    id_ = 0;
    state_.reset();
}

bool ServerFrameBatchListenerSubscription::active() const noexcept {
    return id_ != 0U && !state_.expired();
}

ServerRegistryDirtyFrameSubscription ReplicationServer::subscribe_registry_dirty_frame_listener(
    ServerRegistryDirtyFrameListener& listener) {
    const std::uint64_t id = registry_dirty_frame_listeners_->next_id++;
    registry_dirty_frame_listeners_->listeners.push_back(ServerRegistryDirtyFrameSubscription::Entry{id, &listener});
    return ServerRegistryDirtyFrameSubscription(registry_dirty_frame_listeners_, id);
}

ServerFrameBatchListenerSubscription ReplicationServer::subscribe_frame_batch_listener(
    ServerFrameBatchListener& listener) {
    if (frame_batch_listeners_->broadcasting) {
        ASHIATO_SYNC_ASSERT_FAIL("cannot subscribe a frame batch listener while broadcasting");
        return {};
    }
    const std::uint64_t id = frame_batch_listeners_->next_id++;
    frame_batch_listeners_->listeners.push_back(ServerFrameBatchListenerSubscription::Entry{id, &listener});
    return ServerFrameBatchListenerSubscription(frame_batch_listeners_, id);
}

#ifdef ASHIATO_SYNC_ENABLE_TRACING
void ReplicationServer::set_tracer(SyncTracer* tracer) noexcept {
    trace_writer_.reset();
    tracer_ = tracer;
}

void ReplicationServer::set_trace_options(TraceOptions options) {
    options_.trace = std::move(options);
    trace_writer_ = make_trace_writer(options_.trace);
    tracer_ = trace_writer_ != nullptr ? &trace_writer_->tracer() : nullptr;
}

void ReplicationServer::trace_current_frame_components(ashiato::Registry& registry) {
    trace_frame_components(registry, registry.get<SyncSettings>());
}

void ReplicationServer::flush_trace() {
    if (trace_writer_ != nullptr) {
        trace_writer_->flush();
    }
}

void ReplicationServer::close_trace() {
    if (trace_writer_ != nullptr) {
        trace_writer_->close();
    }
}
#endif

bool ReplicationServer::add_client(ClientId client) {
    return add_client_for_peer(client, client, true);
}

ClientId ReplicationServer::find_next_available_client_id() const {
    ClientId candidate = next_connect_client_id_;
    for (std::uint16_t attempts = 0; attempts < max_client_entity_network_id_client; ++attempts) {
        if (candidate != invalid_client_id && client_to_index_.find(candidate) == client_to_index_.end()) {
            return candidate;
        }
        candidate =
            candidate >= max_client_entity_network_id_client
                ? ClientId{1}
                : static_cast<ClientId>(candidate + 1U);
    }
    return invalid_client_id;
}

bool ReplicationServer::add_client_state(ClientState state) {
    if (state.id == invalid_client_id ||
        state.id > max_client_entity_network_id_client ||
        client_to_index_.find(state.id) != client_to_index_.end()) {
        return false;
    }
    if (!state.local && (state.peer == invalid_peer_id || peer_to_index_.find(state.peer) != peer_to_index_.end())) {
        return false;
    }

    if (!state.local && state.ready_for_updates) {
        create_client_replicator(state);
    }

    const ClientId client = state.id;
    const PeerId peer = state.peer;
    const bool local = state.local;
    const bool ready_for_updates = state.ready_for_updates;
    const std::size_t index = clients_.size();
    clients_.push_back(std::move(state));
    client_to_index_[client] = index;
    if (!local) {
        peer_to_index_[peer] = index;
    }
    if (client >= next_connect_client_id_) {
        next_connect_client_id_ =
            client >= max_client_entity_network_id_client ? ClientId{1} : static_cast<ClientId>(client + 1U);
    }
    if (!local) {
        ++observability_stats_.client_connects_accepted;
        log_info("client_connected", "peer=" + std::to_string(peer) +
            " client=" + std::to_string(client) +
            " ready=" + (ready_for_updates ? std::string("true") : std::string("false")));
    }
    notify_connection_event(ReplicationServerConnectionEvent{
        ReplicationServerConnectionEventType::Accepted,
        peer,
        client,
        local,
        {}});
    if (ready_for_updates) {
        notify_connection_event(ReplicationServerConnectionEvent{
            ReplicationServerConnectionEventType::Ready,
            peer,
            client,
            local,
            {}});
    }
#ifdef ASHIATO_SYNC_ENABLE_TRACING
    if (tracer_ != nullptr && tracer_->enabled()) {
        SyncTraceEvent event = make_server_trace_event(SyncTraceEventType::ClientConnected, client, frame_);
        tracer_->trace(event);
    }
#endif
    return true;
}

void ReplicationServer::notify_connection_event(const ReplicationServerConnectionEvent& event) {
    if (options_.connection_event_handler) {
        options_.connection_event_handler(event);
    }
}

void ReplicationServer::create_client_replicator(ClientState& client) {
    if (client.local || client.replication != nullptr) {
        return;
    }
    client.replication = std::make_unique<ServerClientReplicator>();
    client.replication->id = client.id;
    client.replication->peer = client.peer;
    client.replication->bandwidth = std::make_shared<ReplicationBandwidthBudget>(options_.bandwidth);
    client.replication->bandwidth_participant =
        client.replication->bandwidth->attach_participant(ReplicationBandwidthParticipantOptions{1U, 0});
    client.replication->owner_server = this;
    client.replication->initialize_marking_all_dirty(*this, frame_);
    client.replication->registry_dirty_frame_subscription = subscribe_registry_dirty_frame_listener(*client.replication);
    client.replication->frame_batch_subscription = subscribe_frame_batch_listener(*client.replication);
}

bool ReplicationServer::add_client_for_peer(PeerId peer, ClientId client, bool ready_for_updates) {
    if (client == invalid_client_id ||
        client > max_client_entity_network_id_client ||
        peer == invalid_peer_id ||
        client_to_index_.find(client) != client_to_index_.end() ||
        peer_to_index_.find(peer) != peer_to_index_.end()) {
        return false;
    }

    ClientState state;
    state.id = client;
    state.peer = peer;
    state.ready_for_updates = ready_for_updates;
    state.connect_resend_accumulator_seconds = options_.connect_resend_interval_seconds;
    return add_client_state(std::move(state));
}

void ReplicationServer::set_local_client_id(ashiato::Registry& registry, ClientId client) {
    local_client_ = client;
    registry.write<SyncSettings>().local_client = client;
}

ClientId ReplicationServer::add_local_client(ashiato::Registry& registry) {
    if (local_client_ != invalid_client_id) {
        return invalid_client_id;
    }
    const ClientId local_client = find_next_available_client_id();
    if (local_client == invalid_client_id) {
        return invalid_client_id;
    }

    ClientState state;
    state.id = local_client;
    state.peer = invalid_peer_id;
    state.local = true;
    state.ready_for_updates = true;
    state.connect_resend_accumulator_seconds = options_.connect_resend_interval_seconds;
    if (!add_client_state(std::move(state))) {
        return invalid_client_id;
    }
    set_local_client_id(registry, local_client);
    return local_client_;
}

bool ReplicationServer::is_local_client(ClientId client) const noexcept {
    const auto found = client_to_index_.find(client);
    return found != client_to_index_.end() && clients_[found->second].local;
}

#ifdef ASHIATO_SYNC_ENABLE_TRACING
void ReplicationServer::trace_frame_components(const ashiato::Registry& registry, const SyncSettings& settings) {
    if (tracer_ == nullptr || !tracer_->enabled() || !tracer_->frame_data_enabled()) {
        return;
    }
    for (const ReplicatedSlot& slot : replicated_) {
        if (!slot.active || slot.archetype.value >= settings.archetypes.size()) {
            continue;
        }
        const SyncArchetype& archetype = settings.archetypes[slot.archetype.value];
        for (std::size_t component_index = 0; component_index < archetype.components.size(); ++component_index) {
            const ComponentReplication& replication = archetype.components[component_index];
            const void* value = registry.get(slot.entity, replication.component);
            if (value == nullptr || component_index >= archetype.component_ops.size()) {
                continue;
            }
            const SyncComponentOps& ops = archetype.component_ops[component_index];
            if (ops.serialization.quantize == nullptr) {
                continue;
            }
            std::vector<std::uint8_t> bytes(ops.serialization.quantized_size);
            ops.serialization.quantize(value, bytes.data());
            SyncTraceEvent event = make_server_trace_event(SyncTraceEventType::FrameComponent, invalid_client_id, frame_);
            event.server_entity = slot.entity;
            event.archetype = slot.archetype;
            event.component = replication.component;
            append_trace_component_data(tracer_, archetype, component_index, bytes.data(), event);
            tracer_->trace(event);
        }
    }
}

void ReplicationServer::trace_input_component(
    ashiato::Entity entity,
    ClientState& client,
    SyncFrame frame,
    ashiato::Entity component,
    const SyncComponentOps& ops,
    const std::uint8_t* quantized) {
    if (tracer_ == nullptr || !tracer_->enabled() || quantized == nullptr || !component) {
        return;
    }
    SyncTraceEvent event = make_server_trace_event(SyncTraceEventType::FrameComponent, client.id, frame);
    event.server_entity = entity;
    event.component = component;
    const auto found_replicated = entity_to_replicated_index_.find(entity.value);
    if (client.replication != nullptr &&
        found_replicated != entity_to_replicated_index_.end()) {
        ServerClientReplicator& replication = *client.replication;
        (void)replication.network_id_for(*this, found_replicated->second);
        const ClientEntityState* state = replication.entities.try_get(found_replicated->second);
        if (state != nullptr && state->has_network_id) {
            event.wire_network_id = state->network_id;
            event.network_version = state->network_version;
            event.client_network_id = make_client_entity_network_id(client.id, state->network_id, state->network_version);
            if (found_replicated->second < replicated_.size()) {
                event.archetype = replicated_[found_replicated->second].archetype;
            }
        }
    }
    append_trace_input_component_data(tracer_, ops, quantized, event);
    tracer_->trace(event);
}

void ReplicationServer::trace_input_starved(
    ashiato::Entity entity,
    ClientState& client,
    SyncFrame frame_for_input,
    SyncFrame frame_from_input,
    ashiato::Entity component,
    const SyncComponentOps& ops) {
    if (tracer_ == nullptr || !tracer_->enabled() || !component) {
        return;
    }
    SyncTraceEvent event = make_server_trace_event(SyncTraceEventType::InputStarved, client.id, frame_for_input);
    event.server_entity = entity;
    event.component = component;
    event.component_name = ops.serialization.name;
    const auto found_replicated = entity_to_replicated_index_.find(entity.value);
    if (client.replication != nullptr &&
        found_replicated != entity_to_replicated_index_.end()) {
        ServerClientReplicator& replication = *client.replication;
        (void)replication.network_id_for(*this, found_replicated->second);
        const ClientEntityState* state = replication.entities.try_get(found_replicated->second);
        if (state != nullptr && state->has_network_id) {
            event.wire_network_id = state->network_id;
            event.network_version = state->network_version;
            event.client_network_id = make_client_entity_network_id(client.id, state->network_id, state->network_version);
            if (found_replicated->second < replicated_.size()) {
                event.archetype = replicated_[found_replicated->second].archetype;
            }
        }
    }
    event.data = "input_frame=" + std::to_string(frame_from_input);
    tracer_->trace(event);
}

#ifdef ASHIATO_SYNC_TRACE_PACKET_LOGS
void ReplicationServer::trace_incoming_ack_packet(ServerClientReplicator& client, const std::vector<std::uint32_t>& acks) const {
    if (tracer_ == nullptr || !tracer_->enabled() || !tracer_->packet_logs_enabled()) {
        return;
    }
    SyncTraceEvent event = make_server_trace_event(SyncTraceEventType::PacketLog, client.id, frame_);
    event.data = "direction=in,message=client_ack,client=" + std::to_string(client.id) +
        ",acks=" + packet_ack_list(acks);
    tracer_->trace(event);
}

void ReplicationServer::trace_incoming_ping_packet(ClientState& client, std::uint32_t sequence) const {
    if (tracer_ == nullptr || !tracer_->enabled() || !tracer_->packet_logs_enabled()) {
        return;
    }
    SyncTraceEvent event = make_server_trace_event(SyncTraceEventType::PacketLog, client.id, frame_);
    event.data = "direction=in,message=client_ping,client=" + std::to_string(client.id) +
        ",sequence=" + std::to_string(sequence);
    tracer_->trace(event);
}

void ReplicationServer::trace_outgoing_pong_packet(
    ClientState& client,
    std::uint32_t sequence,
    SyncFrame server_receive_frame,
    SyncFrame server_send_frame) const {
    if (tracer_ == nullptr || !tracer_->enabled() || !tracer_->packet_logs_enabled()) {
        return;
    }
    SyncTraceEvent event = make_server_trace_event(SyncTraceEventType::PacketLog, client.id, server_send_frame);
    std::ostringstream out;
    out << "direction=out,message=server_pong,client=" << static_cast<unsigned>(client.id)
        << ",peer=" << client.peer
        << ",sequence=" << sequence
        << ",server_receive_frame=" << server_receive_frame
        << ",server_frame=" << server_send_frame;
    event.data = out.str();
    tracer_->trace(event);
}

void ReplicationServer::trace_incoming_input_packet(
    ClientState& client,
    const std::vector<std::uint32_t>& acks,
    SyncFrame baseline_frame,
    SyncFrame first_input_frame,
    SyncFrame last_input_frame) const {
    if (tracer_ == nullptr || !tracer_->enabled() || !tracer_->packet_logs_enabled()) {
        return;
    }
    SyncTraceEvent event = make_server_trace_event(SyncTraceEventType::PacketLog, client.id, frame_);
    std::ostringstream out;
    out << "direction=in,message=client_input,client=" << static_cast<unsigned>(client.id)
        << ",acks=" << packet_ack_list(acks)
        << ",input_frames=";
    if (first_input_frame != 0U && last_input_frame >= first_input_frame) {
        out << first_input_frame << "-" << last_input_frame;
    } else {
        out << "none";
    }
    out << ",baseline=" << baseline_frame;
    event.data = out.str();
    tracer_->trace(event);
}

void ReplicationServer::trace_outgoing_update_packet(
    ServerClientReplicator& client,
    SyncFrame frame,
    std::uint32_t packet_id,
    SyncFrame input_ack_frame,
    const std::vector<PacketAckRecord>& records) const {
    if (tracer_ == nullptr || !tracer_->enabled() || !tracer_->packet_logs_enabled()) {
        return;
    }
    SyncTraceEvent event = make_server_trace_event(SyncTraceEventType::PacketLog, client.id, frame);
    std::ostringstream out;
    out << "direction=out,message=server_update,client=" << static_cast<unsigned>(client.id)
        << ",sequence=" << packet_id
        << ",server_frame=" << frame
        << ",input_ack=" << input_ack_frame
        << ",record_count=" << records.size()
        << ",replicated_count=" << active_replicated_count_
        << ",client_count=" << client_count()
        << ",updated_server_entities=[";
    bool first = true;
    for (const PacketAckRecord& record : records) {
        if (record.destroy) {
            continue;
        }
        if (!first) {
            out << ",";
        }
        first = false;
        out << record.entity.value;
    }
    out << "]";
    out << ",destroyed_server_entities=[";
    bool first_destroy = true;
    for (const PacketAckRecord& record : records) {
        if (!record.destroy) {
            continue;
        }
        if (!first_destroy) {
            out << ",";
        }
        first_destroy = false;
        out << record.entity.value;
    }
    out << "]";
    out << ",cues=[";
    bool first_cue = true;
    for (const PacketAckRecord& record : records) {
        if (record.destroy) {
            continue;
        }
        for (const PacketAckRecord::CueSummary& cue : record.cues) {
            if (!first_cue) {
                out << ";";
            }
            first_cue = false;
            out << "{entity=" << record.entity.value
                << ",frame=" << cue.frame
                << ",type=" << cue.type;
#ifdef ASHIATO_SYNC_TRACE_COMPONENT_DATA
            if (tracer_ != nullptr && tracer_->frame_data_enabled() && !cue.data.empty()) {
                out << ",data=" << cue.data;
            }
#endif
            out << "}";
        }
    }
    out << "]";
    event.data = out.str();
    tracer_->trace(event);
}

void ReplicationServer::append_packet_ack_cues(
    const SyncSettings& settings,
    const ClientEntityState& state,
    PacketAckRecord& record) const {
    record.cues.reserve(state.pending_cues.size());
    for (const ClientEntityState::PendingCue& cue : state.pending_cues) {
        PacketAckRecord::CueSummary summary;
        summary.frame = cue.frame;
        summary.type = cue.type;
#ifdef ASHIATO_SYNC_TRACE_COMPONENT_DATA
        if (tracer_ != nullptr && tracer_->frame_data_enabled() &&
            cue.type < settings.cue_ops.size() && settings.cue_ops[cue.type].trace != nullptr) {
            SyncTraceStringBuilder builder;
            if (settings.cue_ops[cue.type].trace(cue.type, settings.cue_ops[cue.type].user_data, cue.payload, builder)) {
                summary.data = std::move(builder.value);
            }
        }
#else
        (void)settings;
#endif
        record.cues.push_back(std::move(summary));
    }
}
#endif
#endif

bool ReplicationServer::remove_client(ashiato::Registry& registry, ClientId client) {
    const auto found = client_to_index_.find(client);
    if (found == client_to_index_.end()) {
        return false;
    }

    const std::size_t index = found->second;
    const PeerId removed_peer = clients_[index].peer;
    const ClientId removed_id = clients_[index].id;
    const bool removed_local = clients_[index].local;
#ifdef ASHIATO_SYNC_ENABLE_TRACING
    const ClientId removed_client = clients_[index].id;
#endif
    if (clients_[index].replication != nullptr) {
        clients_[index].replication->entities.clear_all(*this);
        detach_client_bandwidth_participant(*clients_[index].replication);
    }

    const std::size_t last = clients_.size() - 1;
    if (index != last) {
        clients_[index] = std::move(clients_[last]);
        client_to_index_[clients_[index].id] = index;
        if (!clients_[index].local) {
            peer_to_index_[clients_[index].peer] = index;
        }
    }

    clients_.pop_back();
    client_to_index_.erase(found);
    if (!removed_local) {
        peer_to_index_.erase(removed_peer);
        ++observability_stats_.clients_removed;
        log_info("client_removed", "peer=" + std::to_string(removed_peer) +
            " client=" + std::to_string(removed_id));
    } else {
        set_local_client_id(registry, invalid_client_id);
    }
    notify_connection_event(ReplicationServerConnectionEvent{
        ReplicationServerConnectionEventType::Removed,
        removed_peer,
        removed_id,
        removed_local,
        {}});
#ifdef ASHIATO_SYNC_ENABLE_TRACING
    if (tracer_ != nullptr && tracer_->enabled()) {
        SyncTraceEvent event = make_server_trace_event(SyncTraceEventType::ClientDisconnected, removed_client, frame_);
        tracer_->trace(event);
    }
#endif
    return true;
}

bool ReplicationServer::has_client(ClientId client) const {
    return client_to_index_.find(client) != client_to_index_.end();
}

std::size_t ReplicationServer::client_count() const noexcept {
    return clients_.size();
}

std::vector<ClientId> ReplicationServer::client_ids() const {
    std::vector<ClientId> ids;
    ids.reserve(clients_.size());
    for (const ClientState& client : clients_) {
        ids.push_back(client.id);
    }
    return ids;
}

ClientEntityNetworkId ReplicationServer::client_entity_network_id(ClientId client, ashiato::Entity entity) const noexcept {
    const auto found_client = client_to_index_.find(client);
    const auto found_replicated = entity_to_replicated_index_.find(entity.value);
    if (found_client == client_to_index_.end() ||
        found_client->second >= clients_.size() ||
        found_replicated == entity_to_replicated_index_.end()) {
        return invalid_client_entity_network_id;
    }

    const ClientState& client_state = clients_[found_client->second];
    if (client_state.replication == nullptr) {
        return invalid_client_entity_network_id;
    }

    const ServerClientReplicator& replication = *client_state.replication;
    const ClientEntityState* state = replication.entities.try_get(found_replicated->second);
    if (state == nullptr || !state->has_network_id) {
        return invalid_client_entity_network_id;
    }
    return make_client_entity_network_id(client_state.id, state->network_id, state->network_version);
}

ReplicationServer::ClientInputStats ReplicationServer::input_stats(ClientId client) const noexcept {
    const auto found = client_to_index_.find(client);
    if (found == client_to_index_.end()) {
        return ClientInputStats{};
    }
    return clients_[found->second].input.stats();
}

ReplicationServer::ClientBandwidthStats ReplicationServer::bandwidth_stats(ClientId client) const noexcept {
    const auto found = client_to_index_.find(client);
    if (found == client_to_index_.end()) {
        return ClientBandwidthStats{};
    }
    const ClientState& state = clients_[found->second];
    if (!options_.bandwidth.enabled) {
        return ClientBandwidthStats{
            false,
            static_cast<double>(options_.bandwidth_limit_bytes_per_tick) / options_.fixed_dt_seconds,
            static_cast<double>(options_.bandwidth_limit_bytes_per_tick),
            0,
            0,
            0.0f};
    }
    if (state.replication == nullptr) {
        return ClientBandwidthStats{};
    }
    const ServerClientReplicator& replication = *state.replication;
    if (replication.bandwidth == nullptr) {
        return ClientBandwidthStats{};
    }
    return ClientBandwidthStats{
        true,
        replication.bandwidth->target_bytes_per_second(),
        replication.bandwidth->available_bytes(),
        replication.bandwidth->in_flight_bytes(),
        replication.bandwidth->delivered_bytes(),
        replication.bandwidth->loss_rate()};
}

std::shared_ptr<ReplicationBandwidthBudget> ReplicationServer::client_bandwidth_budget(ClientId client) const noexcept {
    const auto found = client_to_index_.find(client);
    if (found == client_to_index_.end()) {
        return {};
    }
    const ClientState& state = clients_[found->second];
    return state.replication == nullptr ? std::shared_ptr<ReplicationBandwidthBudget>{} : state.replication->bandwidth;
}

bool ReplicationServer::set_client_bandwidth_budget(
    ClientId client,
    std::shared_ptr<ReplicationBandwidthBudget> budget) {
    return set_client_bandwidth_budget(client, std::move(budget), client_bandwidth_share(client));
}

bool ReplicationServer::set_client_bandwidth_budget(
    ClientId client,
    std::shared_ptr<ReplicationBandwidthBudget> budget,
    ReplicationBandwidthParticipantOptions share) {
    const auto found = client_to_index_.find(client);
    if (found == client_to_index_.end() || found->second >= clients_.size()) {
        return false;
    }
    ClientState& state = clients_[found->second];
    if (state.replication == nullptr) {
        return false;
    }
    detach_client_bandwidth_participant(*state.replication);
    state.replication->bandwidth = budget != nullptr
        ? std::move(budget)
        : std::make_shared<ReplicationBandwidthBudget>(options_.bandwidth);
    state.replication->bandwidth_participant =
        state.replication->bandwidth->attach_participant(share);
    return true;
}

bool ReplicationServer::set_client_bandwidth_share(
    ClientId client,
    ReplicationBandwidthParticipantOptions share) {
    const auto found = client_to_index_.find(client);
    if (found == client_to_index_.end() || found->second >= clients_.size()) {
        return false;
    }
    ClientState& state = clients_[found->second];
    if (state.replication == nullptr || state.replication->bandwidth == nullptr ||
        state.replication->bandwidth_participant == invalid_bandwidth_participant_id) {
        return false;
    }
    return state.replication->bandwidth->set_participant_options(
        state.replication->bandwidth_participant,
        share);
}

ReplicationBandwidthParticipantOptions ReplicationServer::client_bandwidth_share(ClientId client) const noexcept {
    const auto found = client_to_index_.find(client);
    if (found == client_to_index_.end() || found->second >= clients_.size()) {
        return {};
    }
    const ClientState& state = clients_[found->second];
    if (state.replication == nullptr || state.replication->bandwidth == nullptr) {
        return {};
    }
    return state.replication->bandwidth->participant_options(state.replication->bandwidth_participant);
}

std::size_t ReplicationServer::begin_client_bandwidth_tick(ClientId client) {
    const auto found = client_to_index_.find(client);
    if (found == client_to_index_.end() || found->second >= clients_.size()) {
        return 0U;
    }
    ClientState& state = clients_[found->second];
    if (state.replication == nullptr) {
        return 0U;
    }
    return begin_client_bandwidth_tick(*state.replication);
}

ReplicationServer::ReplicationSendResult ReplicationServer::flush_client_updates(
    ashiato::Registry& registry,
    ClientId client) {
    const auto found = client_to_index_.find(client);
    if (found == client_to_index_.end() || found->second >= clients_.size()) {
        return {};
    }
    ClientState& state = clients_[found->second];
    if (state.replication == nullptr) {
        return {};
    }
    return flush_client_updates(registry, *state.replication);
}

void ReplicationServer::rediscover_all_replicated_entities(ashiato::Registry& registry) {
    if (!replicated_initialized_) {
        registry.view<const Replicated>().each([&](ashiato::Entity entity, const Replicated& replicated) {
            upsert_replicated(registry, entity, replicated.archetype);
        });
        replicated_initialized_ = true;
        return;
    }

    for (std::uint32_t slot = 0; slot < replicated_.size(); ++slot) {
        if (replicated_[slot].active && !replicated_is_replicable(registry, slot)) {
            deactivate_replicated(slot);
        }
    }
    registry.view<const Replicated>().each([&](ashiato::Entity entity, const Replicated& replicated) {
        upsert_replicated(registry, entity, replicated.archetype);
    });
}

void ReplicationServer::rediscover_replicated_entities(ashiato::Registry& registry, ashiato::Registry::DirtyView dirty) {
    if (!replicated_initialized_) {
        registry.view<const Replicated>().each([&](ashiato::Entity entity, const Replicated& replicated) {
            upsert_replicated(registry, entity, replicated.archetype);
        });
        replicated_initialized_ = true;
        return;
    }

    dirty.each_removed<Replicated>([&](ashiato::Registry::ComponentRemoval removal) {
        deactivate_entity_index(ashiato::Registry::entity_index(removal.entity));
    });

    dirty.each_added<Replicated>([&](ashiato::Entity entity, const void* value) {
        if (value == nullptr) {
            return;
        }
        upsert_replicated(registry, entity, static_cast<const Replicated*>(value)->archetype);
    });

    dirty.each_dirty<Replicated>([&](ashiato::Entity entity, const void* value) {
        if (value == nullptr) {
            return;
        }
        upsert_replicated(registry, entity, static_cast<const Replicated*>(value)->archetype);
    });
}

bool ReplicationServer::is_replicated(ashiato::Entity entity) const {
    return entity_to_replicated_index_.find(entity.value) != entity_to_replicated_index_.end();
}

std::size_t ReplicationServer::replicated_count() const noexcept
{
    return active_replicated_count_;
}

std::size_t ReplicationServer::replicated_slot_count() const noexcept {
    return replicated_.size();
}

std::size_t ReplicationServer::active_replicated_slot_count() const noexcept {
    return active_replicated_count_;
}

bool ReplicationServer::replicated_slot_active(std::uint32_t slot) const noexcept {
    return slot < replicated_.size() && replicated_[slot].active;
}

ashiato::Entity ReplicationServer::replicated_slot_entity(std::uint32_t slot) const noexcept {
    return slot < replicated_.size() ? replicated_[slot].entity : ashiato::Entity{};
}

SyncArchetypeId ReplicationServer::replicated_slot_archetype(std::uint32_t slot) const noexcept {
    return slot < replicated_.size() ? replicated_[slot].archetype : SyncArchetypeId{};
}

bool ReplicationServer::replicated_slot_is_replicable(const ashiato::Registry& registry, std::uint32_t slot) const {
    return replicated_is_replicable(registry, slot);
}

void ReplicationServer::deactivate_replicated_slot(std::uint32_t slot) {
    deactivate_replicated(slot);
}

std::uint32_t ReplicationServer::replicated_slot_for_entity(ashiato::Entity entity) const noexcept {
    const auto found = entity_to_replicated_index_.find(entity.value);
    return found != entity_to_replicated_index_.end() ? found->second : invalid_quantized_frame_id;
}

std::uint32_t ReplicationServer::replicated_slot_for_entity_index(std::uint32_t entity_index) const noexcept {
    const auto found = entity_index_to_replicated_index_.find(entity_index);
    return found != entity_index_to_replicated_index_.end() ? found->second : invalid_quantized_frame_id;
}

std::uint32_t ReplicationServer::quantized_frame_for_client(
    const ashiato::Registry& registry,
    const SyncSettings& settings,
    const server_detail::ServerClientReplicator& client,
    std::uint32_t slot,
    SyncFrame frame,
    QuantizedFrameData& scratch,
    std::vector<std::uint64_t>& scratch_dirty_generations) {
    return find_or_create_quantized_frame(
        registry,
        settings,
        client,
        slot,
        frame,
        scratch,
        scratch_dirty_generations);
}

bool ReplicationServer::quantized_frame_active(std::uint32_t quantized_frame) const noexcept {
    return quantized_frame < quantized_frames_.size() && quantized_frames_[quantized_frame].active;
}

SyncFrame ReplicationServer::quantized_frame_frame(std::uint32_t quantized_frame) const noexcept {
    return quantized_frame < quantized_frames_.size() ? quantized_frames_[quantized_frame].frame : 0;
}

SyncArchetypeId ReplicationServer::quantized_frame_archetype(std::uint32_t quantized_frame) const noexcept {
    return quantized_frame < quantized_frames_.size() ? quantized_frames_[quantized_frame].archetype : SyncArchetypeId{};
}

const QuantizedFrameData* ReplicationServer::quantized_frame_data(std::uint32_t quantized_frame) const noexcept {
    return quantized_frame_active(quantized_frame) ? &quantized_frames_[quantized_frame].data : nullptr;
}

void ReplicationServer::retain_server_quantized_frame(std::uint32_t quantized_frame) {
    retain_quantized_frame(quantized_frame);
}

void ReplicationServer::release_server_quantized_frame(std::uint32_t quantized_frame) {
    release_quantized_frame(quantized_frame);
}

void ReplicationServer::clear_server_client_entity_state(server_detail::ClientEntityState& state) {
    clear_client_entity_state(state);
}

void ReplicationServer::acknowledge_server_cues(server_detail::ClientEntityState& state, SyncFrame frame) {
    acknowledge_cues(state, frame);
}

void ReplicationServer::send_server_update_packet(
    server_detail::ServerClientReplicator& client,
    SyncFrame frame,
    std::uint16_t entity_count,
    const ashiato::BitBuffer& records,
    const std::vector<server_detail::PacketAckRecord>& ack_records) {
    send_packet(client, frame, entity_count, records, ack_records);
}

bool ReplicationServer::prepare_client_update_send(server_detail::ServerClientReplicator& replication) {
    const auto found = client_to_index_.find(replication.id);
    if (found == client_to_index_.end() ||
        found->second >= clients_.size() ||
        clients_[found->second].replication.get() != &replication) {
        return false;
    }
    if (!options_.transport) {
        throw std::logic_error("replication server requires ReplicationServerOptions::transport for serialized sends");
    }

    ClientState& client_state = clients_[found->second];
    if (!client_state.ready_for_updates || client_state.replication == nullptr) {
        return false;
    }

    replication.input_ack_frame = client_state.input.ack_frame();
    return true;
}

ReplicationServer::ReplicationSendResult ReplicationServer::flush_client_updates(
    ashiato::Registry& registry,
    server_detail::ServerClientReplicator& replication) {
    if (!prepare_client_update_send(replication)) {
        return {};
    }
    const SyncSettings& settings = registry.get<SyncSettings>();
    return replication.update_scheduler->send_client(*this, registry, settings, replication, 1U);
}

void ReplicationServer::detach_client_bandwidth_participant(ServerClientReplicator& replication) {
    if (replication.bandwidth != nullptr &&
        replication.bandwidth_participant != invalid_bandwidth_participant_id) {
        replication.bandwidth->detach_participant(replication.bandwidth_participant);
    }
    replication.bandwidth_participant = invalid_bandwidth_participant_id;
}

#ifdef ASHIATO_SYNC_ENABLE_TRACING
SyncTracer* ReplicationServer::server_tracer() const noexcept {
    return tracer_;
}

void ReplicationServer::trace_entity_started_syncing(
    ClientId client,
    std::uint32_t slot,
    std::uint32_t network_id,
    std::uint32_t network_version) {
    if (tracer_ == nullptr || !tracer_->enabled() || slot >= replicated_.size()) {
        return;
    }
    SyncTraceEvent event = make_server_trace_event(SyncTraceEventType::EntityStartedSyncing, client, frame_);
    event.server_entity = replicated_[slot].entity;
    event.wire_network_id = network_id;
    event.network_version = network_version;
    event.client_network_id = make_client_entity_network_id(client, network_id, network_version);
    event.archetype = replicated_[slot].archetype;
    tracer_->trace(event);
}

void ReplicationServer::trace_entity_destroyed(
    ClientId client,
    ashiato::Entity entity,
    std::uint32_t network_id,
    std::uint32_t network_version) {
    if (tracer_ == nullptr || !tracer_->enabled()) {
        return;
    }
    SyncTraceEvent event = make_server_trace_event(SyncTraceEventType::EntityDestroyed, client, frame_);
    event.server_entity = entity;
    event.wire_network_id = network_id;
    event.network_version = network_version;
    event.client_network_id = make_client_entity_network_id(client, network_id, network_version);
    event.remove = true;
    tracer_->trace(event);
}

#ifdef ASHIATO_SYNC_TRACE_PACKET_LOGS
void ReplicationServer::append_server_packet_ack_cues(
    const SyncSettings& settings,
    const server_detail::ClientEntityState& state,
    server_detail::PacketAckRecord& record) const {
    append_packet_ack_cues(settings, state, record);
}
#endif
#endif

bool ReplicationServer::acknowledge_entity(ClientId client, ashiato::Entity entity, SyncFrame frame) {
    const auto client_found = client_to_index_.find(client);
    const auto replicated_found = entity_to_replicated_index_.find(entity.value);
    if (client_found == client_to_index_.end() || replicated_found == entity_to_replicated_index_.end()) {
        return false;
    }

    ClientState& client_state = clients_[client_found->second];
    if (client_state.replication == nullptr) {
        return false;
    }
    return client_state.replication->acknowledge_entity(*this, replicated_found->second, frame);
}

void ReplicationServer::receive_packet(PeerId peer, ashiato::BitBuffer packet) {
    inbound_packets_.push_back(PendingInboundPacket{peer, std::move(packet)});
}

bool ReplicationServer::process_packet(PeerId client, ashiato::BitBuffer packet) {
    const bool previous_processing = processing_client_packet_;
    const bool previous_server_error_logged = server_error_logged_;
    processing_client_packet_ = true;
    server_error_logged_ = false;
    const auto finish = [this, previous_processing, previous_server_error_logged](bool result) {
        processing_client_packet_ = previous_processing;
        server_error_logged_ = previous_server_error_logged;
        return result;
    };
    try {
        detail::BitReader reader(packet);
        std::uint8_t message = 0;
        if (!reader.read_bits(protocol::message_bits, message)) {
            log_client_packet_warning(client, 0, "missing_message_id", "packet_missing_message_id");
            return finish(false);
        }

        if (message == protocol::client_connect_request_message) {
            return finish(process_connect_request_packet(client, packet));
        }

        const auto peer_found = peer_to_index_.find(client);
        if (peer_found == peer_to_index_.end()) {
            log_client_packet_warning(client, message, "unknown_peer", "packet_from_unknown_peer");
            return finish(false);
        }

        ClientState& state = clients_[peer_found->second];
        state.idle_seconds = 0.0;
        switch (message) {
        case protocol::client_connect_ack_message:
            return finish(process_connection_request_ack_packet(state, packet));
        case protocol::client_ping_message:
            return finish(process_ping_packet(state, packet));
        case protocol::client_ack_message:
            return finish(state.replication != nullptr && process_client_ack_packet(*state.replication, packet));
        default:
            log_client_packet_warning(client, message, "unknown_message", "unknown_client_message");
            return finish(false);
        }
    } catch (const std::exception& ex) {
        if (server_error_logged_) {
            server_error_logged_ = false;
        } else {
            log_client_packet_warning(client, 0, "decode_exception", ex.what());
        }
        return finish(false);
    }
}

bool ReplicationServer::process_packet(ashiato::Registry& registry, PeerId client, ashiato::BitBuffer packet) {
    const bool previous_processing = processing_client_packet_;
    const bool previous_server_error_logged = server_error_logged_;
    processing_client_packet_ = true;
    server_error_logged_ = false;
    const auto finish = [this, previous_processing, previous_server_error_logged](bool result) {
        processing_client_packet_ = previous_processing;
        server_error_logged_ = previous_server_error_logged;
        return result;
    };
    try {
        detail::BitReader reader(packet);
        std::uint8_t message = 0;
        if (!reader.read_bits(protocol::message_bits, message)) {
            log_client_packet_warning(client, 0, "missing_message_id", "packet_missing_message_id");
            return finish(false);
        }

        if (message == protocol::client_connect_request_message) {
            return finish(process_connect_request_packet(client, packet));
        }

        const auto peer_found = peer_to_index_.find(client);
        if (peer_found == peer_to_index_.end()) {
            log_client_packet_warning(client, message, "unknown_peer", "packet_from_unknown_peer");
            return finish(false);
        }

        ClientState& state = clients_[peer_found->second];
        state.idle_seconds = 0.0;
        return finish(process_message_from_connected_client(registry, state, message, packet));
    } catch (const std::exception& ex) {
        if (server_error_logged_) {
            server_error_logged_ = false;
        } else {
            log_client_packet_warning(client, 0, "decode_exception", ex.what());
        }
        return finish(false);
    }
}

bool ReplicationServer::process_connect_request_packet(PeerId peer, ashiato::BitBuffer& packet) {
    std::string token;
    if (!protocol::read_string(packet, token)) {
        log_client_packet_warning(peer, protocol::client_connect_request_message, "malformed_connect_request", "malformed_connect_request");
        return false;
    }
    const auto peer_found = peer_to_index_.find(peer);
    if (peer_found != peer_to_index_.end()) {
        clients_[peer_found->second].idle_seconds = 0.0;
        log_info("client_connect_duplicate", "peer=" + std::to_string(peer) +
            " client=" + std::to_string(clients_[peer_found->second].id));
        send_connect_response(clients_[peer_found->second]);
        return true;
    }

    ClientId accepted_client = find_next_available_client_id();
    std::string error;
    bool accepted = true;
    if (options_.connect_handler) {
        try {
            accepted = options_.connect_handler(token, accepted_client, error);
        } catch (const std::exception& ex) {
            log_server_error(peer, "connect_handler", ex.what());
            throw;
        }
    }
    if (!accepted || accepted_client == invalid_client_id ||
        accepted_client > max_client_entity_network_id_client) {
        if (accepted && error.empty() &&
            (accepted_client == invalid_client_id || accepted_client > max_client_entity_network_id_client)) {
            error = "client id out of range";
        }
        ashiato::BitBuffer response;
        response.reserve_bytes(options_.mtu_bytes);
        response.write_bits(protocol::server_connect_response_message, protocol::message_bits);
        response.write_bool(false);
        protocol::write_string(response, error);
        if (options_.transport) {
            try {
                options_.transport(peer, response);
            } catch (const std::exception& ex) {
                log_server_error(peer, "transport_error_connect_rejection", ex.what());
                throw;
            }
        }
        ++observability_stats_.client_connects_rejected;
        log_info("client_connect_rejected", "peer=" + std::to_string(peer) +
            " client=" + std::to_string(accepted_client));
        notify_connection_event(ReplicationServerConnectionEvent{
            ReplicationServerConnectionEventType::Rejected,
            peer,
            accepted_client,
            false,
            error});
        return accepted_client != invalid_client_id || !accepted;
    }

    if (!add_client_for_peer(peer, accepted_client, false)) {
        return false;
    }
    send_connect_response(clients_[client_to_index_[accepted_client]]);
    return true;
}

bool ReplicationServer::process_message_from_connected_client(
    ashiato::Registry& registry,
    ClientState& client,
    std::uint8_t message,
    ashiato::BitBuffer& packet) {
    switch (message) {
    case protocol::client_connect_ack_message:
        return process_connection_request_ack_packet(client, packet);
    case protocol::client_ping_message:
        if (!client.ready_for_updates) {
            log_client_packet_warning(client.peer, message, "ping_before_ready", "ping_before_connection_ready");
            return false;
        }
        return process_ping_packet(client, packet);
    case protocol::client_input_message:
        if (!client.ready_for_updates || client.replication == nullptr) {
            log_client_packet_warning(client.peer, message, "input_before_ready", "input_before_connection_ready");
            return false;
        }
        return process_input_with_acks_packet(registry, client, packet);
    case protocol::client_ack_message:
        return client.ready_for_updates && client.replication != nullptr && process_client_ack_packet(*client.replication, packet);
    default:
        log_client_packet_warning(client.peer, message, "unknown_message", "unknown_client_message");
        return false;
    }
}

bool ReplicationServer::process_connection_request_ack_packet(ClientState& client, ashiato::BitBuffer& packet) {
    detail::BitReader reader(packet);
    ClientId acked_client = invalid_client_id;
    if (!reader.read_bits(protocol::client_id_bits, acked_client)) {
        log_client_packet_warning(client.peer, protocol::client_connect_ack_message, "truncated_connect_ack", "truncated_connect_ack");
        return false;
    }
    if (acked_client != client.id) {
        log_client_packet_warning(client.peer, protocol::client_connect_ack_message, "connect_ack_client_mismatch", "connect_ack_client_id_mismatch");
        return false;
    }
    if (!client.ready_for_updates) {
        client.ready_for_updates = true;
        create_client_replicator(client);
        ++observability_stats_.clients_ready;
        log_info("client_ready", "peer=" + std::to_string(client.peer) +
            " client=" + std::to_string(client.id));
        notify_connection_event(ReplicationServerConnectionEvent{
            ReplicationServerConnectionEventType::Ready,
            client.peer,
            client.id,
            client.local,
            {}});
    }
    return true;
}

bool ReplicationServer::process_ping_packet(ClientState& client, ashiato::BitBuffer& packet) {
    detail::BitReader reader(packet);
    std::uint32_t sequence = 0;
    if (!reader.read_bits(32U, sequence)) {
        log_client_packet_warning(client.peer, protocol::client_ping_message, "truncated_ping", "truncated_ping");
        return false;
    }
#if defined(ASHIATO_SYNC_ENABLE_TRACING) && defined(ASHIATO_SYNC_TRACE_PACKET_LOGS)
    trace_incoming_ping_packet(client, sequence);
#endif
    const double subframe = tick_accumulator_seconds_ / options_.fixed_dt_seconds;
    const auto server_receive_subframe = static_cast<std::uint16_t>(std::clamp(
        static_cast<std::uint32_t>(std::floor(subframe * static_cast<double>(protocol::frame_subframe_scale))),
        std::uint32_t{0},
        protocol::frame_subframe_scale - 1U));
    send_pong(client, sequence, frame_, server_receive_subframe);
    return true;
}

bool ReplicationServer::process_client_ack_packet(ServerClientReplicator& replication, ashiato::BitBuffer& packet) {
#if defined(ASHIATO_SYNC_ENABLE_TRACING) && defined(ASHIATO_SYNC_TRACE_PACKET_LOGS)
    std::vector<std::uint32_t> acks;
#endif
    const ClientUpdateAckResult ack_result = process_client_acks_from_packet(
        replication,
        packet
#if defined(ASHIATO_SYNC_ENABLE_TRACING) && defined(ASHIATO_SYNC_TRACE_PACKET_LOGS)
        ,
        acks
#endif
    );
    if (!ack_result.packet_valid) {
        log_client_packet_warning(replication.peer, protocol::client_ack_message, "malformed_ack_packet", "malformed_ack_packet");
        return false;
    }
#if defined(ASHIATO_SYNC_ENABLE_TRACING) && defined(ASHIATO_SYNC_TRACE_PACKET_LOGS)
    trace_incoming_ack_packet(replication, acks);
#endif
    return ack_result.all_acknowledged;
}

bool ReplicationServer::set_local_input_bytes(ashiato::Registry& registry, ashiato::Entity component, const void* input) {
    if (local_client_ == invalid_client_id || input == nullptr) {
        return false;
    }
    const auto found_client = client_to_index_.find(local_client_);
    if (found_client == client_to_index_.end() || !clients_[found_client->second].local) {
        return false;
    }
    const SyncSettings& settings = registry.get<SyncSettings>();
    if (!settings.input_component || settings.input_component != component) {
        return false;
    }
    const auto found_ops = settings.component_ops.find(component.value);
    if (found_ops == settings.component_ops.end() ||
        found_ops->second.serialization.quantize == nullptr ||
        found_ops->second.serialization.push_to_registry == nullptr ||
        found_ops->second.serialization.quantized_size == 0U) {
        return false;
    }

    const SyncComponentOps& ops = found_ops->second;
    std::vector<std::uint8_t> quantized(ops.serialization.quantized_size);
    ops.serialization.quantize(input, quantized.data());
    return clients_[found_client->second].input.set_local_frame(
        frame_ + 1U,
        quantized.data(),
        quantized.size(),
        options_.input_buffer_capacity_frames);
}

ReplicationServer::ClientUpdateAckResult ReplicationServer::process_client_acks_from_packet(
    ServerClientReplicator& replication,
    ashiato::BitBuffer& packet
#if defined(ASHIATO_SYNC_ENABLE_TRACING) && defined(ASHIATO_SYNC_TRACE_PACKET_LOGS)
    ,
    std::vector<std::uint32_t>& trace_acks
#endif
) {
    const std::size_t packet_id_bits = configured_packet_id_bits(options_);
    detail::BitReader reader(packet);
    std::uint16_t ack_count = 0;
    if (!reader.read_bits(protocol::ack_count_bits, ack_count)) {
        return ClientUpdateAckResult{};
    }
#if defined(ASHIATO_SYNC_ENABLE_TRACING) && defined(ASHIATO_SYNC_TRACE_PACKET_LOGS)
    trace_acks.reserve(trace_acks.size() + ack_count);
#endif

    bool all_acknowledged = true;
    for (std::uint16_t ack = 0; ack < ack_count; ++ack) {
        std::uint32_t packet_id = 0;
        if (!reader.read_bits(packet_id_bits, packet_id)) {
            return ClientUpdateAckResult{};
        }
#if defined(ASHIATO_SYNC_ENABLE_TRACING) && defined(ASHIATO_SYNC_TRACE_PACKET_LOGS)
        trace_acks.push_back(packet_id);
#endif
        all_acknowledged =
            replication.ack_tracker.acknowledge_packet(*this, replication, packet_id) && all_acknowledged;
    }
    replication.ack_tracker.cleanup_packet_acks(*this, replication);
    return ClientUpdateAckResult{true, all_acknowledged};
}

bool ReplicationServer::process_input_with_acks_packet(
    ashiato::Registry& registry,
    ClientState& client,
    ashiato::BitBuffer& packet) {
    const SyncSettings& settings = registry.get<SyncSettings>();
    if (!settings.input_component) {
        log_server_error(client.peer, "client_input", "server registry has no input component");
        return false;
    }
    const auto found_ops = settings.component_ops.find(settings.input_component.value);
    if (found_ops == settings.component_ops.end() ||
        found_ops->second.serialization.deserialize == nullptr ||
        found_ops->second.serialization.push_to_registry == nullptr ||
        found_ops->second.serialization.quantized_size == 0U) {
        log_server_error(client.peer, "client_input", "server registry input component is not serializable");
        return false;
    }
    const SyncComponentOps& ops = found_ops->second;

    if (client.replication == nullptr) {
        log_client_packet_warning(client.peer, protocol::client_input_message, "input_without_replication_state", "input_from_client_without_replication_state");
        return false;
    }
    ServerClientReplicator& replication = *client.replication;
#if defined(ASHIATO_SYNC_ENABLE_TRACING) && defined(ASHIATO_SYNC_TRACE_PACKET_LOGS)
    std::vector<std::uint32_t> acks;
#endif
    const ClientUpdateAckResult ack_result = process_client_acks_from_packet(
        replication,
        packet
#if defined(ASHIATO_SYNC_ENABLE_TRACING) && defined(ASHIATO_SYNC_TRACE_PACKET_LOGS)
        ,
        acks
#endif
    );
    if (!ack_result.packet_valid) {
        log_client_packet_warning(client.peer, protocol::client_input_message, "malformed_input_ack_records", "malformed_input_ack_records");
        return false;
    }

    server_detail::ServerInputPacketTrace trace;
    if (!client.input.process_packet_payload(packet, ops, options_.input_buffer_capacity_frames, &trace)) {
        log_client_packet_warning(client.peer, protocol::client_input_message, "malformed_input_payload", "malformed_input_payload");
        return false;
    }
#if defined(ASHIATO_SYNC_ENABLE_TRACING) && defined(ASHIATO_SYNC_TRACE_PACKET_LOGS)
    trace_incoming_input_packet(client, acks, trace.baseline_frame, trace.first_input_frame, trace.last_input_frame);
#endif
    return true;
}

void ReplicationServer::log_info(const char* event, const std::string& fields) const {
    if (logger_ != nullptr && logger_->should_log(spdlog::level::info)) {
        logger_->info("event={} frame={} {}", event, frame_, fields);
    }
}

void ReplicationServer::log_client_packet_warning(
    PeerId peer,
    std::uint8_t message,
    const char* reason_code,
    const char* reason_detail) {
    ++observability_stats_.client_packet_warnings;
    const std::uint32_t max_logs = options_.logging.max_warning_logs_per_source;
    std::uint32_t& logged_count = warning_logs_by_peer_[peer];
    if (max_logs != 0U && logged_count >= max_logs) {
        ++observability_stats_.suppressed_client_packet_warnings;
        if (logged_count == max_logs) {
            ++logged_count;
            if (logger_ != nullptr && logger_->should_log(spdlog::level::warn)) {
                logger_->warn(
                    "event=client_packet_warnings_suppressed frame={} peer={} max_logs={}",
                    frame_,
                    peer,
                    max_logs);
            }
        }
        return;
    }
    ++logged_count;
    if (logger_ != nullptr && logger_->should_log(spdlog::level::warn)) {
        logger_->warn(
            "event=client_packet_rejected frame={} peer={} message_id={} reason={} detail={}",
            frame_,
            peer,
            message,
            reason_code,
            detail::log_token(reason_detail));
    }
}

void ReplicationServer::log_server_error(PeerId peer, const char* event, const char* reason) {
    if (processing_client_packet_) {
        server_error_logged_ = true;
    }
    ++observability_stats_.server_errors;
    if (logger_ != nullptr && logger_->should_log(spdlog::level::err)) {
        logger_->error("event={} frame={} peer={} reason={}", event, frame_, peer, detail::log_token(reason));
    }
}

void ReplicationServer::log_entity_update_exceeds_mtu(
    PeerId peer,
    ClientId client,
    ashiato::Entity entity,
    SyncArchetypeId archetype,
    std::size_t packet_bytes,
    std::size_t mtu_bytes,
    std::size_t record_bits) {
    ++observability_stats_.server_errors;
    if (logger_ != nullptr && logger_->should_log(spdlog::level::err)) {
        logger_->error(
            "event=server_entity_update_exceeds_mtu frame={} peer={} client={} entity={} archetype={} packet_bytes={} mtu_bytes={} record_bits={}",
            frame_,
            peer,
            client,
            entity.value,
            archetype.value,
            packet_bytes,
            mtu_bytes,
            record_bits);
    }
}

std::size_t ReplicationServer::retained_quantized_frame_count() const noexcept {
    std::size_t count = 0;
    for (const QuantizedFrame& quantized_frame : quantized_frames_) {
        if (quantized_frame.active && quantized_frame.ref_count != 0) {
            ++count;
        }
    }
    return count;
}

std::size_t ReplicationServer::retained_quantized_frame_bytes() const noexcept {
    std::size_t bytes = 0;
    for (const QuantizedFrame& quantized_frame : quantized_frames_) {
        if (!quantized_frame.active || quantized_frame.ref_count == 0) {
            continue;
        }
        bytes += quantized_frame.data.bytes.size();
    }
    return bytes;
}

void ReplicationServer::push_client_inputs_to_ashiato(ashiato::Registry& registry) {
    const SyncSettings& settings = registry.get<SyncSettings>();
    if (!settings.input_component) {
        return;
    }
    const auto found_ops = settings.component_ops.find(settings.input_component.value);
    if (found_ops == settings.component_ops.end() || found_ops->second.serialization.push_to_registry == nullptr) {
        return;
    }
    const SyncComponentOps& ops = found_ops->second;
    const SyncFrame input_frame_num = frame_;
    std::unordered_map<ClientId, server_detail::ServerInputForFrame> input_for_frame_cache;

    registry.view<const NetworkOwner>().each([this, &input_for_frame_cache, input_frame_num, &ops, &registry
#ifdef ASHIATO_SYNC_ENABLE_TRACING
          , &settings
#endif
    ]
          (ashiato::Entity entity, const NetworkOwner& owner) {
        const auto found_client = client_to_index_.find(owner.client);
        if (found_client == client_to_index_.end()) {
            return;
        }
        ClientState& client = clients_[found_client->second];

        server_detail::ServerInputForFrame& input_for_frame = input_for_frame_cache[client.id];
        if (!input_for_frame.cached) {
            input_for_frame = client.input.select_input_for_frame(input_frame_num, ops.serialization.quantized_size);
        }

        if (input_for_frame.bytes == nullptr || input_for_frame.bytes->size() != ops.serialization.quantized_size) {
#ifdef ASHIATO_SYNC_ENABLE_TRACING
            trace_input_starved(entity, client, input_frame_num, input_for_frame.input_frame, settings.input_component, ops);
#endif
            return;
        }
        (void)ops.serialization.push_to_registry(registry, entity, input_for_frame.bytes->data());
#ifdef ASHIATO_SYNC_ENABLE_TRACING
        trace_input_component(entity, client, input_frame_num, settings.input_component, ops, input_for_frame.bytes->data());
        if (input_for_frame.input_frame < input_frame_num) {
            trace_input_starved(entity, client, input_frame_num, input_for_frame.input_frame, settings.input_component, ops);
        }
#endif
    });
}

bool ReplicationServer::tick(ashiato::Registry& registry, double dt_seconds) {
    if (dt_seconds < 0.0 || !std::isfinite(dt_seconds)) {
        return false;
    }

    advance_client_idle_timers(dt_seconds);
    tick_accumulator_seconds_ += dt_seconds;

    for (PendingInboundPacket& inbound : inbound_packets_) {
        (void)process_packet(registry, inbound.client, std::move(inbound.packet));
    }
    inbound_packets_.clear();

    resend_pending_connect_responses(dt_seconds);
    disconnect_timed_out_clients(registry);

    const ServerFixedStepAdvance advance = consume_server_fixed_steps(
        tick_accumulator_seconds_,
        options_.fixed_dt_seconds,
        options_.max_fixed_steps_per_tick);
    if (advance.dropped_steps != 0U) {
        observability_stats_.dropped_fixed_step_frames += advance.dropped_steps;
        ++observability_stats_.fixed_step_overflow_events;
    }
    const std::uint32_t completed_frames = advance.steps;
    for (std::uint32_t step = 0; step < completed_frames; ++step) {
        ++frame_;
        registry.write<FrameInfo>().frame = frame_;
        push_client_inputs_to_ashiato(registry);
        registry.run_jobs();
        push_dirty_info_to_listeners(registry);
    }

    if (completed_frames > 0U) {
        push_frame_to_listeners(registry, dt_seconds, completed_frames);
    }

    return true;
}

bool ReplicationServer::advance_frame_without_simulating(ashiato::Registry& registry) {
    return advance_frame_without_simulating(registry, frame_ + 1U);
}

bool ReplicationServer::advance_frame_without_simulating(ashiato::Registry& registry, SyncFrame frame) {
    if (frame == 0U) {
        return false;
    }
    frame_ = frame;
    tick_accumulator_seconds_ = 0.0;
    registry.write<FrameInfo>().frame = frame_;
    push_dirty_info_to_listeners(registry);
    push_frame_to_listeners(registry, options_.fixed_dt_seconds, 1U);
    return true;
}

bool ReplicationServer::set_frame_without_broadcast(ashiato::Registry& registry, SyncFrame frame) {
    if (frame == 0U) {
        return false;
    }
    frame_ = frame;
    tick_accumulator_seconds_ = 0.0;
    registry.write<FrameInfo>().frame = frame_;
    return true;
}

void ReplicationServer::advance_client_idle_timers(double dt_seconds) {
    if (options_.idle_client_timeout_seconds == 0.0 || dt_seconds == 0.0) {
        return;
    }
    for (ClientState& client : clients_) {
        if (!client.local) {
            client.idle_seconds += dt_seconds;
        }
    }
}

void ReplicationServer::resend_pending_connect_responses(double dt_seconds) {
    if (dt_seconds == 0.0 || options_.connect_resend_interval_seconds == 0.0) {
        return;
    }
    for (ClientState& client : clients_) {
        if (client.local || client.ready_for_updates || client.replication != nullptr) {
            continue;
        }
        client.connect_resend_accumulator_seconds += dt_seconds;
        if (client.connect_resend_accumulator_seconds >= options_.connect_resend_interval_seconds) {
            send_connect_response(client);
        }
    }
}

void ReplicationServer::disconnect_timed_out_clients(ashiato::Registry& registry) {
    if (options_.idle_client_timeout_seconds == 0.0) {
        return;
    }
    for (std::size_t index = 0; index < clients_.size();) {
        const ClientState& client = clients_[index];
        if (client.local) {
            ++index;
            continue;
        }
        if (client.idle_seconds >= options_.idle_client_timeout_seconds) {
            const ClientId client_id = client.id;
            const PeerId peer = client.peer;
            ++observability_stats_.clients_timed_out;
            log_info("client_timed_out", "peer=" + std::to_string(client.peer) +
                " client=" + std::to_string(client.id));
            notify_connection_event(ReplicationServerConnectionEvent{
                ReplicationServerConnectionEventType::TimedOut,
                peer,
                client_id,
                false,
                "idle timeout"});
            remove_client(registry, client_id);
            continue;
        }
        ++index;
    }
}

void ReplicationServer::push_dirty_info_to_listeners(ashiato::Registry& registry) {
    post_tick_destroyed_slots_.clear();
    registry_dirty_frame_broadcaster_.broadcast(registry);
    post_tick_destroyed_slots_.clear();
}

void ReplicationServer::on_registry_dirty_frame(const ashiato::RegistryDirtyFrame& frame) {
    rediscover_replicated_entities(frame.registry, frame.dirty);
    const SyncSettings& settings = frame.registry.get<SyncSettings>();
    capture_dirty_generations(frame.dirty, settings);
#ifdef ASHIATO_SYNC_ENABLE_TRACING
    trace_frame_components(frame.registry, settings);
#endif
    // This atomic swap is the cue observation boundary. Re-entrant emissions remain queued.
    frame.registry.write<CueDispatcher>().drain_into(frame_cues_);
    const QueuedSyncCueView cue_view{
        frame_cues_.empty() ? nullptr : frame_cues_.data(),
        frame_cues_.size()};
    broadcast_registry_dirty_frame(frame, cue_view);
    capture_queued_cues(frame.registry, settings, cue_view);
}

void ReplicationServer::push_frame_to_listeners(
    ashiato::Registry& registry,
    double dt_seconds,
    std::uint32_t completed_frames) {
    broadcast_frame_batch(registry, dt_seconds, completed_frames);
}

void ReplicationServer::broadcast_registry_dirty_frame(
    const ashiato::RegistryDirtyFrame& frame,
    QueuedSyncCueView cues) {
    ServerRegistryDirtyFrame server_frame{
        *this,
        frame.registry,
        frame.dirty,
        frame_,
        cues,
        ServerDestroyedReplicatedSlotView{
            post_tick_destroyed_slots_.empty() ? nullptr : post_tick_destroyed_slots_.data(),
            post_tick_destroyed_slots_.size()}};
    const std::size_t listener_count = registry_dirty_frame_listeners_->listeners.size();
    for (std::size_t index = 0; index < listener_count; ++index) {
        ServerRegistryDirtyFrameSubscription::Entry& entry = registry_dirty_frame_listeners_->listeners[index];
        if (entry.listener != nullptr) {
            entry.listener->on_server_registry_dirty_frame(server_frame);
        }
    }
    remove_unsubscribed_registry_dirty_frame_listeners();
}

void ReplicationServer::remove_unsubscribed_registry_dirty_frame_listeners() {
    registry_dirty_frame_listeners_->listeners.erase(
        std::remove_if(
            registry_dirty_frame_listeners_->listeners.begin(),
            registry_dirty_frame_listeners_->listeners.end(),
            [](const ServerRegistryDirtyFrameSubscription::Entry& entry) {
                return entry.listener == nullptr;
            }),
        registry_dirty_frame_listeners_->listeners.end());
}

void ReplicationServer::broadcast_frame_batch(
    ashiato::Registry& registry,
    double dt_seconds,
    std::uint32_t completed_frames) {
    ScopedBroadcastFlag broadcasting(frame_batch_listeners_->broadcasting);
    ServerFrameBatch batch{*this, registry, dt_seconds, completed_frames};
    for (const ServerFrameBatchListenerSubscription::Entry& entry : frame_batch_listeners_->listeners) {
        if (entry.listener != nullptr) {
            entry.listener->on_server_frame_batch_complete(batch);
        }
    }
    remove_unsubscribed_frame_batch_listeners();
}

void ReplicationServer::remove_unsubscribed_frame_batch_listeners() {
    frame_batch_listeners_->listeners.erase(
        std::remove_if(
            frame_batch_listeners_->listeners.begin(),
            frame_batch_listeners_->listeners.end(),
            [](const ServerFrameBatchListenerSubscription::Entry& entry) {
                return entry.listener == nullptr;
            }),
        frame_batch_listeners_->listeners.end());
}

void ReplicationServer::capture_dirty_generations(ashiato::Registry::DirtyView dirty, const SyncSettings& settings) {
    for (const SyncArchetype& archetype : settings.archetypes) {
        for (const SyncTagReplication& tag_replication : archetype.tags) {
            dirty.each_dirty(tag_replication.tag, [&](ashiato::Entity entity, const void*) {
                const auto found = entity_to_replicated_index_.find(entity.value);
                if (found != entity_to_replicated_index_.end()) {
                    mark_dirty_tag(settings, found->second, tag_replication.tag);
                }
            });
            dirty.each_removed(tag_replication.tag, [&](ashiato::Registry::ComponentRemoval removal) {
                const auto found =
                    entity_index_to_replicated_index_.find(ashiato::Registry::entity_index(removal.entity));
                if (found != entity_index_to_replicated_index_.end()) {
                    mark_dirty_tag(settings, found->second, tag_replication.tag);
                }
            });
        }
    }

    for (const auto& component_ops : settings.component_ops) {
        const ashiato::Entity component{component_ops.first};
        dirty.each_dirty(component, [&](ashiato::Entity entity, const void*) {
            const auto found = entity_to_replicated_index_.find(entity.value);
            if (found != entity_to_replicated_index_.end()) {
                mark_dirty_component(settings, found->second, component);
            }
        });
        dirty.each_removed(component, [&](ashiato::Registry::ComponentRemoval removal) {
            const auto found =
                entity_index_to_replicated_index_.find(ashiato::Registry::entity_index(removal.entity));
            if (found != entity_index_to_replicated_index_.end()) {
                mark_dirty_component(settings, found->second, component);
            }
        });
    }

    dirty.each_dirty<NetworkOwner>([&](ashiato::Entity entity, const void*) {
        const auto found = entity_to_replicated_index_.find(entity.value);
        if (found != entity_to_replicated_index_.end()) {
            mark_owner_visibility_dirty(settings, found->second);
        }
    });
    dirty.each_removed<NetworkOwner>([&](ashiato::Registry::ComponentRemoval removal) {
        const auto found = entity_index_to_replicated_index_.find(ashiato::Registry::entity_index(removal.entity));
        if (found != entity_index_to_replicated_index_.end()) {
            mark_owner_visibility_dirty(settings, found->second);
        }
    });
}

bool ReplicationServer::play_local_cue(
    ashiato::Registry& registry,
    const SyncSettings& settings,
    const QueuedSyncCue& cue) {
    if (local_client_ == invalid_client_id ||
        cue.type >= settings.cue_ops.size() ||
        settings.cue_ops[cue.type].play == nullptr ||
        settings.cue_ops[cue.type].deserialize_into == nullptr ||
        !registry.alive(cue.entity)) {
        return false;
    }
    const NetworkOwner* owner = registry.try_get<NetworkOwner>(cue.entity);
    if (cue.only_replicate_to_owner && (owner == nullptr || owner->client != local_client_)) {
        return true;
    }

    struct ReferenceContextData {
        ReplicationServer* server = nullptr;
    } reference_context_data{this};
    EntityReferenceContext reference_context;
    reference_context.userContext = &reference_context_data;
    reference_context.network_entity_id_tier0_bits = options_.protocol.network_entity_id_tier0_bits;
    reference_context.server_network_id_for_entity = [](void* userContext, ashiato::Entity entity) {
        ReferenceContextData& data = *static_cast<ReferenceContextData*>(userContext);
        const auto found = data.server->entity_to_replicated_index_.find(entity.value);
        if (found == data.server->entity_to_replicated_index_.end()) {
            return 0U;
        }
        return found->second + 1U;
    };
    reference_context.client_entity_network_id_for_wire = [](void* userContext, std::uint32_t wire_network_id) {
        if (wire_network_id == 0U) {
            return invalid_client_entity_network_id;
        }
        ReferenceContextData& data = *static_cast<ReferenceContextData*>(userContext);
        const std::uint32_t slot = wire_network_id - 1U;
        if (slot >= data.server->replicated_.size() || !data.server->replicated_[slot].active) {
            return invalid_client_entity_network_id;
        }
        return make_client_entity_network_id(data.server->local_client_, wire_network_id, 1U);
    };
    reference_context.client_local_entity = [](void* userContext, ClientEntityNetworkId network_id) {
        ReferenceContextData& data = *static_cast<ReferenceContextData*>(userContext);
        const std::uint32_t slot = client_entity_network_id_wire_id(network_id) - 1U;
        if (slot >= data.server->replicated_.size() || !data.server->replicated_[slot].active) {
            return ashiato::Entity{};
        }
        return data.server->replicated_[slot].entity;
    };

    CueValue decoded_value = cue.value;
    if (settings.cue_ops[cue.type].references_entities && cue.value.has_value()) {
        if (settings.cue_ops[cue.type].serialize == nullptr) {
            return false;
        }
        ashiato::BitBuffer payload;
        ashiato::ComponentSerializationContext serialization_context{&reference_context};
        serialization_context.currentFrame = cue.frame;
        serialization_context.previousFrame = 0U;
        settings.cue_ops[cue.type].serialize(cue.value.data(), payload, serialization_context);
        if (!settings.cue_ops[cue.type].deserialize_into(
                cue.type,
                settings.cue_ops[cue.type].user_data,
                payload,
                decoded_value,
                serialization_context)) {
            return false;
        }
    } else if (!decoded_value.has_value()) {
        ashiato::BitBuffer payload = cue.payload;
        EntityReferenceContext* references = settings.cue_ops[cue.type].references_entities ? &reference_context : nullptr;
        ashiato::ComponentSerializationContext serialization_context{references};
        serialization_context.currentFrame = cue.frame;
        serialization_context.previousFrame = 0U;
        if (!settings.cue_ops[cue.type].deserialize_into(
                cue.type,
                settings.cue_ops[cue.type].user_data,
                payload,
                decoded_value,
                serialization_context)) {
            return false;
        }
    }
    return settings.cue_ops[cue.type].play(
        cue.type,
        settings.cue_ops[cue.type].user_data,
        registry,
        cue.entity,
        decoded_value.data(),
        0.0f,
        cue.frame);
}

void ReplicationServer::capture_queued_cues(
    ashiato::Registry& registry,
    const SyncSettings& settings,
    QueuedSyncCueView cues) {
    for (const QueuedSyncCue& cue : cues) {
        const auto found = entity_to_replicated_index_.find(cue.entity.value);
        if (found == entity_to_replicated_index_.end()) {
            continue;
        }
        (void)play_local_cue(registry, settings, cue);
#ifdef ASHIATO_SYNC_ENABLE_TRACING
        if (tracer_ != nullptr && tracer_->enabled()) {
            SyncTraceEvent event = make_server_trace_event(SyncTraceEventType::CueEmitted, invalid_client_id, cue.frame);
            event.server_entity = cue.entity;
            if (found->second < replicated_.size()) {
                event.archetype = replicated_[found->second].archetype;
            }
            event.cue_type = cue.type;
            append_trace_cue_name(settings, cue.type, event);
#ifdef ASHIATO_SYNC_TRACE_COMPONENT_DATA
            const ashiato::BitBuffer* trace_payload = &cue.payload;
            ashiato::BitBuffer serialized_trace_payload;
            if (tracer_->frame_data_enabled() &&
                cue.value.has_value() &&
                cue.type < settings.cue_ops.size() &&
                settings.cue_ops[cue.type].serialize != nullptr) {
                ashiato::ComponentSerializationContext serialization_context;
                settings.cue_ops[cue.type].serialize(cue.value.data(), serialized_trace_payload, serialization_context);
                trace_payload = &serialized_trace_payload;
            }
            append_trace_cue_data(tracer_, settings, cue.type, *trace_payload, event);
#else
            append_trace_cue_data(tracer_, settings, cue.type, cue.payload, event);
#endif
            append_trace_data_field(event, "source", "server");
            tracer_->trace(event);
        }
#endif
        attach_cue_to_clients(registry, settings, found->second, cue);
    }
}

void ReplicationServer::attach_cue_to_clients(
    const ashiato::Registry& registry,
    const SyncSettings& settings,
    std::uint32_t slot,
    const QueuedSyncCue& cue) {
    if (slot >= replicated_.size() || !replicated_[slot].active) {
        return;
    }
    if (cue.type >= settings.cue_ops.size() || settings.cue_ops[cue.type].serialize == nullptr) {
        return;
    }
    if (!detail::cue_relevance_fits_frame_range(cue.frame, cue.relevance_seconds, options_.fixed_dt_seconds)) {
        ASHIATO_SYNC_ASSERT_FAIL("cue relevance must be finite, non-negative, and fit in the frame range");
        return;
    }
    const auto relevance_frames = static_cast<SyncFrame>(
        std::ceil(static_cast<double>(cue.relevance_seconds) / options_.fixed_dt_seconds));
    const SyncFrame expire_frame = cue.frame + relevance_frames;
    const NetworkOwner* owner = registry.try_get<NetworkOwner>(cue.entity);
    for (ClientState& client_state : clients_) {
        if (client_state.replication == nullptr) {
            continue;
        }
        ServerClientReplicator& client = *client_state.replication;
        if (cue.only_replicate_to_owner && (owner == nullptr || owner->client != client.id)) {
            continue;
        }
        ClientEntityState* state = client.entities.try_get(slot);
        if (state == nullptr) {
            continue;
        }
        ashiato::BitBuffer payload = cue.payload;
#if defined(ASHIATO_SYNC_ENABLE_TRACING) && defined(ASHIATO_SYNC_TRACE_COMPONENT_DATA)
        std::vector<ashiato::SerializationTraceScope> payload_trace_scopes = cue.payload_trace_scopes;
#endif
        if (cue.value.has_value()) {
            struct ReferenceContextData {
                ReplicationServer* server = nullptr;
                ServerClientReplicator* client = nullptr;
                std::uint32_t source_slot = 0;
            } reference_context_data{this, &client, slot};
            EntityReferenceContext reference_context;
            reference_context.userContext = &reference_context_data;
            reference_context.network_entity_id_tier0_bits = options_.protocol.network_entity_id_tier0_bits;
            reference_context.server_network_id_for_entity = [](void* userContext, ashiato::Entity entity) {
                ReferenceContextData& data = *static_cast<ReferenceContextData*>(userContext);
                const auto found = data.server->entity_to_replicated_index_.find(entity.value);
                if (found == data.server->entity_to_replicated_index_.end()) {
                    return 0U;
                }
                const std::uint32_t reference_slot = found->second;
                if (reference_slot >= data.server->replicated_.size() ||
                    data.client->entities.try_get(reference_slot) == nullptr ||
                    !data.server->replicated_[reference_slot].active) {
                    return 0U;
                }
                if (reference_slot != data.source_slot) {
                    data.client->entities.try_get(reference_slot)->reference_priority_boost_pending = true;
                }
                return data.client->network_id_for(*data.server, reference_slot);
            };
            EntityReferenceContext* references = settings.cue_ops[cue.type].references_entities ? &reference_context : nullptr;
            payload.clear();
#ifdef ASHIATO_SYNC_ENABLE_TRACING
            ScopedSerializationTraceCapture cue_serialization_capture(
                tracer_,
                SyncTracePayloadSource::Network,
                SyncTraceRole::Server,
                client.id,
                cue.frame,
                "server_cue_payload",
                false);
            cue_serialization_capture.set_target(&payload);
            if (cue_serialization_capture.active()) {
                SyncTraceEvent& event = cue_serialization_capture.event();
                event.server_entity = cue.entity;
                event.wire_network_id = client.network_id_for(*this, slot);
                if (ClientEntityState* client_entity = client.entities.try_get(slot)) {
                    event.network_version = client_entity->network_version;
                    event.client_network_id =
                        make_client_entity_network_id(client.id, event.wire_network_id, client_entity->network_version);
                }
                event.cue_type = cue.type;
                append_trace_cue_name(settings, cue.type, event);
                add_sync_trace_payload_tag(event, sync_trace_payload_tag_outgoing);
                append_trace_data_field(event, "payload_kind", "cue");
            }
            ashiato::ComponentSerializationContext serialization_context{
                references,
                cue_serialization_capture.payload_capture()};
            serialization_context.currentFrame = cue.frame;
            serialization_context.previousFrame = 0U;
            {
                ASHIATO_SYNC_TRACE_SCOPE_WITH_CONTEXT(cue_serialization_capture, settings.cue_ops[cue.type].name.c_str());
                settings.cue_ops[cue.type].serialize(cue.value.data(), payload, serialization_context);
            }
            if (cue_serialization_capture.active()) {
                cue_serialization_capture.finish();
                SyncTraceEvent& event = cue_serialization_capture.event();
                event.payload_bits = payload.bit_size();
                event.wire_bits = payload.bit_size();
                append_trace_data_field(event, "payload_bits", static_cast<std::uint64_t>(payload.bit_size()));
                append_trace_data_field(event, "payload_bytes", static_cast<std::uint64_t>(payload.byte_size()));
#ifdef ASHIATO_SYNC_TRACE_COMPONENT_DATA
                payload_trace_scopes = event.payload_scopes;
#endif
                cue_serialization_capture.flush();
            }
#else
            ashiato::ComponentSerializationContext serialization_context{references};
            serialization_context.currentFrame = cue.frame;
            serialization_context.previousFrame = 0U;
            settings.cue_ops[cue.type].serialize(cue.value.data(), payload, serialization_context);
#endif
        }
        if (payload.bit_size() > protocol::max_cue_payload_bits) {
            ASHIATO_SYNC_ASSERT_FAIL("serialized cue payload exceeds the protocol limit");
            continue;
        }
        ClientEntityState::PendingCue pending_cue;
        pending_cue.frame = cue.frame;
        pending_cue.expire_frame = expire_frame;
        pending_cue.type = cue.type;
        pending_cue.relevance_seconds = cue.relevance_seconds;
        pending_cue.payload = std::move(payload);
#if defined(ASHIATO_SYNC_ENABLE_TRACING) && defined(ASHIATO_SYNC_TRACE_COMPONENT_DATA)
        pending_cue.payload_trace_scopes = std::move(payload_trace_scopes);
#endif
        state->pending_cues.push_back(std::move(pending_cue));
        client.mark_dirty(*this, slot, frame_);
    }
}

void ReplicationServer::mark_dirty_component(
    const SyncSettings& settings,
    std::uint32_t slot,
    ashiato::Entity component) {
    if (slot >= replicated_.size() || !replicated_[slot].active) {
        return;
    }
    ReplicatedSlot& replicated = replicated_[slot];
    if (replicated.archetype.value >= settings.archetypes.size()) {
        return;
    }
    const SyncArchetype& archetype = settings.archetypes[replicated.archetype.value];
    if (replicated.component_dirty_generations.size() < sync_slot_count(archetype)) {
        replicated.component_dirty_generations.resize(sync_slot_count(archetype), 1U);
    }
    for (std::size_t index = 0; index < archetype.components.size(); ++index) {
        if (archetype.components[index].component == component) {
            const std::size_t dirty_index = index + 1U;
            ++replicated.component_dirty_generations[dirty_index];
            if (replicated.component_dirty_generations[dirty_index] == 0) {
                replicated.component_dirty_generations[dirty_index] = 1;
            }
            return;
        }
    }
}

void ReplicationServer::mark_dirty_tag(const SyncSettings& settings, std::uint32_t slot, ashiato::Entity tag) {
    if (slot >= replicated_.size() || !replicated_[slot].active) {
        return;
    }
    const ReplicatedSlot& replicated = replicated_[slot];
    if (replicated.archetype.value >= settings.archetypes.size()) {
        return;
    }
    const SyncArchetype& archetype = settings.archetypes[replicated.archetype.value];
    for (const SyncTagReplication& replication : archetype.tags) {
        if (replication.tag == tag) {
            mark_dirty_tags(settings, slot);
            return;
        }
    }
}

void ReplicationServer::mark_dirty_tags(const SyncSettings& settings, std::uint32_t slot) {
    if (slot >= replicated_.size() || !replicated_[slot].active) {
        return;
    }
    ReplicatedSlot& replicated = replicated_[slot];
    if (replicated.archetype.value >= settings.archetypes.size()) {
        return;
    }
    const SyncArchetype& archetype = settings.archetypes[replicated.archetype.value];
    if (archetype.tags.empty()) {
        return;
    }
    if (replicated.component_dirty_generations.size() < sync_slot_count(archetype)) {
        replicated.component_dirty_generations.resize(sync_slot_count(archetype), 1U);
    }
    ++replicated.component_dirty_generations[0];
    if (replicated.component_dirty_generations[0] == 0) {
        replicated.component_dirty_generations[0] = 1;
    }
}

void ReplicationServer::mark_owner_visibility_dirty(const SyncSettings& settings, std::uint32_t slot) {
    if (slot >= replicated_.size() || !replicated_[slot].active) {
        return;
    }
    ReplicatedSlot& replicated = replicated_[slot];
    if (replicated.archetype.value >= settings.archetypes.size()) {
        return;
    }
    const SyncArchetype& archetype = settings.archetypes[replicated.archetype.value];
    if (replicated.component_dirty_generations.size() < sync_slot_count(archetype)) {
        replicated.component_dirty_generations.resize(sync_slot_count(archetype), 1U);
    }
    for (const SyncTagReplication& replication : archetype.tags) {
        if (replication.audience != ReplicationAudience::All) {
            ++replicated.component_dirty_generations[0];
            if (replicated.component_dirty_generations[0] == 0) {
                replicated.component_dirty_generations[0] = 1;
            }
            break;
        }
    }
    for (std::size_t index = 0; index < archetype.components.size(); ++index) {
        if (archetype.components[index].audience != ReplicationAudience::All) {
            const std::size_t dirty_index = index + 1U;
            ++replicated.component_dirty_generations[dirty_index];
            if (replicated.component_dirty_generations[dirty_index] == 0) {
                replicated.component_dirty_generations[dirty_index] = 1;
            }
        }
    }
}

bool ReplicationServer::archetype_is_same_frame_cacheable(const SyncArchetype& archetype) {
    for (const SyncTagReplication& replication : archetype.tags) {
        if (replication.audience != ReplicationAudience::All) {
            return false;
        }
    }
    for (const ComponentReplication& replication : archetype.components) {
        if (replication.audience != ReplicationAudience::All) {
            return false;
        }
    }
    return true;
}

bool server_detail::ServerClientReplicator::UpdateWriter::serialize_entity(
    ReplicationServer& replication_server,
    const ashiato::Registry& registry,
    const SyncSettings& settings,
    ServerClientReplicator& client,
    std::uint32_t slot,
    SyncFrame frame,
    std::uint64_t component_mask,
    SerializedEntity& out) {
    if (slot >= replication_server.replicated_slot_count() || client.entities.try_get(slot) == nullptr) {
        return false;
    }

    const SyncArchetypeId archetype = replication_server.replicated_slot_archetype(slot);
    if (archetype.value >= settings.archetypes.size()) {
        return false;
    }

    const std::uint32_t quantized_frame = replication_server.quantized_frame_for_client(
        registry,
        settings,
        client,
        slot,
        frame,
        quantized_frame_scratch_,
        quantized_frame_dirty_scratch_);
    if (quantized_frame == server_detail::invalid_quantized_frame_id) {
        return false;
    }

    out.quantized_frame = quantized_frame;
    write_entity_record(
        replication_server,
        registry,
        settings,
        client,
        slot,
        quantized_frame,
        component_mask,
        out.payload
#ifdef ASHIATO_SYNC_ENABLE_TRACING
        ,
        &out.serialization_events
#endif
    );
#ifdef ASHIATO_SYNC_ENABLE_TRACING
    (void)archetype;
#endif
    return !out.payload.empty();
}

std::uint32_t ReplicationServer::find_or_create_quantized_frame(
    const ashiato::Registry& registry,
    const SyncSettings& settings,
    const ServerClientReplicator& client,
    std::uint32_t slot,
    SyncFrame frame,
    QuantizedFrameData& scratch,
    std::vector<std::uint64_t>& scratch_dirty_generations) {
    if (slot >= replicated_.size()) {
        return invalid_quantized_frame_id;
    }

    const ReplicatedSlot& replicated = replicated_[slot];
    const SyncArchetype& archetype = settings.archetypes[replicated.archetype.value];
    if (replicated.same_frame_cacheable &&
        replicated.same_frame_quantized_frame_frame == frame &&
        replicated.same_frame_quantized_frame != invalid_quantized_frame_id &&
        replicated.same_frame_quantized_frame < quantized_frames_.size()) {
        const QuantizedFrame& cached = quantized_frames_[replicated.same_frame_quantized_frame];
        if (cached.active && cached.slot == slot && cached.frame == frame &&
            cached.archetype == replicated.archetype) {
            return replicated.same_frame_quantized_frame;
        }
    }

    const NetworkOwner* owner = registry.try_get<NetworkOwner>(replicated.entity);
    const ClientId owner_client = owner != nullptr ? owner->client : invalid_client_id;
    const ClientEntityState* entity_state = client.entities.try_get(slot);
    const QuantizedFrame* baseline_quantized_frame = nullptr;
    if (entity_state != nullptr &&
        entity_state->baseline != invalid_quantized_frame_id &&
        entity_state->baseline < quantized_frames_.size() &&
        quantized_frames_[entity_state->baseline].active &&
        quantized_frames_[entity_state->baseline].archetype == replicated.archetype) {
        baseline_quantized_frame = &quantized_frames_[entity_state->baseline];
    }

    if (!init_frame_data(archetype, scratch)) {
        return invalid_quantized_frame_id;
    }
    scratch_dirty_generations.assign(sync_slot_count(archetype), 0U);
    if (has_tag_slot(archetype)) {
        scratch.tag_mask = visible_tag_mask(registry, archetype, replicated.entity, client.id);
        scratch_dirty_generations[0] = !replicated.component_dirty_generations.empty()
            ? replicated.component_dirty_generations[0]
            : 0;
    }
    for (std::size_t component_index = 0; component_index < archetype.components.size(); ++component_index) {
        const ComponentReplication& replication = archetype.components[component_index];
        if (!replication_audience_matches(replication.audience, owner_client, client.id)) {
            continue;
        }

        const void* component_value = registry.get(replicated.entity, replication.component);
        if (component_value == nullptr) {
            continue;
        }

        if (component_index >= archetype.component_ops.size()) {
            throw std::logic_error("sync component traits are not registered for replicated component");
        }
        const SyncComponentOps& ops = archetype.component_ops[component_index];

        const std::size_t dirty_index = component_index + 1U;
        const std::uint64_t dirty_generation = dirty_index < replicated.component_dirty_generations.size()
            ? replicated.component_dirty_generations[dirty_index]
            : 0;
        scratch_dirty_generations[dirty_index] = dirty_generation;

        std::uint8_t* destination = mutable_frame_component_data(archetype, scratch, component_index);
        if (destination == nullptr) {
            return invalid_quantized_frame_id;
        }
        if (baseline_quantized_frame != nullptr &&
            dirty_index < baseline_quantized_frame->dirty_generations.size() &&
            baseline_quantized_frame->dirty_generations[dirty_index] == dirty_generation &&
            frame_has_component(baseline_quantized_frame->data, component_index)) {
            const std::size_t offset = archetype.component_offsets[component_index];
            const std::size_t size = archetype.component_ops[component_index].serialization.quantized_size;
            if (offset + size > baseline_quantized_frame->data.bytes.size()) {
                return invalid_quantized_frame_id;
            }
            std::memcpy(destination, baseline_quantized_frame->data.bytes.data() + offset, size);
        } else {
            if (ops.serialization.quantize == nullptr) {
                return invalid_quantized_frame_id;
            }
            ops.serialization.quantize(component_value, destination);
        }
    }

    if (scratch.present_mask == 0U && !has_tag_slot(archetype)) {
        return invalid_quantized_frame_id;
    }

    for (const std::uint32_t index : replicated_[slot].quantized_frames) {
        if (index >= quantized_frames_.size()) {
            continue;
        }
        const QuantizedFrame& quantized_frame = quantized_frames_[index];
        if (quantized_frame.active && quantized_frame.slot == slot && quantized_frame.frame == frame &&
            quantized_frame.archetype == replicated.archetype &&
            same_quantized_frame_components(quantized_frame, scratch, scratch_dirty_generations)) {
            return index;
        }
    }

    std::uint32_t index = invalid_quantized_frame_id;
    if (!free_quantized_frames_.empty()) {
        index = free_quantized_frames_.back();
        free_quantized_frames_.pop_back();
    } else {
        if (quantized_frames_.size() > static_cast<std::size_t>(std::numeric_limits<std::uint32_t>::max())) {
            throw std::length_error("Ashiato Sync quantized frame space exhausted");
        }
        index = static_cast<std::uint32_t>(quantized_frames_.size());
        quantized_frames_.push_back(QuantizedFrame{});
    }

    QuantizedFrame& quantized_frame = quantized_frames_[index];
    quantized_frame.slot = slot;
    quantized_frame.frame = frame;
    quantized_frame.archetype = replicated.archetype;
    quantized_frame.ref_count = 0;
    quantized_frame.active = true;
    quantized_frame.data = std::move(scratch);
    quantized_frame.dirty_generations = std::move(scratch_dirty_generations);
    replicated_[slot].quantized_frames.push_back(index);
    if (replicated_[slot].same_frame_cacheable) {
        replicated_[slot].same_frame_quantized_frame = index;
        replicated_[slot].same_frame_quantized_frame_frame = frame;
    }
    return index;
}

void server_detail::ServerClientReplicator::UpdateWriter::write_entity_record(
    ReplicationServer& replication_server,
    const ashiato::Registry& registry,
    const SyncSettings& settings,
    ServerClientReplicator& client,
    std::uint32_t slot,
    std::uint32_t quantized_frame_id,
    std::uint64_t component_mask,
    ashiato::BitBuffer& out
#ifdef ASHIATO_SYNC_ENABLE_TRACING
    ,
    std::vector<SyncTraceEvent>* serialization_events
#endif
) {
    const ClientEntityState* entity_state = client.entities.try_get(slot);
    if (entity_state == nullptr) {
        return;
    }
    const QuantizedFrameData* quantized_data = replication_server.quantized_frame_data(quantized_frame_id);
    if (quantized_data == nullptr) {
        return;
    }
    const SyncFrame quantized_frame = replication_server.quantized_frame_frame(quantized_frame_id);
    const SyncArchetypeId quantized_archetype = replication_server.quantized_frame_archetype(quantized_frame_id);
    bool delta = entity_state->baseline != server_detail::invalid_quantized_frame_id &&
        replication_server.quantized_frame_active(entity_state->baseline) &&
        replication_server.quantized_frame_archetype(entity_state->baseline) == quantized_archetype;
    if (delta) {
        const QuantizedFrameData* baseline_data = replication_server.quantized_frame_data(entity_state->baseline);
        delta = baseline_data != nullptr && baseline_data->present_mask == quantized_data->present_mask;
    }

    const std::uint32_t network_id = client.network_id_for(replication_server, slot);
    if (network_id == 0U) {
        return;
    }
#ifdef ASHIATO_SYNC_ENABLE_TRACING
    ScopedSerializationTraceCapture serialization_capture(
        replication_server.server_tracer(),
        SyncTracePayloadSource::Network,
        SyncTraceRole::Server,
        client.id,
        quantized_frame,
        "server_update_entity_record",
        false);
    serialization_capture.set_target(&out);
    if (serialization_capture.active()) {
        SyncTraceEvent& event = serialization_capture.event();
        event.server_entity = replication_server.replicated_slot_entity(slot);
        event.wire_network_id = network_id;
        event.network_version = entity_state->network_version;
        event.client_network_id = make_client_entity_network_id(client.id, network_id, entity_state->network_version);
        event.archetype = quantized_archetype;
        add_sync_trace_payload_tag(event, sync_trace_payload_tag_outgoing);
        event.data = "message=server_update_record";
    }
    auto store_serialization_event = [&]() {
        if (serialization_capture.active() && serialization_events != nullptr) {
            serialization_events->push_back(serialization_capture.release_event());
        }
    };
#endif
    struct ReferenceContextData {
        ReplicationServer* server = nullptr;
        ServerClientReplicator* client = nullptr;
        std::uint32_t source_slot = 0;
    } reference_context_data{&replication_server, &client, slot};
    EntityReferenceContext reference_context;
    bool reference_context_initialized = false;
    auto references_for_component = [&]() -> EntityReferenceContext* {
        if (!reference_context_initialized) {
            reference_context.userContext = &reference_context_data;
            reference_context.network_entity_id_tier0_bits =
                replication_server.options().protocol.network_entity_id_tier0_bits;
            reference_context.server_network_id_for_entity = [](void* userContext, ashiato::Entity entity) {
                ReferenceContextData& data = *static_cast<ReferenceContextData*>(userContext);
                const std::uint32_t reference_slot = data.server->replicated_slot_for_entity(entity);
                if (reference_slot == server_detail::invalid_quantized_frame_id ||
                    data.client->entities.try_get(reference_slot) == nullptr ||
                    !data.server->replicated_slot_active(reference_slot)) {
                    return 0U;
                }
                if (reference_slot != data.source_slot) {
                    data.client->entities.try_get(reference_slot)->reference_priority_boost_pending = true;
                }
                return data.client->network_id_for(*data.server, reference_slot);
            };
            reference_context_initialized = true;
        }
        return &reference_context;
    };
    auto write_pending_cues =
#ifdef ASHIATO_SYNC_ENABLE_TRACING
    [
        entity_state,
        &serialization_capture,
        &out,
        quantized_frame,
        &replication_server,
        &client,
        slot,
        network_id,
        quantized_archetype,
        &settings]
#else
    [
        entity_state,
        &out,
        quantized_frame]
#endif
    () {
        for (const ClientEntityState::PendingCue& cue : entity_state->pending_cues) {
            ASHIATO_SERIALIZE_BOOL_TRACE_WITH_CONTEXT(serialization_capture, out, true, "has_cue");
#ifdef ASHIATO_SYNC_ENABLE_TRACING
            const std::size_t cue_wire_begin_bits = out.bit_size();
#endif
            {
                ASHIATO_SYNC_TRACE_SCOPE_WITH_CONTEXT(serialization_capture, "cue");
                {
                    ASHIATO_SYNC_TRACE_SCOPE_WITH_CONTEXT(serialization_capture, "frame");
                    protocol::write_cue_frame(out, quantized_frame, cue.frame);
                }
                ASHIATO_SERIALIZE_TRACE_WITH_CONTEXT(serialization_capture, out, cue.type, 16U, "type");
#if defined(ASHIATO_SYNC_ENABLE_TRACING) && defined(ASHIATO_SYNC_TRACE_COMPONENT_DATA)
                if (!cue.payload_trace_scopes.empty()) {
                    push_buffer_bits_with_replayed_trace(
                        serialization_capture,
                        out,
                        cue.payload,
                        cue.payload_trace_scopes,
                        "payload");
                } else
#endif
                {
                    ASHIATO_SERIALIZE_BUFFER_TRACE_WITH_CONTEXT(serialization_capture, out, cue.payload, "payload");
                }
            }
#ifdef ASHIATO_SYNC_ENABLE_TRACING
            if (replication_server.server_tracer() != nullptr && replication_server.server_tracer()->enabled()) {
                const std::size_t wire_bits = out.bit_size() - cue_wire_begin_bits;
                SyncTraceEvent event = make_server_trace_event(SyncTraceEventType::CueSent, client.id, cue.frame);
                event.server_entity = replication_server.replicated_slot_entity(slot);
                event.wire_network_id = network_id;
                event.network_version = entity_state->network_version;
                event.client_network_id = make_client_entity_network_id(client.id, network_id, entity_state->network_version);
                event.archetype = quantized_archetype;
                event.cue_type = cue.type;
                event.wire_bits = wire_bits;
                event.payload_bits = cue.payload.bit_size();
                append_trace_cue_name(settings, cue.type, event);
                append_trace_cue_data(replication_server.server_tracer(), settings, cue.type, cue.payload, event);
                append_trace_data_field(event, "source", "server");
                append_trace_data_field(event, "payload_kind", "cue");
                append_trace_data_field(event, "payload_bits", static_cast<std::uint64_t>(cue.payload.bit_size()));
                append_trace_data_field(event, "payload_bytes", static_cast<std::uint64_t>(cue.payload.byte_size()));
                append_trace_data_field(event, "wire_bits", static_cast<std::uint64_t>(wire_bits));
                append_trace_data_field(event, "wire_bytes", static_cast<std::uint64_t>(protocol::bytes_for_bits(wire_bits)));
                replication_server.server_tracer()->trace(event);
            }
#endif
        }
        ASHIATO_SERIALIZE_BOOL_TRACE_WITH_CONTEXT(serialization_capture, out, false, "has_cue");
    };

    {
        ASHIATO_SYNC_TRACE_SCOPE_WITH_CONTEXT(serialization_capture, "entity_header");
        {
            ASHIATO_SYNC_TRACE_SCOPE_WITH_CONTEXT(serialization_capture, "network_id");
            protocol::write_network_entity_id(
                out,
                network_id,
                replication_server.options().protocol.network_entity_id_tier0_bits);
        }
        ASHIATO_SERIALIZE_BOOL_TRACE_WITH_CONTEXT(serialization_capture, out, !delta, "full_state");
        if (!delta) {
            ASHIATO_SERIALIZE_TRACE_WITH_CONTEXT(serialization_capture, out, quantized_archetype.value, 32U, "archetype");
        } else {
            ASHIATO_SYNC_TRACE_SCOPE_WITH_CONTEXT(serialization_capture, "baseline_frame");
            protocol::write_baseline_frame(
                out,
                quantized_frame,
                replication_server.quantized_frame_frame(entity_state->baseline));
        }
    }

    const SyncArchetype& archetype = settings.archetypes[quantized_archetype.value];
    if (delta) {
        const QuantizedFrameData* baseline_quantized_frame =
            replication_server.quantized_frame_data(entity_state->baseline);
        if (baseline_quantized_frame == nullptr) {
            return;
        }
        std::uint64_t changed_mask = 0;
        if (has_tag_slot(archetype)) {
            const std::uint64_t tag_bit_mask = archetype.tags.size() == 64U
                ? std::numeric_limits<std::uint64_t>::max()
                : ((std::uint64_t{1} << archetype.tags.size()) - 1U);
            const bool tags_changed =
                (quantized_data->tag_mask & tag_bit_mask) !=
                (baseline_quantized_frame->tag_mask & tag_bit_mask);
            ASHIATO_SERIALIZE_BOOL_TRACE_WITH_CONTEXT(serialization_capture, out, tags_changed, "tags_changed");
            if (tags_changed) {
                changed_mask |= sync_slot_bit(0);
            }
        } else {
            ASHIATO_SERIALIZE_BOOL_TRACE_WITH_CONTEXT(serialization_capture, out, false, "tags_changed");
        }
        for (std::size_t component_index = 0; component_index < archetype.components.size(); ++component_index) {
            if (!frame_has_component(*quantized_data, component_index) ||
                (component_mask & (std::uint64_t{1} << component_index)) == 0U) {
                ASHIATO_SERIALIZE_BOOL_TRACE_WITH_CONTEXT(serialization_capture, out, false, "component_changed");
                continue;
            }
            const std::size_t offset = archetype.component_offsets[component_index];
            const std::size_t size = archetype.component_ops[component_index].serialization.quantized_size;
            if (offset + size > quantized_data->bytes.size() ||
                offset + size > baseline_quantized_frame->bytes.size()) {
                throw std::logic_error("replicated quantized frame component bytes are out of range");
            }
            const bool component_changed =
                std::memcmp(
                    (*quantized_data).bytes.data() + offset,
                    baseline_quantized_frame->bytes.data() + offset,
                    size) != 0;
            ASHIATO_SERIALIZE_BOOL_TRACE_WITH_CONTEXT(serialization_capture, out, component_changed, "component_changed");
            if (component_changed) {
                changed_mask |= sync_slot_bit(component_index + 1U);
            }
        }
        if ((changed_mask & sync_slot_bit(0)) != 0U) {
            {
                ASHIATO_SYNC_TRACE_SCOPE_WITH_CONTEXT(serialization_capture, "tag_mask");
                ASHIATO_SERIALIZE_UNSIGNED_TRACE_WITH_CONTEXT(
                    serialization_capture,
                    out,
                    quantized_data->tag_mask,
                    archetype.tags.size(),
                    "value");
            }
#ifdef ASHIATO_SYNC_ENABLE_TRACING
            if (replication_server.server_tracer() != nullptr && replication_server.server_tracer()->enabled()) {
                for (std::size_t tag_index = 0; tag_index < archetype.tags.size(); ++tag_index) {
                    SyncTraceEvent event = make_server_trace_event(SyncTraceEventType::TagSent, client.id, quantized_frame);
                    event.server_entity = replication_server.replicated_slot_entity(slot);
                    event.wire_network_id = network_id;
                    event.network_version = entity_state->network_version;
                    event.client_network_id = make_client_entity_network_id(client.id, network_id, entity_state->network_version);
                    event.archetype = quantized_archetype;
                    event.tag = archetype.tags[tag_index].tag;
                    event.remove = ((*quantized_data).tag_mask & (std::uint64_t{1} << tag_index)) == 0U;
                    replication_server.server_tracer()->trace(event);
                }
            }
#endif
        }
        for (std::size_t component_index = 0; component_index < archetype.components.size(); ++component_index) {
            if ((changed_mask & sync_slot_bit(component_index + 1U)) == 0U) {
                continue;
            }
            if (component_index >= archetype.component_ops.size()) {
                throw std::logic_error("sync component traits are not registered for replicated component");
            }
            const SyncComponentOps& ops = archetype.component_ops[component_index];
            const std::uint8_t* previous = frame_component_data(archetype, *baseline_quantized_frame, component_index);
            const std::uint8_t* current = frame_component_data(archetype, (*quantized_data), component_index);
            if (previous == nullptr || current == nullptr) {
                throw std::logic_error("replicated quantized frame component bytes are missing");
            }
            EntityReferenceContext* references = ops.references_entities ? references_for_component() : nullptr;
            const SyncFrame baseline_frame = replication_server.quantized_frame_frame(entity_state->baseline);
#ifdef ASHIATO_SYNC_ENABLE_TRACING
            ashiato::ComponentSerializationContext serialization_context{references, serialization_capture.payload_capture()};
#else
            ashiato::ComponentSerializationContext serialization_context{references};
#endif
            serialization_context.currentFrame = quantized_frame;
            serialization_context.previousFrame = baseline_frame;
#ifdef ASHIATO_SYNC_ENABLE_TRACING
            const std::size_t component_wire_begin_bits = out.bit_size();
#endif
            {
                ASHIATO_SYNC_TRACE_SCOPE_WITH_CONTEXT(
                    serialization_capture,
                    archetype.component_ops[component_index].serialization.name.c_str());
                ops.serialization.serialize(previous, current, out, serialization_context);
            }
#ifdef ASHIATO_SYNC_ENABLE_TRACING
            if (replication_server.server_tracer() != nullptr && replication_server.server_tracer()->enabled()) {
                const std::size_t wire_bits = out.bit_size() - component_wire_begin_bits;
                SyncTraceEvent event = make_server_trace_event(SyncTraceEventType::ComponentSent, client.id, quantized_frame);
                event.server_entity = replication_server.replicated_slot_entity(slot);
                event.wire_network_id = network_id;
                event.network_version = entity_state->network_version;
                event.client_network_id = make_client_entity_network_id(client.id, network_id, entity_state->network_version);
                event.archetype = quantized_archetype;
                event.component = archetype.components[component_index].component;
                event.wire_bits = wire_bits;
                event.payload_bits = wire_bits;
                append_trace_component_data(replication_server.server_tracer(), archetype, component_index, current, event);
                append_trace_data_field(event, "payload_kind", "component");
                append_trace_data_field(event, "wire_bits", static_cast<std::uint64_t>(wire_bits));
                append_trace_data_field(event, "wire_bytes", static_cast<std::uint64_t>(protocol::bytes_for_bits(wire_bits)));
                replication_server.server_tracer()->trace(event);
            }
#endif
        }
        write_pending_cues();
        (void)registry;
#ifdef ASHIATO_SYNC_ENABLE_TRACING
        store_serialization_event();
#endif
        return;
    }

    std::uint16_t component_count = 0;
    std::uint64_t present_sync_slots = 0;
    if (has_tag_slot(archetype)) {
        ++component_count;
        present_sync_slots |= sync_slot_bit(0);
    }
    for (std::size_t component_index = 0; component_index < archetype.components.size(); ++component_index) {
        if (frame_has_component((*quantized_data), component_index) &&
            (component_mask & (std::uint64_t{1} << component_index)) != 0U) {
            ++component_count;
            present_sync_slots |= sync_slot_bit(component_index + 1U);
        }
    }

    const std::size_t sync_slots = sync_slot_count(archetype);
    const std::size_t sync_slot_bits = protocol::bits_for_range(sync_slot_count(archetype));
    const std::size_t slot_list_bits = 16U + static_cast<std::size_t>(component_count) * sync_slot_bits;
    const bool use_presence_mask = sync_slots < slot_list_bits;
    {
        ASHIATO_SYNC_TRACE_SCOPE_WITH_CONTEXT(serialization_capture, "presence");
        ASHIATO_SERIALIZE_BOOL_TRACE_WITH_CONTEXT(serialization_capture, out, use_presence_mask, "uses_mask");
        if (use_presence_mask) {
            ASHIATO_SERIALIZE_UNSIGNED_TRACE_WITH_CONTEXT(serialization_capture, out, present_sync_slots, sync_slots, "mask");
        } else {
            ASHIATO_SERIALIZE_TRACE_WITH_CONTEXT(
                serialization_capture,
                out,
                static_cast<std::int64_t>(component_count),
                16U,
                "component_count");
        }
    }

    if (has_tag_slot(archetype)) {
        {
            ASHIATO_SYNC_TRACE_SCOPE_WITH_CONTEXT(serialization_capture, "tag_mask");
            if (!use_presence_mask) {
                ASHIATO_SERIALIZE_TRACE_WITH_CONTEXT(serialization_capture, out, 0, sync_slot_bits, "slot");
            }
            ASHIATO_SERIALIZE_UNSIGNED_TRACE_WITH_CONTEXT(
                serialization_capture,
                out,
                (*quantized_data).tag_mask,
                archetype.tags.size(),
                "value");
        }
#ifdef ASHIATO_SYNC_ENABLE_TRACING
        if (replication_server.server_tracer() != nullptr && replication_server.server_tracer()->enabled()) {
            for (std::size_t tag_index = 0; tag_index < archetype.tags.size(); ++tag_index) {
                SyncTraceEvent event = make_server_trace_event(SyncTraceEventType::TagSent, client.id, quantized_frame);
                event.server_entity = replication_server.replicated_slot_entity(slot);
                event.wire_network_id = network_id;
                event.network_version = entity_state->network_version;
                event.client_network_id = make_client_entity_network_id(client.id, network_id, entity_state->network_version);
                event.archetype = quantized_archetype;
                event.tag = archetype.tags[tag_index].tag;
                event.remove = ((*quantized_data).tag_mask & (std::uint64_t{1} << tag_index)) == 0U;
                replication_server.server_tracer()->trace(event);
            }
        }
#endif
    }
    for (std::size_t component_index = 0; component_index < archetype.components.size(); ++component_index) {
        if (!frame_has_component((*quantized_data), component_index) ||
            (component_mask & (std::uint64_t{1} << component_index)) == 0U) {
            continue;
        }
        if (component_index >= archetype.component_ops.size()) {
            throw std::logic_error("sync component traits are not registered for replicated component");
        }
        const SyncComponentOps& ops = archetype.component_ops[component_index];
        const std::uint8_t* current = frame_component_data(archetype, (*quantized_data), component_index);
        if (current == nullptr) {
            throw std::logic_error("replicated quantized frame component bytes are missing");
        }

        if (!use_presence_mask) {
            ASHIATO_SERIALIZE_TRACE_WITH_CONTEXT(
                serialization_capture,
                out,
                static_cast<std::int64_t>(component_index + 1U),
                sync_slot_bits,
                "component_slot");
        }
        EntityReferenceContext* references = ops.references_entities ? references_for_component() : nullptr;
#ifdef ASHIATO_SYNC_ENABLE_TRACING
        ashiato::ComponentSerializationContext serialization_context{references, serialization_capture.payload_capture()};
#else
        ashiato::ComponentSerializationContext serialization_context{references};
#endif
        serialization_context.currentFrame = quantized_frame;
        serialization_context.previousFrame = 0U;
#ifdef ASHIATO_SYNC_ENABLE_TRACING
        const std::size_t component_wire_begin_bits = out.bit_size();
#endif
        {
            ASHIATO_SYNC_TRACE_SCOPE_WITH_CONTEXT(
                serialization_capture,
                archetype.component_ops[component_index].serialization.name.c_str());
            ops.serialization.serialize(nullptr, current, out, serialization_context);
        }
#ifdef ASHIATO_SYNC_ENABLE_TRACING
        if (replication_server.server_tracer() != nullptr && replication_server.server_tracer()->enabled()) {
            const std::size_t wire_bits = out.bit_size() - component_wire_begin_bits;
            SyncTraceEvent event = make_server_trace_event(SyncTraceEventType::ComponentSent, client.id, quantized_frame);
            event.server_entity = replication_server.replicated_slot_entity(slot);
            event.wire_network_id = network_id;
            event.network_version = entity_state->network_version;
            event.client_network_id = make_client_entity_network_id(client.id, network_id, entity_state->network_version);
            event.archetype = quantized_archetype;
            event.component = archetype.components[component_index].component;
            event.wire_bits = wire_bits;
            event.payload_bits = wire_bits;
            append_trace_component_data(replication_server.server_tracer(), archetype, component_index, current, event);
            append_trace_data_field(event, "payload_kind", "component");
            append_trace_data_field(event, "wire_bits", static_cast<std::uint64_t>(wire_bits));
            append_trace_data_field(event, "wire_bytes", static_cast<std::uint64_t>(protocol::bytes_for_bits(wire_bits)));
            replication_server.server_tracer()->trace(event);
        }
#endif
    }

    write_pending_cues();

    (void)registry;
#ifdef ASHIATO_SYNC_ENABLE_TRACING
    store_serialization_event();
#endif
}

bool ReplicationServer::valid_archetype(const ashiato::Registry& registry, SyncArchetypeId archetype) const {
    const SyncSettings& settings = registry.get<SyncSettings>();
    return archetype.value < settings.archetypes.size();
}

bool ReplicationServer::upsert_replicated(ashiato::Registry& registry, ashiato::Entity entity, SyncArchetypeId archetype) {
    if (!registry.alive(entity) || !valid_archetype(registry, archetype)) {
        deactivate_entity_index(ashiato::Registry::entity_index(entity));
        return false;
    }

    const EntityKey key = entity.value;
    const auto found = entity_to_replicated_index_.find(key);
    if (found != entity_to_replicated_index_.end()) {
        const std::uint32_t slot = found->second;
        if (slot >= replicated_.size() || !replicated_[slot].active) {
            entity_to_replicated_index_.erase(found);
            entity_index_to_replicated_index_.erase(ashiato::Registry::entity_index(entity));
        } else if (replicated_[slot].archetype != archetype) {
            throw std::logic_error("replicated entity archetype cannot change while syncing");
        } else {
            return true;
        }
    }

    deactivate_entity_index(ashiato::Registry::entity_index(entity));

    const std::uint32_t slot = allocate_replicated_slot(entity, archetype);
    const SyncSettings& settings = registry.get<SyncSettings>();
    if (archetype.value < settings.archetypes.size()) {
        set_sync_debug_name(registry, entity, settings.archetypes[archetype.value]);
        replicated_[slot].same_frame_cacheable =
            archetype_is_same_frame_cacheable(settings.archetypes[archetype.value]);
        replicated_[slot].component_dirty_generations.assign(
            sync_slot_count(settings.archetypes[archetype.value]),
            1U);
    }
    entity_to_replicated_index_[key] = slot;
    entity_index_to_replicated_index_[ashiato::Registry::entity_index(entity)] = slot;
    for (ClientState& client : clients_) {
        if (client.replication == nullptr) {
            continue;
        }
        ServerClientReplicator& replication = *client.replication;
        replication.ensure_capacity(replicated_.size());
        replication.entities.clear(*this, slot);
        replication.mark_dirty(*this, slot, frame_);
    }

    ++active_replicated_count_;
    return true;
}

std::uint32_t ReplicationServer::allocate_replicated_slot(ashiato::Entity entity, SyncArchetypeId archetype) {
    if (!free_replicated_indices_.empty()) {
        const std::uint32_t slot = free_replicated_indices_.back();
        free_replicated_indices_.pop_back();
        replicated_[slot] =
            ReplicatedSlot{entity, archetype, {}, {}, invalid_quantized_frame_id, 0, false, true};
        return slot;
    }

    if (replicated_.size() > static_cast<std::size_t>(std::numeric_limits<std::uint32_t>::max())) {
        throw std::length_error("Ashiato Sync replicated slot space exhausted");
    }

    const std::uint32_t slot = static_cast<std::uint32_t>(replicated_.size());
    replicated_.push_back(
        ReplicatedSlot{entity, archetype, {}, {}, invalid_quantized_frame_id, 0, false, true});
    return slot;
}

void ReplicationServer::deactivate_replicated(std::uint32_t slot) {
    if (slot >= replicated_.size() || !replicated_[slot].active) {
        return;
    }

    const ashiato::Entity entity = replicated_[slot].entity;
    post_tick_destroyed_slots_.push_back(ServerDestroyedReplicatedSlot{slot, entity});
    entity_to_replicated_index_.erase(entity.value);
    entity_index_to_replicated_index_.erase(ashiato::Registry::entity_index(entity));
    replicated_[slot].active = false;
    replicated_[slot].quantized_frames.clear();
    replicated_[slot].same_frame_quantized_frame = invalid_quantized_frame_id;
    replicated_[slot].same_frame_quantized_frame_frame = 0;
    replicated_[slot].same_frame_cacheable = false;
    free_replicated_indices_.push_back(slot);
    --active_replicated_count_;
    for (ClientState& client_state : clients_) {
        if (client_state.replication == nullptr) {
            continue;
        }
        ServerClientReplicator& client = *client_state.replication;
        client.enqueue_destroy(*this, slot, entity, frame_);
    }
    remove_replicated_from_client_replicators(slot);
}

void ReplicationServer::deactivate_entity_index(std::uint32_t entity_index) {
    const auto found = entity_index_to_replicated_index_.find(entity_index);
    if (found == entity_index_to_replicated_index_.end()) {
        return;
    }

    deactivate_replicated(found->second);
}

void ReplicationServer::remove_replicated_from_client_replicators(std::uint32_t slot) {
    for (ClientState& client : clients_) {
        if (client.replication != nullptr) {
            client.replication->clear_dirty(slot);
        }
    }
}

bool ReplicationServer::replicated_is_replicable(const ashiato::Registry& registry, std::uint32_t slot) const {
    if (slot >= replicated_.size() || !replicated_[slot].active) {
        return false;
    }

    const ashiato::Entity entity = replicated_[slot].entity;
    return registry.alive(entity) && registry.contains<Replicated>(entity);
}

}  // namespace ashiato::sync
