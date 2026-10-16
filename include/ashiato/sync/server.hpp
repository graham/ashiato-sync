#pragma once

#include "ashiato/sync/components.hpp"
#include "ashiato/sync/server_frame_consumer.hpp"

#include <cstddef>
#include <cstdint>
#include <limits>
#include <memory>
#include <string>
#include <unordered_map>
#include <utility>
#include <vector>

namespace ashiato::sync {

class SyncTracer;
class KTraceDirectoryWriter;
class FractionalTickSampler;

namespace server_detail {
struct ClientDirtyQueue;
struct ClientEntityState;
struct PacketAckRecord;
struct ServerClientReplicator;
struct SerializedEntity;
}

class ReplicationServer : private ashiato::RegistryDirtyFrameBroadcastListener {
public:
    struct ClientInputStats {
        std::uint64_t input_frames_applied = 0;
        std::uint64_t input_starvation_frames = 0;
        std::uint64_t input_reused_frames = 0;
        SyncFrame latest_received_input_frame = 0;
        SyncFrame latest_applied_input_frame = 0;
    };

    struct ClientBandwidthStats {
        bool dynamic = false;
        double target_bytes_per_second = 0.0;
        double available_bytes = 0.0;
        std::size_t in_flight_bytes = 0;
        std::uint64_t delivered_bytes_window = 0;
        float loss_rate = 0.0f;
    };

    struct ReplicationSendResult {
        std::size_t charged_bytes = 0;
        bool had_pending_data = false;
        bool stopped_for_budget = false;
    };

    struct ObservabilityStats {
        std::uint64_t client_packet_warnings = 0;
        std::uint64_t suppressed_client_packet_warnings = 0;
        std::uint64_t server_errors = 0;
        std::uint64_t client_connects_accepted = 0;
        std::uint64_t client_connects_rejected = 0;
        std::uint64_t clients_ready = 0;
        std::uint64_t clients_removed = 0;
        std::uint64_t clients_timed_out = 0;
        std::uint64_t dropped_fixed_step_frames = 0;
        std::uint64_t fixed_step_overflow_events = 0;
    };

    explicit ReplicationServer(ashiato::Registry& registry, ReplicationServerOptions options = {});
    ~ReplicationServer() override;
    ReplicationServer(const ReplicationServer& other) = delete;
    ReplicationServer& operator=(const ReplicationServer& other) = delete;
    ReplicationServer(ReplicationServer&& other) noexcept = delete;
    ReplicationServer& operator=(ReplicationServer&& other) noexcept = delete;

    const ReplicationServerOptions& options() const noexcept {
        return options_;
    }

    void set_transport(TransportFn transport);
    void set_logger(std::shared_ptr<spdlog::logger> logger);
    void set_log_level(LogLevel level);
    LogLevel log_level() const noexcept {
        return options_.logging.level;
    }
#ifdef ASHIATO_SYNC_ENABLE_TRACING
    void set_tracer(SyncTracer* tracer) noexcept;
    void set_trace_options(TraceOptions options);
    void trace_current_frame_components(ashiato::Registry& registry);
    void flush_trace();
    void close_trace();
#endif

    bool add_client(ClientId client);
    ClientId add_local_client(ashiato::Registry& registry);
    ClientId local_client() const noexcept {
        return local_client_;
    }
    bool is_local_client(ClientId client) const noexcept;
    bool remove_client(ashiato::Registry& registry, ClientId client);
    bool has_client(ClientId client) const;
    std::size_t client_count() const noexcept;
    std::vector<ClientId> client_ids() const;
    ClientEntityNetworkId client_entity_network_id(ClientId client, ashiato::Entity entity) const noexcept;

    void rediscover_all_replicated_entities(ashiato::Registry& registry);
    bool is_replicated(ashiato::Entity entity) const;
    std::size_t replicated_count() const noexcept;

    bool acknowledge_entity(ClientId client, ashiato::Entity entity, SyncFrame frame);
    void receive_packet(PeerId peer, ashiato::BitBuffer packet);
    bool process_packet(PeerId peer, ashiato::BitBuffer packet);
    bool process_packet(ashiato::Registry& registry, PeerId peer, ashiato::BitBuffer packet);
    template <typename T>
    bool set_local_input(ashiato::Registry& registry, const T& input) {
        register_components(registry);
        const ashiato::Entity component = registry.template component<T>();
        const SyncSettings& settings = registry.template get<SyncSettings>();
        if (settings.input_component != component) {
            return false;
        }
        return set_local_input_bytes(registry, component, &input);
    }
    bool set_local_input_bytes(ashiato::Registry& registry, ashiato::Entity component, const void* input);
    ClientInputStats input_stats(ClientId client) const noexcept;
    ClientBandwidthStats bandwidth_stats(ClientId client) const noexcept;
    std::shared_ptr<ReplicationBandwidthBudget> client_bandwidth_budget(ClientId client) const noexcept;
    /// Gives one client its own fixed per-tick byte limit in place of `ReplicationServerOptions::bandwidth_limit_bytes_per_tick`
    /// (0 clears it). Only meaningful with `options.bandwidth.enabled == false`. False for an unknown client.
    bool set_client_bandwidth_limit(ClientId client, std::size_t bytes_per_tick);
    bool set_client_bandwidth_budget(ClientId client, std::shared_ptr<ReplicationBandwidthBudget> budget);
    bool set_client_bandwidth_budget(
        ClientId client,
        std::shared_ptr<ReplicationBandwidthBudget> budget,
        ReplicationBandwidthParticipantOptions share);
    bool set_client_bandwidth_share(ClientId client, ReplicationBandwidthParticipantOptions share);
    ReplicationBandwidthParticipantOptions client_bandwidth_share(ClientId client) const noexcept;
    std::size_t begin_client_bandwidth_tick(ClientId client);
    ReplicationSendResult flush_client_updates(ashiato::Registry& registry, ClientId client);
    ObservabilityStats observability_stats() const noexcept {
        return observability_stats_;
    }
    std::size_t retained_quantized_frame_count() const noexcept;
    std::size_t retained_quantized_frame_bytes() const noexcept;
    ServerRegistryDirtyFrameSubscription subscribe_registry_dirty_frame_listener(
        ServerRegistryDirtyFrameListener& listener);
    ServerFrameBatchListenerSubscription subscribe_frame_batch_listener(ServerFrameBatchListener& listener);

    bool tick(ashiato::Registry& registry, double dt_seconds);
    bool advance_frame_without_simulating(ashiato::Registry& registry);
    bool advance_frame_without_simulating(ashiato::Registry& registry, SyncFrame frame);
    // Moves the server clock without sending updates; intended for restoring synthetic timelines such as replay playback.
    bool set_frame_without_broadcast(ashiato::Registry& registry, SyncFrame frame);
    // Current server simulation frame; remains 0 until the first tick begins.
    SyncFrame frame() const noexcept {
        return frame_;
    }
    double accumulator_seconds() const noexcept {
        return tick_accumulator_seconds_;
    }
    double continuous_frame() const noexcept {
        return static_cast<double>(frame_) + tick_accumulator_seconds_ / options_.fixed_dt_seconds;
    }

    // Implementation-facing API for server-side frame consumers. These functions are public so
    // internal consumers do not depend on ReplicationServer's private storage layout.
    std::size_t replicated_slot_count() const noexcept;
    std::size_t active_replicated_slot_count() const noexcept;
    bool replicated_slot_active(std::uint32_t slot) const noexcept;
    ashiato::Entity replicated_slot_entity(std::uint32_t slot) const noexcept;
    SyncArchetypeId replicated_slot_archetype(std::uint32_t slot) const noexcept;
    bool replicated_slot_is_replicable(const ashiato::Registry& registry, std::uint32_t slot) const;
    void deactivate_replicated_slot(std::uint32_t slot);
    std::uint32_t replicated_slot_for_entity(ashiato::Entity entity) const noexcept;
    std::uint32_t replicated_slot_for_entity_index(std::uint32_t entity_index) const noexcept;
    std::uint32_t quantized_frame_for_client(
        const ashiato::Registry& registry,
        const SyncSettings& settings,
        const server_detail::ServerClientReplicator& client,
        std::uint32_t slot,
        SyncFrame frame,
        QuantizedFrameData& scratch,
        std::vector<std::uint64_t>& scratch_dirty_generations);
    bool quantized_frame_active(std::uint32_t quantized_frame) const noexcept;
    SyncFrame quantized_frame_frame(std::uint32_t quantized_frame) const noexcept;
    SyncArchetypeId quantized_frame_archetype(std::uint32_t quantized_frame) const noexcept;
    const QuantizedFrameData* quantized_frame_data(std::uint32_t quantized_frame) const noexcept;
    void retain_server_quantized_frame(std::uint32_t quantized_frame);
    void release_server_quantized_frame(std::uint32_t quantized_frame);
    void clear_server_client_entity_state(server_detail::ClientEntityState& state);
    void acknowledge_server_cues(server_detail::ClientEntityState& state, SyncFrame frame);
    void send_server_update_packet(
        server_detail::ServerClientReplicator& client,
        SyncFrame frame,
        std::uint16_t entity_count,
        const ashiato::BitBuffer& records,
        const std::vector<server_detail::PacketAckRecord>& ack_records);
    bool prepare_client_update_send(server_detail::ServerClientReplicator& client);
    std::size_t begin_client_bandwidth_tick(server_detail::ServerClientReplicator& client);
    ReplicationSendResult flush_client_updates(ashiato::Registry& registry, server_detail::ServerClientReplicator& client);
    std::size_t charged_packet_bytes(std::size_t payload_bytes) const noexcept;
    void log_entity_update_exceeds_mtu(
        PeerId peer,
        ClientId client,
        ashiato::Entity entity,
        SyncArchetypeId archetype,
        std::size_t packet_bytes,
        std::size_t mtu_bytes,
        std::size_t record_bits);
#ifdef ASHIATO_SYNC_ENABLE_TRACING
    SyncTracer* server_tracer() const noexcept;
    void trace_entity_started_syncing(
        ClientId client,
        std::uint32_t slot,
        std::uint32_t network_id,
        std::uint32_t network_version);
    void trace_entity_destroyed(
        ClientId client,
        ashiato::Entity entity,
        std::uint32_t network_id,
        std::uint32_t network_version);
#ifdef ASHIATO_SYNC_TRACE_PACKET_LOGS
    void append_server_packet_ack_cues(
        const SyncSettings& settings,
        const server_detail::ClientEntityState& state,
        server_detail::PacketAckRecord& record) const;
#endif
#endif

private:
    friend class FractionalTickSampler;

    static constexpr std::uint32_t invalid_quantized_frame_id = std::numeric_limits<std::uint32_t>::max();

    struct ReplicatedSlot;
    struct QuantizedFrame;
    using ClientDirtyQueue = server_detail::ClientDirtyQueue;
    using ClientEntityState = server_detail::ClientEntityState;
    using PacketAckRecord = server_detail::PacketAckRecord;
    using ServerClientReplicator = server_detail::ServerClientReplicator;
    using SerializedEntity = server_detail::SerializedEntity;

    struct ClientState;
    struct PendingInboundPacket;

    using EntityKey = std::uint64_t;
    using ReplicatedSlotIndex = std::uint32_t;

    bool valid_archetype(const ashiato::Registry& registry, SyncArchetypeId archetype) const;
    bool upsert_replicated(ashiato::Registry& registry, ashiato::Entity entity, SyncArchetypeId archetype);
    ReplicatedSlotIndex allocate_replicated_slot(ashiato::Entity entity, SyncArchetypeId archetype);
    void deactivate_replicated(ReplicatedSlotIndex replicated_index);
    void deactivate_entity_index(std::uint32_t entity_index);
    void remove_replicated_from_client_replicators(ReplicatedSlotIndex replicated_index);
    bool replicated_is_replicable(const ashiato::Registry& registry, ReplicatedSlotIndex replicated_index) const;
    void push_dirty_info_to_listeners(ashiato::Registry& registry);
    void push_frame_to_listeners(ashiato::Registry& registry, double dt_seconds, std::uint32_t completed_frames);
    void on_registry_dirty_frame(const ashiato::RegistryDirtyFrame& frame) override;
    void broadcast_registry_dirty_frame(
        const ashiato::RegistryDirtyFrame& frame,
        QueuedSyncCueView cues);
    void remove_unsubscribed_registry_dirty_frame_listeners();
    void broadcast_frame_batch(ashiato::Registry& registry, double dt_seconds, std::uint32_t completed_frames);
    void remove_unsubscribed_frame_batch_listeners();
    void rediscover_replicated_entities(ashiato::Registry& registry, ashiato::Registry::DirtyView dirty);
    void capture_dirty_generations(ashiato::Registry::DirtyView dirty, const SyncSettings& settings);
    void capture_queued_cues(
        ashiato::Registry& registry,
        const SyncSettings& settings,
        QueuedSyncCueView cues);
    bool play_local_cue(ashiato::Registry& registry, const SyncSettings& settings, const QueuedSyncCue& cue);
    void attach_cue_to_clients(const ashiato::Registry& registry, const SyncSettings& settings, std::uint32_t slot, const QueuedSyncCue& cue);
    void mark_dirty_component(const SyncSettings& settings, std::uint32_t slot, ashiato::Entity component);
    void mark_dirty_tag(const SyncSettings& settings, std::uint32_t slot, ashiato::Entity tag);
    void mark_dirty_tags(const SyncSettings& settings, std::uint32_t slot);
    void mark_owner_visibility_dirty(const SyncSettings& settings, std::uint32_t slot);
    static bool archetype_is_same_frame_cacheable(const SyncArchetype& archetype);
    void advance_client_idle_timers(double dt_seconds);
    void resend_pending_connect_responses(double dt_seconds);
    void disconnect_timed_out_clients(ashiato::Registry& registry);
    void set_local_client_id(ashiato::Registry& registry, ClientId client);
    std::uint32_t find_or_create_quantized_frame(
        const ashiato::Registry& registry,
        const SyncSettings& settings,
        const ServerClientReplicator& client,
        std::uint32_t slot,
        SyncFrame frame,
        QuantizedFrameData& scratch,
        std::vector<std::uint64_t>& scratch_dirty_generations);
    void retain_quantized_frame(std::uint32_t quantized_frame);
    void release_quantized_frame(std::uint32_t quantized_frame);
    void clear_client_entity_state(ClientEntityState& state);
    struct ClientUpdateAckResult {
        bool packet_valid = false;
        bool all_acknowledged = false;
    };
    bool process_connect_request_packet(PeerId peer, ashiato::BitBuffer& packet);
    bool process_message_from_connected_client(
        ashiato::Registry& registry,
        ClientState& client,
        std::uint8_t message,
        ashiato::BitBuffer& packet);
    bool process_connection_request_ack_packet(ClientState& client, ashiato::BitBuffer& packet);
    bool process_ping_packet(ClientState& client, ashiato::BitBuffer& packet);
    bool process_client_ack_packet(ServerClientReplicator& replication, ashiato::BitBuffer& packet);
    ClientUpdateAckResult process_client_acks_from_packet(
        ServerClientReplicator& replication,
        ashiato::BitBuffer& packet
#if defined(ASHIATO_SYNC_ENABLE_TRACING) && defined(ASHIATO_SYNC_TRACE_PACKET_LOGS)
        ,
        std::vector<std::uint32_t>& trace_acks
#endif
    );
    bool process_input_with_acks_packet(ashiato::Registry& registry, ClientState& client, ashiato::BitBuffer& packet);
    void detach_client_bandwidth_participant(ServerClientReplicator& replication);
    void log_info(const char* event, const std::string& fields) const;
    void log_client_packet_warning(PeerId peer, std::uint8_t message, const char* reason_code, const char* reason_detail);
    void log_server_error(PeerId peer, const char* event, const char* reason);
    void push_client_inputs_to_ashiato(ashiato::Registry& registry);
    void acknowledge_cues(ClientEntityState& state, SyncFrame frame);
    bool same_quantized_frame_components(
        const QuantizedFrame& quantized_frame,
        const QuantizedFrameData& data,
        const std::vector<std::uint64_t>& dirty_generations) const;
    void send_packet(
        ServerClientReplicator& client,
        SyncFrame frame,
        std::uint16_t entity_count,
        const ashiato::BitBuffer& records,
        const std::vector<PacketAckRecord>& ack_records);
    bool add_client_for_peer(PeerId peer, ClientId client, bool ready_for_updates);
    bool add_client_state(ClientState state);
    ClientId find_next_available_client_id() const;
    void notify_connection_event(const ReplicationServerConnectionEvent& event);
    void create_client_replicator(ClientState& client);
    void send_connect_response(ClientState& client);
    void send_pong(
        ClientState& client,
        std::uint32_t sequence,
        SyncFrame server_receive_frame,
        std::uint16_t server_receive_subframe);
#ifdef ASHIATO_SYNC_ENABLE_TRACING
    void trace_frame_components(const ashiato::Registry& registry, const SyncSettings& settings);
    void trace_input_component(
        ashiato::Entity entity,
        ClientState& client,
        SyncFrame frame,
        ashiato::Entity component,
        const SyncComponentOps& ops,
        const std::uint8_t* quantized);
    void trace_input_starved(
        ashiato::Entity entity,
        ClientState& client,
        SyncFrame due_frame,
        SyncFrame input_frame,
        ashiato::Entity component,
        const SyncComponentOps& ops);
#ifdef ASHIATO_SYNC_TRACE_PACKET_LOGS
    void trace_incoming_ack_packet(ServerClientReplicator& client, const std::vector<std::uint32_t>& acks) const;
    void trace_incoming_ping_packet(ClientState& client, std::uint32_t sequence) const;
    void trace_outgoing_pong_packet(
        ClientState& client,
        std::uint32_t sequence,
        SyncFrame server_receive_frame,
        SyncFrame server_send_frame) const;
    void trace_incoming_input_packet(
        ClientState& client,
        const std::vector<std::uint32_t>& acks,
        SyncFrame baseline_frame,
        SyncFrame first_input_frame,
        SyncFrame last_input_frame) const;
    void trace_outgoing_update_packet(
        ServerClientReplicator& client,
        SyncFrame frame,
        std::uint32_t packet_id,
        SyncFrame input_ack_frame,
        const std::vector<PacketAckRecord>& records) const;
    void append_packet_ack_cues(
        const SyncSettings& settings,
        const ClientEntityState& state,
        PacketAckRecord& record) const;
#endif
#endif

    ReplicationServerOptions options_;
    std::vector<ReplicatedSlot> replicated_;
    std::vector<std::uint32_t> free_replicated_indices_;
    std::vector<QuantizedFrame> quantized_frames_;
    std::vector<std::uint32_t> free_quantized_frames_;
    std::unordered_map<EntityKey, std::uint32_t> entity_to_replicated_index_;
    std::unordered_map<std::uint32_t, std::uint32_t> entity_index_to_replicated_index_;
    std::vector<ClientState> clients_;
    std::unordered_map<ClientId, std::size_t> client_to_index_;
    std::unordered_map<PeerId, std::size_t> peer_to_index_;
    ashiato::RegistryDirtyFrameBroadcaster registry_dirty_frame_broadcaster_;
    ashiato::RegistryDirtyFrameBroadcastSubscription server_dirty_frame_subscription_;
    std::shared_ptr<ServerRegistryDirtyFrameSubscription::State> registry_dirty_frame_listeners_;
    std::shared_ptr<ServerFrameBatchListenerSubscription::State> frame_batch_listeners_;
    std::vector<PendingInboundPacket> inbound_packets_;
    std::vector<QueuedSyncCue> frame_cues_;
    std::vector<ServerDestroyedReplicatedSlot> post_tick_destroyed_slots_;
    std::shared_ptr<spdlog::logger> logger_;
    ObservabilityStats observability_stats_;
    std::unordered_map<PeerId, std::uint32_t> warning_logs_by_peer_;
    mutable bool processing_client_packet_ = false;
    mutable bool server_error_logged_ = false;
    std::size_t active_replicated_count_ = 0;
    ClientId next_connect_client_id_ = 1;
    ClientId local_client_ = invalid_client_id;
    SyncFrame frame_ = 0;
    double tick_accumulator_seconds_ = 0.0;
    bool replicated_initialized_ = false;
#ifdef ASHIATO_SYNC_ENABLE_TRACING
    SyncTracer* tracer_ = nullptr;
    std::unique_ptr<KTraceDirectoryWriter> trace_writer_;
#endif
};

template <std::size_t NetworkEntityIdTier0Bits = protocol::default_network_entity_id_tier0_bits>
class ReplicationServerT : public ReplicationServer {
    static_assert(
        protocol::valid_network_entity_id_tier0_bits(NetworkEntityIdTier0Bits),
        "NetworkEntityIdTier0Bits must be in [1, 22]");

public:
    static constexpr std::size_t network_entity_id_tier0_bits = NetworkEntityIdTier0Bits;

    explicit ReplicationServerT(ashiato::Registry& registry, ReplicationServerOptions options = {})
        : ReplicationServer(registry, configure(std::move(options))) {}

private:
    static ReplicationServerOptions configure(ReplicationServerOptions options) {
        options.protocol.network_entity_id_tier0_bits = NetworkEntityIdTier0Bits;
        return options;
    }
};

}  // namespace ashiato::sync
