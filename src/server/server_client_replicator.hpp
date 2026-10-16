#pragma once

#include "ashiato/sync/components.hpp"
#include "ashiato/sync/server_frame_consumer.hpp"
#ifdef ASHIATO_SYNC_ENABLE_TRACING
#include "ashiato/sync/tracing.hpp"
#endif
#include "server/bandwidth_controller.hpp"

#include <cstddef>
#include <cstdint>
#include <limits>
#include <memory>
#include <string>
#include <vector>

namespace ashiato::sync {

class ReplicationServer;

namespace server_detail {

constexpr std::uint32_t invalid_quantized_frame_id = std::numeric_limits<std::uint32_t>::max();

struct ClientEntityState {
    struct PendingQuantizedFrame {
        std::uint32_t quantized_frame = invalid_quantized_frame_id;
        SyncFrame frame = 0;
        std::uint32_t baseline_epoch_when_sent = 0;
    };

    struct PendingCue {
        SyncFrame frame = 0;
        SyncFrame expire_frame = 0;
        SyncCueTypeId type = 0;
        float relevance_seconds = 0.0f;
        ashiato::BitBuffer payload;
#if defined(ASHIATO_SYNC_ENABLE_TRACING) && defined(ASHIATO_SYNC_TRACE_COMPONENT_DATA)
        std::vector<ashiato::SerializationTraceScope> payload_trace_scopes;
#endif
    };

    std::uint32_t baseline = invalid_quantized_frame_id;
    std::uint32_t baseline_epoch = 0;  // Blocks stale ACKs from restoring invalidated baselines.
    std::uint32_t network_id = 0;
    std::uint32_t network_version = 0;
    float last_priority = std::numeric_limits<float>::quiet_NaN();
    std::uint64_t component_mask = std::numeric_limits<std::uint64_t>::max();
    bool reference_priority_boost_pending = false;
    bool has_network_id = false;
    std::vector<PendingQuantizedFrame> pending;
    std::vector<PendingCue> pending_cues;
};

struct ClientDirtyQueue {
    struct Entry {
        SyncFrame dirty_frame = 0;
        SyncFrame baseline_frame = 0;
        float priority_accumulator = 0.0f;
        float last_priority = std::numeric_limits<float>::quiet_NaN();
        std::uint64_t component_mask = std::numeric_limits<std::uint64_t>::max();
        bool queued = false;
        bool listed = false;
    };

    std::vector<Entry> entries;
    std::vector<std::uint32_t> dirty_replicated_indices;
};

struct ClientDestroyState {
    ashiato::Entity entity;
    SyncFrame frame = 0;
    std::uint64_t reset_epoch = 0;
    std::uint32_t network_id = 0;
    std::uint32_t network_version = 0;
};

struct PacketAckRecord {
    struct CueSummary {
        SyncFrame frame = 0;
        SyncCueTypeId type = 0;
        std::string data;
    };

    ashiato::Entity entity;
    SyncFrame frame = 0;
    bool destroy = false;
#if defined(ASHIATO_SYNC_ENABLE_TRACING) && defined(ASHIATO_SYNC_TRACE_PACKET_LOGS)
    std::vector<CueSummary> cues{};
#endif
};

struct PendingPacketAck {
    std::uint32_t packet_id = 0;
    SyncFrame sent_frame = 0;
    std::size_t charged_bytes = 0;
    std::vector<PacketAckRecord> records;
};

struct SerializedEntity {
    std::uint32_t quantized_frame = invalid_quantized_frame_id;
    ashiato::BitBuffer payload;
    bool full_state = false;
#ifdef ASHIATO_SYNC_ENABLE_TRACING
    std::vector<SyncTraceEvent> deferred_trace_events;
#endif
};

struct SerializedCandidate {
    enum class Kind {
        Update,
        Destroy
    };

    Kind kind = Kind::Update;
    std::uint32_t slot = 0;
    std::size_t destroy_index = 0;
    float priority = 0.0f;
    std::uint64_t component_mask = std::numeric_limits<std::uint64_t>::max();
};

struct ServerClientReplicator final : ServerRegistryDirtyFrameListener, ServerFrameBatchListener {
    struct UpdateScheduler;
    struct UpdateWriter;
    struct EntityStates {
        std::vector<ClientEntityState> states;

        void ensure_capacity(std::size_t size);
        std::size_t size() const noexcept;
        ClientEntityState& at(std::uint32_t slot);
        const ClientEntityState& at(std::uint32_t slot) const;
        ClientEntityState* try_get(std::uint32_t slot) noexcept;
        const ClientEntityState* try_get(std::uint32_t slot) const noexcept;
        void clear(ReplicationServer& replication_server, std::uint32_t slot);
        void clear_all(ReplicationServer& replication_server);
        void clear_preserving_network_identity(ReplicationServer& replication_server, std::uint32_t slot);
        void expire_pending_cues(SyncFrame frame);
    };

    struct DestroyQueue {
        std::vector<ClientDestroyState> pending;

        bool empty() const noexcept;
        std::size_t size() const noexcept;
        ClientDestroyState& at(std::size_t index) noexcept;
        const ClientDestroyState& at(std::size_t index) const noexcept;
        void enqueue(ashiato::Entity entity, SyncFrame frame, std::uint32_t network_id, std::uint32_t network_version);
        bool acknowledge(ServerClientReplicator& client, ashiato::Entity entity, SyncFrame frame);
        bool contains_ack_record(ashiato::Entity entity, SyncFrame frame) const;
    };

    struct NetworkIds {
        struct Entry {
            std::uint32_t replicated_index_or_next_free_network_id = 0;
            std::uint32_t version = 0;
            bool active = false;
            bool pending_destroy = false;
        };

        std::vector<Entry> entries;
        std::uint32_t free_network_id = 0;

        std::uint32_t network_id_for(ReplicationServer& replication_server, ServerClientReplicator& client, std::uint32_t slot);
        std::uint32_t allocate_for(ReplicationServer& replication_server, ServerClientReplicator& client, std::uint32_t slot);
        void free(std::uint32_t network_id);
        bool mark_pending_destroy(std::uint32_t network_id);
        Entry* try_get(std::uint32_t network_id) noexcept;
        const Entry* try_get(std::uint32_t network_id) const noexcept;
    };

    struct AckTracker {
        std::uint32_t next_packet_id = 1;
        std::vector<PendingPacketAck> pending_packet_acks;

        bool acknowledge_packet(ReplicationServer& replication_server, ServerClientReplicator& client, std::uint32_t packet_id);
        void cleanup_packet_acks(ReplicationServer& replication_server, ServerClientReplicator& client);
        std::uint32_t allocate_packet_id(ReplicationServer& replication_server, ServerClientReplicator& client);
        void enforce_pending_packet_ack_limit(ReplicationServer& replication_server, ServerClientReplicator& client);
        void track_packet_ack(
            ServerClientReplicator& client,
            std::uint32_t packet_id,
            SyncFrame sent_frame,
            std::size_t charged_bytes,
            const std::vector<PacketAckRecord>& records);

    private:
        bool client_acknowledged_destroy(
            ReplicationServer& replication_server,
            ServerClientReplicator& client,
            ashiato::Entity entity,
            SyncFrame frame);
        bool packet_ack_record_pending(
            const ReplicationServer& replication_server,
            const ServerClientReplicator& client,
            const PacketAckRecord& record) const;
    };

    ClientId id = invalid_client_id;
    PeerId peer = invalid_peer_id;
    std::uint64_t epoch = 0;
    SyncFrame input_ack_frame = 0;
    ClientDirtyQueue dirty_queue;
    EntityStates entities;
    DestroyQueue destroys;
    NetworkIds network_ids;
    std::shared_ptr<ReplicationBandwidthBudget> bandwidth;
    /// This client's own fixed per-tick byte limit; 0 means the server's `bandwidth_limit_bytes_per_tick`.
    std::size_t bandwidth_limit_bytes_per_tick = 0;
    ReplicationBandwidthParticipantId bandwidth_participant = invalid_bandwidth_participant_id;
    AckTracker ack_tracker;
    std::unique_ptr<UpdateScheduler> update_scheduler;
    ReplicationServer* owner_server = nullptr;
    ServerRegistryDirtyFrameSubscription registry_dirty_frame_subscription;
    ServerFrameBatchListenerSubscription frame_batch_subscription;

    ServerClientReplicator();
    ~ServerClientReplicator() override;
    ServerClientReplicator(const ServerClientReplicator& other) = delete;
    ServerClientReplicator& operator=(const ServerClientReplicator& other) = delete;
    ServerClientReplicator(ServerClientReplicator&& other) = delete;
    ServerClientReplicator& operator=(ServerClientReplicator&& other) = delete;

    void on_server_registry_dirty_frame(const ServerRegistryDirtyFrame& frame) override;
    void on_server_frame_batch_complete(const ServerFrameBatch& batch) override;

    void ensure_capacity(std::size_t replicated_count);
    void initialize_marking_all_dirty(ReplicationServer& replication_server, SyncFrame frame);
    void mark_dirty(const ReplicationServer& replication_server, std::uint32_t replicated_index, SyncFrame frame);
    void clear_dirty(std::uint32_t replicated_index);
    void expire_pending_cues(SyncFrame frame);
    std::uint32_t network_id_for(ReplicationServer& replication_server, std::uint32_t replicated_index);
    void free_network_id(std::uint32_t network_id);
    bool enqueue_destroy(
        ReplicationServer& replication_server,
        std::uint32_t replicated_index,
        ashiato::Entity entity,
        SyncFrame frame);
    bool acknowledge_entity(ReplicationServer& replication_server, std::uint32_t replicated_index, SyncFrame frame);
    void invalidate_entity_baseline(ReplicationServer& replication_server, ClientEntityState& entity_state);
};

struct ServerClientReplicator::UpdateWriter {
    bool serialize_entity(
        ReplicationServer& replication_server,
        const ashiato::Registry& registry,
        const SyncSettings& settings,
        ServerClientReplicator& client,
        std::uint32_t slot,
        SyncFrame frame,
        std::uint64_t component_mask,
        SerializedEntity& out);

private:
    void write_entity_record(
        ReplicationServer& replication_server,
        const ashiato::Registry& registry,
        const SyncSettings& settings,
        ServerClientReplicator& client,
        std::uint32_t slot,
        std::uint32_t quantized_frame,
        std::uint64_t component_mask,
        ashiato::BitBuffer& out,
        bool& full_state
#ifdef ASHIATO_SYNC_ENABLE_TRACING
        ,
        std::vector<SyncTraceEvent>& deferred_trace_events
#endif
    );

    QuantizedFrameData quantized_frame_scratch_;
    std::vector<std::uint64_t> quantized_frame_dirty_scratch_;
};

struct ServerClientReplicator::UpdateScheduler {
    ReplicationServer::ReplicationSendResult send_client(
        ReplicationServer& replication_server,
        ashiato::Registry& registry,
        const SyncSettings& settings,
        ServerClientReplicator& replication,
        std::uint32_t completed_frames);

private:
    void cleanup_dirty_queue(ServerClientReplicator& replication);
    void refresh_replication_decision_if_due(
        ReplicationServer& replication_server,
        ServerClientReplicator& replication,
        std::uint32_t slot,
        ClientDirtyQueue::Entry& entry);

    std::vector<SerializedCandidate> candidates_;
    std::vector<SerializedCandidate> update_candidates_;
    std::vector<std::size_t> destroy_order_;
    ashiato::BitBuffer records_;
    std::vector<PacketAckRecord> packet_ack_records_;
    SerializedEntity serialized_;
    UpdateWriter writer_;
};

}  // namespace server_detail
}  // namespace ashiato::sync
