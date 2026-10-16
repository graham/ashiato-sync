#include "ashiato/sync/server.hpp"

#include "ashiato/sync/bandwidth_budget.hpp"
#include "server/packet.hpp"
#include "server/packet_ack_tracker.hpp"
#include "server/state.hpp"

#include <algorithm>
#include <vector>

namespace ashiato::sync {

bool server_detail::ServerClientReplicator::AckTracker::client_acknowledged_destroy(
    ReplicationServer& server,
    ServerClientReplicator& client,
    ashiato::Entity entity,
    SyncFrame frame) {
    (void)server;
    return client.destroys.acknowledge(client, entity, frame);
}

bool server_detail::ServerClientReplicator::AckTracker::acknowledge_packet(
    ReplicationServer& server,
    ServerClientReplicator& client,
    std::uint32_t packet_id) {
    const auto found = std::find_if(
        pending_packet_acks.begin(),
        pending_packet_acks.end(),
        [packet_id](const PendingPacketAck& pending) {
            return pending.packet_id == packet_id;
        });
    if (found == pending_packet_acks.end()) {
        return false;
    }

    bool all_valid = true;
    for (const PacketAckRecord& record : found->records) {
        const bool accepted = record.destroy
            ? client_acknowledged_destroy(server, client, record.entity, record.frame)
            : server.acknowledge_entity(client.id, record.entity, record.frame);
        all_valid = accepted && all_valid;
    }
    if (server.options().bandwidth.enabled && client.bandwidth != nullptr) {
        client.bandwidth->packet_acked(server.options().bandwidth, found->sent_frame, server.frame(), found->charged_bytes);
    }
    pending_packet_acks.erase(found);
    return all_valid;
}

bool server_detail::ServerClientReplicator::AckTracker::packet_ack_record_pending(
    const ReplicationServer& server,
    const ServerClientReplicator& client,
    const PacketAckRecord& record) const {
    if (record.destroy) {
        return client.destroys.contains_ack_record(record.entity, record.frame);
    }

    const std::uint32_t slot = server.replicated_slot_for_entity(record.entity);
    if (slot == server_detail::invalid_quantized_frame_id) {
        return false;
    }
    const ClientEntityState* state = client.entities.try_get(slot);
    if (state == nullptr) {
        return false;
    }
    return std::any_of(
        state->pending.begin(),
        state->pending.end(),
        [&](const ClientEntityState::PendingQuantizedFrame& pending) {
            return pending.frame == record.frame;
        });
}

void server_detail::ServerClientReplicator::AckTracker::cleanup_packet_acks(
    ReplicationServer& server,
    ServerClientReplicator& client) {
    pending_packet_acks.erase(
        std::remove_if(
            pending_packet_acks.begin(),
            pending_packet_acks.end(),
            [&](const PendingPacketAck& pending_packet) {
                const bool stale = std::none_of(
                    pending_packet.records.begin(),
                    pending_packet.records.end(),
                    [&](const PacketAckRecord& record) {
                        return packet_ack_record_pending(server, client, record);
                    });
                if (stale && server.options().bandwidth.enabled && client.bandwidth != nullptr) {
                    client.bandwidth->packet_lost(server.options().bandwidth, server.frame(), pending_packet.charged_bytes);
                }
                return stale;
            }),
        pending_packet_acks.end());
}

std::uint32_t server_detail::ServerClientReplicator::AckTracker::allocate_packet_id(
    ReplicationServer& server,
    ServerClientReplicator& client) {
    const ReplicationServerOptions& options = server.options();
    const server_detail::ServerPacketIdAllocator allocator(options.protocol.max_pending_packet_acks_per_client);
    const std::uint32_t packet_id = allocator.allocate(next_packet_id);
    pending_packet_acks.erase(
        std::remove_if(
            pending_packet_acks.begin(),
            pending_packet_acks.end(),
            [&](const PendingPacketAck& pending) {
                if (pending.packet_id != packet_id) {
                    return false;
                }
                if (options.bandwidth.enabled && client.bandwidth != nullptr) {
                    client.bandwidth->packet_lost(options.bandwidth, server.frame(), pending.charged_bytes);
                }
                return true;
            }),
        pending_packet_acks.end());
    return packet_id;
}

void server_detail::ServerClientReplicator::AckTracker::enforce_pending_packet_ack_limit(
    ReplicationServer& server,
    ServerClientReplicator& client) {
    const ReplicationServerOptions& options = server.options();
    const std::size_t max_pending = options.protocol.max_pending_packet_acks_per_client;
    if (pending_packet_acks.size() <= max_pending) {
        return;
    }
    const std::size_t drop_count = pending_packet_acks.size() - max_pending;
    if (options.bandwidth.enabled && client.bandwidth != nullptr) {
        for (std::size_t index = 0; index < drop_count; ++index) {
            client.bandwidth->packet_lost(
                options.bandwidth,
                server.frame(),
                pending_packet_acks[index].charged_bytes);
        }
    }
    pending_packet_acks.erase(
        pending_packet_acks.begin(),
        pending_packet_acks.begin() +
            static_cast<std::vector<PendingPacketAck>::difference_type>(drop_count));
}

std::size_t ReplicationServer::begin_client_bandwidth_tick(ServerClientReplicator& client) {
    if (client.bandwidth == nullptr) {
        client.bandwidth = std::make_shared<ReplicationBandwidthBudget>(options_.bandwidth);
    }
    if (!options_.bandwidth.enabled) {
        return client.bandwidth->begin_fixed_tick_once(
            client.bandwidth_limit_bytes_per_tick != 0 ? client.bandwidth_limit_bytes_per_tick
                                                       : options_.bandwidth_limit_bytes_per_tick);
    }
    return client.bandwidth->begin_tick_once(options_.bandwidth, options_.fixed_dt_seconds);
}

std::size_t ReplicationServer::charged_packet_bytes(std::size_t payload_bytes) const noexcept {
    return payload_bytes + (options_.bandwidth.enabled ? options_.bandwidth.transport_overhead_bytes_per_packet : 0U);
}

void server_detail::ServerClientReplicator::AckTracker::track_packet_ack(
    ServerClientReplicator& client,
    std::uint32_t packet_id,
    SyncFrame sent_frame,
    std::size_t charged_bytes,
    const std::vector<PacketAckRecord>& records) {
    if (records.empty()) {
        return;
    }
    server_detail::track_packet_ack(pending_packet_acks, packet_id, sent_frame, charged_bytes, records);
    if (charged_bytes != 0U && client.bandwidth != nullptr) {
        client.bandwidth->packet_sent(charged_bytes);
    }
}

}  // namespace ashiato::sync
