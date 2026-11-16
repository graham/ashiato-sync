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

bool server_detail::ServerClientReplicator::AckTracker::packet_unacknowledgeable(
    const ReplicationServer& server,
    const ServerClientReplicator& client,
    const PendingPacketAck& pending) const {
    // The client applies a packet whole or not at all, and refuses it whole when any entity's frame is not newer than the frame it
    // already holds for that entity. So once a newer-or-equal frame of any entity in this packet has been acknowledged, this packet
    // can never be acknowledged. Destroy records prove nothing and are left to the lifetime.
    return std::any_of(
        pending.records.begin(),
        pending.records.end(),
        [&](const PacketAckRecord& record) {
            if (record.destroy) {
                return false;
            }
            const std::uint32_t slot = server.replicated_slot_for_entity(record.entity);
            if (slot == server_detail::invalid_quantized_frame_id) {
                return false;
            }
            const ClientEntityState* state = client.entities.try_get(slot);
            return state != nullptr && state->has_acknowledged_frame && state->acknowledged_frame >= record.frame;
        });
}

void server_detail::ServerClientReplicator::AckTracker::release_dead_packet_acks(
    ReplicationServer& server,
    ServerClientReplicator& client,
    bool proven_only) {
    const ReplicationServerOptions& options = server.options();
    const SyncFrame lifetime = options.packet_ack_lifetime_frames;
    const SyncFrame now = server.frame();
    pending_packet_acks.erase(
        std::remove_if(
            pending_packet_acks.begin(),
            pending_packet_acks.end(),
            [&](const PendingPacketAck& pending) {
                const bool expired = !proven_only && lifetime != 0U && now >= pending.sent_frame &&
                    now - pending.sent_frame >= lifetime;
                const bool dead = expired || packet_unacknowledgeable(server, client, pending);
                if (dead && options.bandwidth.enabled && client.bandwidth != nullptr) {
                    client.bandwidth->packet_lost(options.bandwidth, now, pending.charged_bytes);
                }
                return dead;
            }),
        pending_packet_acks.end());
}

void server_detail::ServerClientReplicator::AckTracker::cleanup_packet_acks(
    ReplicationServer& server,
    ServerClientReplicator& client) {
    // Only proof frees an id here. A packet whose records simply left their entity's pending list (overflow, an entity that left)
    // can still be acknowledged, so it keeps its id until it is acknowledged, proven dead or past packet_ack_lifetime_frames.
    release_dead_packet_acks(server, client, true);
}

std::uint32_t server_detail::ServerClientReplicator::AckTracker::allocate_packet_id(
    ReplicationServer& server,
    ServerClientReplicator& client) {
    const std::size_t max_pending = server.options().protocol.max_pending_packet_acks_per_client;
    std::uint32_t packet_id = server_detail::allocate_free_packet_id(next_packet_id, pending_packet_acks, max_pending);
    if (packet_id == 0U) {
        // Every id is held (a client this far behind is starved or cut off): free what has expired, then try once more.
        release_dead_packet_acks(server, client, false);
        packet_id = server_detail::allocate_free_packet_id(next_packet_id, pending_packet_acks, max_pending);
    }
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
