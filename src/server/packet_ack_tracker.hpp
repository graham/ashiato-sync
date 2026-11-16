#pragma once

#include "server/packet.hpp"

#include <algorithm>
#include <cstddef>
#include <cstdint>
#include <vector>

namespace ashiato::sync::server_detail {

// The next id, in allocation order, that no pending (unacknowledged) packet holds; 0 when the pending list is full. An ACK carries
// only the id, so handing out an id whose last packet may still be acknowledged would credit that late ACK to the new packet: the
// server would then delta against a frame the client never applied, and the client rejects every later update. Id 0 is never
// allocated, so a packet sent with it is never credited. Ids run 1..max_packet_id and max_packet_id >= max_pending, so while
// fewer than max_pending packets are pending, one of the next pending.size() + 1 ids is free.
template <typename PendingPacketAck>
std::uint32_t allocate_free_packet_id(
    std::uint32_t& next_packet_id,
    const std::vector<PendingPacketAck>& pending_packet_acks,
    std::size_t max_pending_packet_acks_per_client) {
    if (pending_packet_acks.size() >= max_pending_packet_acks_per_client) {
        return 0U;
    }
    const ServerPacketIdAllocator allocator(max_pending_packet_acks_per_client);
    for (std::size_t attempt = 0; attempt <= pending_packet_acks.size(); ++attempt) {
        const std::uint32_t candidate = allocator.allocate(next_packet_id);
        const bool held = std::any_of(
            pending_packet_acks.begin(),
            pending_packet_acks.end(),
            [candidate](const PendingPacketAck& pending) {
                return pending.packet_id == candidate;
            });
        if (!held) {
            return candidate;
        }
    }
    return 0U;
}

template <typename PendingPacketAck>
void enforce_pending_packet_ack_limit(
    std::vector<PendingPacketAck>& pending_packet_acks,
    std::size_t max_pending_packet_acks_per_client) {
    if (pending_packet_acks.size() > max_pending_packet_acks_per_client) {
        pending_packet_acks.erase(
            pending_packet_acks.begin(),
            pending_packet_acks.begin() +
                static_cast<typename std::vector<PendingPacketAck>::difference_type>(
                    pending_packet_acks.size() - max_pending_packet_acks_per_client));
    }
}

template <typename PendingPacketAck, typename PacketAckRecord>
void track_packet_ack(
    std::vector<PendingPacketAck>& pending_packet_acks,
    std::uint32_t packet_id,
    ashiato::sync::SyncFrame sent_frame,
    std::size_t charged_bytes,
    const std::vector<PacketAckRecord>& records) {
    if (records.empty() || packet_id == 0U) {
        return;
    }
    pending_packet_acks.push_back(PendingPacketAck{packet_id, sent_frame, charged_bytes, records});
}

}  // namespace ashiato::sync::server_detail
