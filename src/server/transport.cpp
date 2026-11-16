#include "ashiato/sync/server.hpp"

#include "server/packet.hpp"
#include "server/state.hpp"

#include <algorithm>
#include <cmath>
#include <exception>

namespace ashiato::sync {

void ReplicationServer::send_packet(
    ServerClientReplicator& client,
    SyncFrame frame,
    std::uint16_t entity_count,
    const ashiato::BitBuffer& records,
    const std::vector<PacketAckRecord>& ack_records) {
    if (!options_.transport || entity_count == 0) {
        return;
    }

    const std::uint32_t packet_id = client.ack_tracker.allocate_packet_id(*this, client);
    ashiato::BitBuffer packet =
        server_detail::make_server_packet(
            options_.mtu_bytes,
            server_detail::configured_packet_id_bits(options_),
            frame,
            packet_id,
            client.input_ack_frame,
            entity_count,
            records);
    const std::size_t charged_bytes = charged_packet_bytes(packet.byte_size());
    client.ack_tracker.track_packet_ack(client, packet_id, frame, charged_bytes, ack_records);
    if (packet_id != 0U && options_.baseline_recovery_frames != 0U) {
        // Start each entity's recovery wait at its first tracked send since an ACK; an untracked (id 0) send can never be
        // acknowledged, so it proves nothing about the client.
        for (const server_detail::PacketAckRecord& record : ack_records) {
            if (record.destroy) {
                continue;
            }
            const std::uint32_t slot = replicated_slot_for_entity(record.entity);
            if (slot == server_detail::invalid_quantized_frame_id) {
                continue;
            }
            server_detail::ClientEntityState* state = client.entities.try_get(slot);
            if (state != nullptr && !state->awaiting_acknowledgement) {
                state->awaiting_acknowledgement = true;
                state->unacknowledged_since_frame = frame;
            }
        }
    }
    if (client.bandwidth != nullptr) {
        client.bandwidth->spend(charged_bytes);
    }
    client.ack_tracker.enforce_pending_packet_ack_limit(*this, client);
#if defined(ASHIATO_SYNC_ENABLE_TRACING) && defined(ASHIATO_SYNC_TRACE_PACKET_LOGS)
    trace_outgoing_update_packet(client, frame, packet_id, client.input_ack_frame, ack_records);
#endif
    try {
        options_.transport(client.peer, packet);
    } catch (const std::exception& ex) {
        log_server_error(client.peer, "transport_error_server_update", ex.what());
        throw;
    }
}

void ReplicationServer::send_connect_response(ClientState& client) {
    if (!options_.transport) {
        return;
    }
    ashiato::BitBuffer packet;
    packet.reserve_bytes(options_.mtu_bytes);
    packet.write_bits(protocol::server_connect_response_message, protocol::message_bits);
    packet.write_bool(true);
    packet.write_unsigned_bits(client.id, protocol::client_id_bits);
    try {
        options_.transport(client.peer, packet);
    } catch (const std::exception& ex) {
        log_server_error(client.peer, "transport_error_connect_response", ex.what());
        throw;
    }
    client.connect_resend_accumulator_seconds = 0.0;
}

void ReplicationServer::send_pong(
    ClientState& client,
    std::uint32_t sequence,
    SyncFrame server_receive_frame,
    std::uint16_t server_receive_subframe) {
    if (!options_.transport) {
        return;
    }
    const double subframe = tick_accumulator_seconds_ / options_.fixed_dt_seconds;
    const auto server_subframe = static_cast<std::uint16_t>(std::clamp(
        static_cast<std::uint32_t>(std::floor(subframe * static_cast<double>(protocol::frame_subframe_scale))),
        std::uint32_t{0},
        protocol::frame_subframe_scale - 1U));
    ashiato::BitBuffer packet;
    packet.reserve_bytes(options_.mtu_bytes);
    packet.write_bits(protocol::server_pong_message, protocol::message_bits);
    packet.write_bits(sequence, 32U);
    packet.write_bits(server_receive_frame, 32U);
    packet.write_bits(server_receive_subframe, protocol::frame_subframe_bits);
    packet.write_bits(frame_, 32U);
    packet.write_bits(server_subframe, protocol::frame_subframe_bits);
#if defined(ASHIATO_SYNC_ENABLE_TRACING) && defined(ASHIATO_SYNC_TRACE_PACKET_LOGS)
    trace_outgoing_pong_packet(client, sequence, server_receive_frame, frame_);
#endif
    try {
        options_.transport(client.peer, packet);
    } catch (const std::exception& ex) {
        log_server_error(client.peer, "transport_error_pong", ex.what());
        throw;
    }
}

}  // namespace ashiato::sync
