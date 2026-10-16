#include "ashiato/sync/server.hpp"

#include "server/detail.hpp"
#include "server/packet.hpp"
#include "server/state.hpp"

#include "ashiato/sync/assert.hpp"
#include "ashiato/sync/protocol.hpp"
#ifdef ASHIATO_SYNC_ENABLE_TRACING
#include "ashiato/sync/tracing.hpp"
#endif

#include <algorithm>
#include <cmath>
#include <limits>
#include <stdexcept>
#include <string>

namespace ashiato::sync {

namespace {

bool is_filtered_priority(float priority) noexcept {
    return !(priority > 0.0f);
}

class QuantizedFrameReferenceGuard {
public:
    QuantizedFrameReferenceGuard(ReplicationServer& server, std::uint32_t frame)
        : server_(server),
          frame_(frame) {
        // Serialization may reuse a frame owned by another pending entry or baseline,
        // so this send attempt needs its own reference before any rejection path.
        server_.retain_server_quantized_frame(frame_);
    }

    ~QuantizedFrameReferenceGuard() {
        if (owns_reference_) {
            server_.release_server_quantized_frame(frame_);
        }
    }

    QuantizedFrameReferenceGuard(const QuantizedFrameReferenceGuard&) = delete;
    QuantizedFrameReferenceGuard& operator=(const QuantizedFrameReferenceGuard&) = delete;

    void transfer_to_pending() noexcept {
        owns_reference_ = false;
    }

private:
    ReplicationServer& server_;
    std::uint32_t frame_;
    bool owns_reference_ = true;
};

}  // namespace

ReplicationServer::ReplicationSendResult server_detail::ServerClientReplicator::UpdateScheduler::send_client(
    ReplicationServer& replication_server,
    ashiato::Registry& registry,
    const SyncSettings& settings,
    ServerClientReplicator& replication,
    std::uint32_t completed_frames) {
    (void)completed_frames;
    ReplicationServer::ReplicationSendResult result;
    ++replication.epoch;
    replication.expire_pending_cues(replication_server.frame());

    const ReplicationServerOptions& options = replication_server.options();
    std::size_t remaining = replication.bandwidth == nullptr
        ? (replication.bandwidth_limit_bytes_per_tick != 0 ? replication.bandwidth_limit_bytes_per_tick
                                                           : options.bandwidth_limit_bytes_per_tick)
        : replication.bandwidth->send_available_bytes();
    std::uint16_t packet_entities = 0;
    // Number of records this client serialized but could not fit in its remaining bandwidth.
    std::size_t budget_refusals = 0;
    candidates_.clear();
    candidates_.reserve(replication.dirty_queue.dirty_replicated_indices.size() + replication.destroys.size());
    update_candidates_.clear();
    update_candidates_.reserve(replication.dirty_queue.dirty_replicated_indices.size());
    destroy_order_.clear();
    destroy_order_.reserve(replication.destroys.size());
    records_.clear();
    records_.reserve_bytes(options.mtu_bytes);
    packet_ack_records_.clear();
    packet_ack_records_.reserve(options.mtu_bytes / 8U);

    replication.ensure_capacity(replication_server.replicated_slot_count());
    for (const std::uint32_t slot : replication.dirty_queue.dirty_replicated_indices) {
        if (slot >= replication.dirty_queue.entries.size()) {
            continue;
        }
        ClientDirtyQueue::Entry& entry = replication.dirty_queue.entries[slot];
        if (!entry.queued || entry.baseline_frame >= entry.dirty_frame) {
            continue;
        }
        if (!replication_server.replicated_slot_is_replicable(registry, slot)) {
            if (replication_server.replicated_slot_active(slot)) {
                replication_server.deactivate_replicated_slot(slot);
            }
            continue;
        }
        refresh_replication_decision_if_due(replication_server, replication, slot, entry);
        if (is_filtered_priority(entry.last_priority)) {
            continue;
        }
        if (std::numeric_limits<float>::max() - entry.priority_accumulator < entry.last_priority) {
            entry.priority_accumulator = std::numeric_limits<float>::max();
        } else {
            entry.priority_accumulator += entry.last_priority;
        }
        const ClientEntityState* entity_state = replication.entities.try_get(slot);
        if (entity_state == nullptr) {
            continue;
        }
        update_candidates_.push_back(SerializedCandidate{
            SerializedCandidate::Kind::Update,
            slot,
            0,
            server_detail::boosted_candidate_priority(
                entry.priority_accumulator,
                0.0f,
                entity_state->reference_priority_boost_pending),
            entry.component_mask});
    }
    std::stable_sort(
        update_candidates_.begin(),
        update_candidates_.end(),
        [](const SerializedCandidate& lhs, const SerializedCandidate& rhs) {
            return lhs.priority > rhs.priority;
        });

    if (replication.destroys.empty()) {
        candidates_.insert(candidates_.end(), update_candidates_.begin(), update_candidates_.end());
    } else {
        for (std::size_t index = 0; index < replication.destroys.size(); ++index) {
            destroy_order_.push_back(index);
        }
        std::stable_sort(
            destroy_order_.begin(),
            destroy_order_.end(),
            [&](std::size_t lhs, std::size_t rhs) {
                const ClientDestroyState& left = replication.destroys.at(lhs);
                const ClientDestroyState& right = replication.destroys.at(rhs);
                if (left.reset_epoch != right.reset_epoch) {
                    return left.reset_epoch < right.reset_epoch;
                }
                return lhs < rhs;
            });

        for (const std::size_t destroy_index : destroy_order_) {
            const ClientDestroyState& destroy = replication.destroys.at(destroy_index);
            candidates_.push_back(SerializedCandidate{
                SerializedCandidate::Kind::Destroy,
                0,
                destroy_index,
                static_cast<float>(replication.epoch - destroy.reset_epoch)});
        }
        candidates_.insert(candidates_.end(), update_candidates_.begin(), update_candidates_.end());
    }
    result.had_pending_data = !candidates_.empty();

    const std::size_t update_header_bits = server_detail::server_update_header_bits(options);
    for (const SerializedCandidate& candidate : candidates_) {
        if (candidate.kind == SerializedCandidate::Kind::Destroy) {
            if (candidate.destroy_index >= replication.destroys.size()) {
                continue;
            }
            ClientDestroyState& destroy = replication.destroys.at(candidate.destroy_index);
            const std::size_t destroy_bits =
                server_detail::destroy_record_bits(
                    destroy.network_id,
                    options.protocol.network_entity_id_tier0_bits);
            const std::size_t next_packet_bits =
                update_header_bits + records_.bit_size() + destroy_bits;
            if (!records_.empty() && protocol::bytes_for_bits(next_packet_bits) > options.mtu_bytes) {
                const std::size_t packet_bytes =
                    protocol::bytes_for_bits(update_header_bits + records_.bit_size());
                const std::size_t charged_bytes = replication_server.charged_packet_bytes(packet_bytes);
                ASHIATO_SYNC_ASSERT(charged_bytes <= remaining);
                replication_server.send_server_update_packet(replication, replication_server.frame(), packet_entities, records_, packet_ack_records_);
                remaining -= charged_bytes;
                result.charged_bytes += charged_bytes;
                records_.clear();
                packet_ack_records_.clear();
                packet_entities = 0;
            }

            const std::size_t packet_bytes =
                protocol::bytes_for_bits(update_header_bits + records_.bit_size() + destroy_bits);
            const std::size_t charged_bytes = replication_server.charged_packet_bytes(packet_bytes);
            if (packet_bytes > options.mtu_bytes || charged_bytes > remaining) {
                if (packet_bytes <= options.mtu_bytes) {
                    result.stopped_for_budget = true;
                }
                continue;
            }

            destroy.frame = replication_server.frame();
            destroy.reset_epoch = replication.epoch;
            records_.write_bool(true);
            protocol::write_network_entity_id(
                records_,
                destroy.network_id,
                options.protocol.network_entity_id_tier0_bits);
#ifdef ASHIATO_SYNC_ENABLE_TRACING
            replication_server.trace_entity_destroyed(replication.id, destroy.entity, destroy.network_id, destroy.network_version);
#endif
            packet_ack_records_.push_back(PacketAckRecord{destroy.entity, replication_server.frame(), true});
            ++packet_entities;
            continue;
        }

        const std::uint32_t slot = candidate.slot;
        if (!replication_server.replicated_slot_is_replicable(registry, slot)) {
            continue;
        }

        serialized_.payload.clear();
        serialized_.quantized_frame = server_detail::invalid_quantized_frame_id;
        serialized_.full_state = false;
#ifdef ASHIATO_SYNC_ENABLE_TRACING
        serialized_.deferred_trace_events.clear();
#endif
        if (!writer_.serialize_entity(
                replication_server,
                registry,
                settings,
                replication,
                slot,
                replication_server.frame(),
                candidate.component_mask,
                serialized_)) {
            continue;
        }
        QuantizedFrameReferenceGuard frame_reference{
            replication_server,
            serialized_.quantized_frame};

        const std::size_t next_packet_bits =
            update_header_bits + records_.bit_size() + 1U + serialized_.payload.bit_size();
        if (!records_.empty() && protocol::bytes_for_bits(next_packet_bits) > options.mtu_bytes) {
            const std::size_t packet_bytes =
                protocol::bytes_for_bits(update_header_bits + records_.bit_size());
            const std::size_t charged_bytes = replication_server.charged_packet_bytes(packet_bytes);
            ASHIATO_SYNC_ASSERT(charged_bytes <= remaining);
            replication_server.send_server_update_packet(replication, replication_server.frame(), packet_entities, records_, packet_ack_records_);
            remaining -= charged_bytes;
            result.charged_bytes += charged_bytes;
            records_.clear();
            packet_ack_records_.clear();
            packet_entities = 0;
        }

        const std::size_t single_packet_bits =
            update_header_bits + records_.bit_size() + 1U + serialized_.payload.bit_size();
        const std::size_t packet_bytes = protocol::bytes_for_bits(single_packet_bits);
        const std::size_t charged_bytes = replication_server.charged_packet_bytes(packet_bytes);
        if (packet_bytes > options.mtu_bytes || charged_bytes > remaining) {
            if (packet_bytes <= options.mtu_bytes) {
                result.stopped_for_budget = true;
                ++budget_refusals;
                if (budget_refusals >= options.max_budget_refusals_per_client_tick) {
                    break;
                }
            } else {
                replication_server.log_entity_update_exceeds_mtu(
                    replication.peer,
                    replication.id,
                    replication_server.replicated_slot_entity(slot),
                    replication_server.replicated_slot_archetype(slot),
                    packet_bytes,
                    options.mtu_bytes,
                    serialized_.payload.bit_size());
#if defined(ASHIATO_SYNC_ENABLE_TRACING) && defined(ASHIATO_SYNC_TRACE_PACKET_LOGS)
                if (SyncTracer* tracer = replication_server.server_tracer();
                    tracer != nullptr && tracer->enabled() && tracer->packet_logs_enabled()) {
                    SyncTraceEvent event;
                    event.type = SyncTraceEventType::PacketLog;
                    event.role = SyncTraceRole::Server;
                    event.client = replication.id;
                    event.frame = replication_server.frame();
                    event.server_entity = replication_server.replicated_slot_entity(slot);
                    event.archetype = replication_server.replicated_slot_archetype(slot);
                    event.data = "direction=meta,message=server_entity_update_exceeds_mtu,client=" + std::to_string(replication.id) +
                        ",entity=" + std::to_string(replication_server.replicated_slot_entity(slot).value) +
                        ",archetype=" + std::to_string(replication_server.replicated_slot_archetype(slot).value) +
                        ",packet_bytes=" + std::to_string(packet_bytes) +
                        ",mtu_bytes=" + std::to_string(options.mtu_bytes) +
                        ",record_bits=" + std::to_string(serialized_.payload.bit_size());
                    tracer->trace(event);
                }
#endif
            }
            continue;
        }

        records_.write_bool(false);
        records_.write_buffer_bits(serialized_.payload);
#ifdef ASHIATO_SYNC_ENABLE_TRACING
        if (SyncTracer* tracer = replication_server.server_tracer()) {
            for (const SyncTraceEvent& event : serialized_.deferred_trace_events) {
                tracer->trace(event);
            }
        }
#endif
        PacketAckRecord ack_record{replication_server.replicated_slot_entity(slot), replication_server.frame(), false};
#if defined(ASHIATO_SYNC_ENABLE_TRACING) && defined(ASHIATO_SYNC_TRACE_PACKET_LOGS)
        if (const ClientEntityState* entity_state = replication.entities.try_get(slot)) {
            replication_server.append_server_packet_ack_cues(settings, *entity_state, ack_record);
        }
#endif
        packet_ack_records_.push_back(ack_record);
        ++packet_entities;
        if (slot < replication.dirty_queue.entries.size()) {
            ClientDirtyQueue::Entry& entry = replication.dirty_queue.entries[slot];
            entry.dirty_frame = replication_server.frame();
            entry.priority_accumulator = 0.0f;
        }
        ClientEntityState& entity_state = replication.entities.at(slot);
        if (serialized_.full_state &&
            entity_state.baseline != server_detail::invalid_quantized_frame_id) {
            replication.invalidate_entity_baseline(replication_server, entity_state);
        }
        entity_state.reference_priority_boost_pending = false;
        entity_state.pending.push_back(ClientEntityState::PendingQuantizedFrame{
            serialized_.quantized_frame,
            replication_server.frame(),
            entity_state.baseline_epoch});
        // The pending entry now owns the reference retained by the guard.
        frame_reference.transfer_to_pending();
        while (entity_state.pending.size() > server_detail::max_pending_quantized_frames_per_entity) {
            replication_server.release_server_quantized_frame(entity_state.pending.front().quantized_frame);
            entity_state.pending.erase(entity_state.pending.begin());
        }
    }

    if (!records_.empty()) {
        const std::size_t packet_bytes =
            protocol::bytes_for_bits(update_header_bits + records_.bit_size());
        const std::size_t charged_bytes = replication_server.charged_packet_bytes(packet_bytes);
        if (charged_bytes <= remaining) {
            replication_server.send_server_update_packet(replication, replication_server.frame(), packet_entities, records_, packet_ack_records_);
            result.charged_bytes += charged_bytes;
        } else {
            result.stopped_for_budget = true;
        }
    }
    cleanup_dirty_queue(replication);
    return result;
}

void server_detail::ServerClientReplicator::UpdateScheduler::refresh_replication_decision_if_due(
    ReplicationServer& replication_server,
    ServerClientReplicator& replication,
    std::uint32_t slot,
    ClientDirtyQueue::Entry& entry) {
    const SyncFrame interval = replication_server.options().entity_replication_decision_interval_frames;
    const bool bucket_due = interval == 0U || slot % interval == replication_server.frame() % interval;
    if (!is_filtered_priority(entry.last_priority) && !bucket_due) {
        return;
    }

    const EntityReplicationDecision decision = replication_server.options().entity_replication_decider(
        replication.id,
        EntityReplicationDecisionContext{replication_server.replicated_slot_entity(slot)});
#if !defined(NDEBUG) || defined(ASHIATO_SYNC_ENABLE_ASSERT)
    if (std::isnan(decision.priority)) {
        throw std::invalid_argument("replication priority must not be NaN");
    }
#endif
    // Entity state outlives rebuilt dirty-queue entries.
    ClientEntityState* entity_state = replication.entities.try_get(slot);
    const std::uint64_t previous_mask = entity_state != nullptr
        ? entity_state->component_mask
        : entry.component_mask;
    entry.last_priority = decision.priority;
    entry.component_mask = decision.component_mask;
    if (entity_state != nullptr) {
        entity_state->last_priority = entry.last_priority;
        entity_state->component_mask = entry.component_mask;
        // Reopened components require a full update to restore a shared baseline.
        if ((decision.component_mask & ~previous_mask) != 0U) {
            replication.invalidate_entity_baseline(replication_server, *entity_state);
        }
    }
}

void server_detail::ServerClientReplicator::UpdateScheduler::cleanup_dirty_queue(
    ServerClientReplicator& replication) {
    replication.dirty_queue.dirty_replicated_indices.erase(
        std::remove_if(
            replication.dirty_queue.dirty_replicated_indices.begin(),
            replication.dirty_queue.dirty_replicated_indices.end(),
            [&](std::uint32_t slot) {
                const bool remove = slot >= replication.dirty_queue.entries.size() ||
                    !replication.dirty_queue.entries[slot].queued;
                if (remove && slot < replication.dirty_queue.entries.size()) {
                    replication.dirty_queue.entries[slot].listed = false;
                }
                return remove;
            }),
        replication.dirty_queue.dirty_replicated_indices.end());
}

}  // namespace ashiato::sync
