#include "ashiato/sync/server.hpp"

#include "server/state.hpp"

#include <algorithm>
#include <limits>

namespace ashiato::sync {

void ReplicationServer::retain_quantized_frame(std::uint32_t quantized_frame) {
    if (quantized_frame != invalid_quantized_frame_id &&
        quantized_frame < quantized_frames_.size() &&
        quantized_frames_[quantized_frame].active) {
        ++quantized_frames_[quantized_frame].ref_count;
    }
}

void ReplicationServer::release_quantized_frame(std::uint32_t quantized_frame) {
    if (quantized_frame == invalid_quantized_frame_id ||
        quantized_frame >= quantized_frames_.size() ||
        !quantized_frames_[quantized_frame].active) {
        return;
    }

    QuantizedFrame& current = quantized_frames_[quantized_frame];
    if (current.ref_count != 0) {
        --current.ref_count;
        if (current.ref_count != 0) {
            return;
        }
    }

    if (current.slot < replicated_.size()) {
        std::vector<std::uint32_t>& slot_quantized_frames = replicated_[current.slot].quantized_frames;
        slot_quantized_frames.erase(
            std::remove(slot_quantized_frames.begin(), slot_quantized_frames.end(), quantized_frame),
            slot_quantized_frames.end());
    }
    current.active = false;
    current.data.clear();
    current.dirty_generations.clear();
    free_quantized_frames_.push_back(quantized_frame);
}

void ReplicationServer::clear_client_entity_state(ClientEntityState& state) {
    release_quantized_frame(state.baseline);
    for (const ClientEntityState::PendingQuantizedFrame& pending : state.pending) {
        release_quantized_frame(pending.quantized_frame);
    }
    state.baseline = invalid_quantized_frame_id;
    state.baseline_epoch = 0;
    state.pending.clear();
    state.network_id = 0;
    state.network_version = 0;
    state.last_priority = std::numeric_limits<float>::quiet_NaN();
    state.component_mask = std::numeric_limits<std::uint64_t>::max();
    state.last_reserved = false;
    state.reference_priority_boost_pending = false;
    state.has_network_id = false;
    state.pending_cues.clear();
}

void ReplicationServer::acknowledge_cues(ClientEntityState& state, SyncFrame frame) {
    state.pending_cues.erase(
        std::remove_if(
            state.pending_cues.begin(),
            state.pending_cues.end(),
            [frame](const ClientEntityState::PendingCue& cue) {
                return cue.frame <= frame;
            }),
        state.pending_cues.end());
}

bool ReplicationServer::same_quantized_frame_components(
    const QuantizedFrame& quantized_frame,
    const QuantizedFrameData& data,
    const std::vector<std::uint64_t>& dirty_generations) const {
    if (quantized_frame.data.tag_mask != data.tag_mask ||
        quantized_frame.data.present_mask != data.present_mask ||
        quantized_frame.data.bytes != data.bytes ||
        quantized_frame.dirty_generations.size() != dirty_generations.size()) {
        return false;
    }
    for (std::size_t index = 0; index < dirty_generations.size(); ++index) {
        if (quantized_frame.dirty_generations[index] != dirty_generations[index]) {
            return false;
        }
    }
    return true;
}

}  // namespace ashiato::sync
