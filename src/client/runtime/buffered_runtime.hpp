#pragma once

#include "client/state.hpp"
#include "client/store/frame_ring_store.hpp"

#include "ashiato/ashiato.hpp"
#include "ashiato/sync/client_clock.hpp"

#include <cstddef>
#include <cstdint>

namespace ashiato::sync {

class ReplicationClient;

namespace client_detail {

enum class MissingBufferedFramePolicy : std::uint8_t {
    FailApply,
    AllowMissing,
};

class ClientBufferedRuntime {
public:
    explicit ClientBufferedRuntime(std::size_t frame_capacity = 64);

    ClientFrameRingStore& frames() noexcept {
        return buffered_frames_;
    }

    const ClientFrameRingStore& frames() const noexcept {
        return buffered_frames_;
    }

    bool has_applied_frame() const noexcept {
        return has_applied_buffered_frame_;
    }

    SyncFrame last_applied_frame() const noexcept {
        return last_applied_buffered_frame_;
    }

    void reset_entity(std::uint32_t entity_index) noexcept;
    void ensure_entity(std::uint32_t entity_index);
    void clear_entity(std::uint32_t entity_index) noexcept;

    bool apply_frames(
        ReplicationClient& client,
        ashiato::Registry& registry,
        const ReplicationClientClock::FrameRange& frames);
    bool apply_frame(
        ReplicationClient& client,
        ashiato::Registry& registry,
        SyncFrame buffered_frame,
        MissingBufferedFramePolicy missing_frame_policy = MissingBufferedFramePolicy::FailApply);
    // Re-applies one buffered frame to the listed buffered entities alone (PredictedOnly's companions). An entity with
    // no sample for the frame, or one the sample says is gone, is left as it is: nothing is destroyed and no cue plays.
    bool apply_frame_for(
        ReplicationClient& client,
        ashiato::Registry& registry,
        SyncFrame buffered_frame,
        const std::vector<std::uint32_t>& entity_indices);

private:
    ClientFrameRingStore buffered_frames_;
    SyncFrame last_applied_buffered_frame_ = 0;
    bool has_applied_buffered_frame_ = false;
};

}  // namespace client_detail
}  // namespace ashiato::sync
