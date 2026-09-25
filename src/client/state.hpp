#pragma once

#include "ashiato/sync/client.hpp"

#include <cstddef>
#include <cstdint>
#include <limits>
#include <vector>

namespace ashiato::sync::client_detail {

inline constexpr std::uint32_t invalid_entity_index = std::numeric_limits<std::uint32_t>::max();
inline constexpr std::size_t max_baseline_history_per_entity = protocol::baseline_retention_count;
static_assert(
    max_baseline_history_per_entity > 0U &&
    (max_baseline_history_per_entity & (max_baseline_history_per_entity - 1U)) == 0U);

struct EntityCue {
    SyncFrame frame = 0;
    SyncCueTypeId type = 0;
    CueValue value;
};

struct EntityPlayedCue {
    std::uint32_t entity_index = invalid_entity_index;
    SyncFrame frame = 0;
    SyncCueTypeId type = 0;
    CueValue value;
    bool confirmed = false;
    std::uint32_t seen_resim_generation = 0;
};

struct EntityFrameBaseline {
    SyncFrame frame = 0;
    bool valid = false;
    QuantizedFrameData baseline;
};

struct EntityComponentError {
    ashiato::Entity component;
    SyncComponentSerializerId serializer = invalid_sync_component_serializer_id;
    SyncComponentOps::QuantizedBytes bytes;
};

struct EntityBufferedFrame {
    SyncFrame frame = 0;
    bool valid = false;
    bool entity_present = false;
    std::uint64_t component_apply_mask = 0;
    QuantizedFrameData baseline;
};

struct EntityState {
    struct Identity {
        ashiato::Entity local;
        ClientEntityNetworkId client_entity_network_id = invalid_client_entity_network_id;
        std::uint32_t wire_network_id = 0;
        SyncArchetypeId archetype;
    } identity;

    struct Replication {
        SyncFrame frame = 0;
        bool entity_present = true;
        QuantizedFrameData baseline;
        std::vector<EntityFrameBaseline> history;
        std::size_t history_next = 0;
        EntityFrameBaseline pinned_wire_baseline;
        SyncFrame referenced_baseline_frame = 0;
        bool has_referenced_baseline = false;
        std::uint64_t applied_present_mask = 0;
        // The frame of an authoritative destroy this client has recorded but not yet applied, or 0. See
        // ClientBufferedRuntime::apply_frame: a destroy whose absent sample is never viewed is applied from here.
        SyncFrame destroy_frame = 0;
    } replication;

    struct Mode {
        ReplicationClientMode current = ReplicationClientMode::Snap;
        bool selected = false;
        SyncFrame last_applied_buffered_frame = 0;
    } mode;

    struct VisualCorrection {
        std::vector<EntityComponentError> snap_errors;
    } visual;

    struct Memberships {
        std::size_t active_index = std::numeric_limits<std::size_t>::max();
        std::size_t buffered_index = std::numeric_limits<std::size_t>::max();
        std::size_t snap_error_index = std::numeric_limits<std::size_t>::max();
        std::size_t predicted_index = std::numeric_limits<std::size_t>::max();
        std::size_t prediction_rollback_index = std::numeric_limits<std::size_t>::max();
    } memberships;

    struct Prediction {
        bool rollback_pending = false;
        SyncFrame rollback_frame = 0;
    } prediction;
};

struct OriginalPredictionCapture {
    std::uint32_t entity_index = invalid_entity_index;
    QuantizedFrameData baseline;
};

struct WireNetworkIdState {
    std::uint32_t version = 0;
    std::uint32_t entity_index = invalid_entity_index;
    bool alive = false;
};

struct InputFrame {
    SyncFrame frame = 0;
    bool valid = false;
    std::vector<std::uint8_t> bytes;
};

struct PendingPing {
    double local_send_time_seconds = 0.0;
};

}  // namespace ashiato::sync::client_detail
