#pragma once

#include "ashiato/bit_buffer.hpp"
#include "ashiato/sync/logging.hpp"
#include "ashiato/sync/protocol.hpp"

#include "ashiato/ashiato.hpp"

#include <algorithm>
#include <array>
#include <cmath>
#include <cstddef>
#include <cstdint>
#include <cstring>
#include <functional>
#include <limits>
#include <mutex>
#include <new>
#include <stdexcept>
#include <string>
#include <type_traits>
#include <typeindex>
#include <unordered_map>
#include <utility>
#include <vector>

namespace ashiato::sync {

namespace client_detail {
class ClientCueRuntime;
}  // namespace client_detail

using ClientId = std::uint8_t;
using PeerId = std::uint64_t;
using SyncFrame = std::uint32_t;
using SyncCueTypeId = std::uint16_t;
using ClientEntityNetworkId = std::uint64_t;
using TransportFn = std::function<void(PeerId, const ashiato::BitBuffer&)>;
using ConnectHandlerFn = std::function<bool(const std::string&, ClientId&, std::string&)>;

namespace detail {

inline bool valid_cue_relevance_value(float relevance_seconds) noexcept {
    return relevance_seconds >= 0.0f && std::isfinite(relevance_seconds);
}

inline bool cue_relevance_fits_frame_range(
    SyncFrame frame,
    float relevance_seconds,
    double fixed_dt_seconds) noexcept {
    if (frame == 0U || !valid_cue_relevance_value(relevance_seconds) ||
        fixed_dt_seconds <= 0.0 || !std::isfinite(fixed_dt_seconds)) {
        return false;
    }
    const double relevance_frames = std::ceil(static_cast<double>(relevance_seconds) / fixed_dt_seconds);
    return std::isfinite(relevance_frames) &&
        relevance_frames <= static_cast<double>(std::numeric_limits<SyncFrame>::max() - frame);
}

}  // namespace detail

struct SyncSettings;
struct QueuedSyncCueView;
class ReplicationClient;
class ReplicationBandwidthBudget;
class ReplicationReplayStreamer;
class ReplicationServer;
#ifdef ASHIATO_SYNC_ENABLE_TRACING
class SyncTracer;
#endif

struct ReplicationPriorityObject {
    ashiato::Entity entity;
};

struct ReplicationPriorityDecision {
    float priority = 0.0f;
    std::uint64_t component_mask = std::numeric_limits<std::uint64_t>::max();
};

using ReplicationPrioritizerFn = std::function<ReplicationPriorityDecision(ClientId, ReplicationPriorityObject)>;

inline constexpr ClientId invalid_client_id = std::numeric_limits<ClientId>::max();
inline constexpr PeerId invalid_peer_id = std::numeric_limits<PeerId>::max();
inline constexpr ClientEntityNetworkId invalid_client_entity_network_id = 0;
inline constexpr ClientId max_client_entity_network_id_client = invalid_client_id - 1U;
inline constexpr std::uint32_t max_client_local_wire_network_id = (std::uint32_t{1} << 20U) - 1U;

enum class ReplicationServerConnectionEventType {
    Accepted,
    Ready,
    Rejected,
    Removed,
    TimedOut
};

struct ReplicationServerConnectionEvent {
    ReplicationServerConnectionEventType type = ReplicationServerConnectionEventType::Accepted;
    PeerId peer = invalid_peer_id;
    ClientId client = invalid_client_id;
    bool local = false;
    std::string reason;
};

using ServerConnectionEventFn = std::function<void(const ReplicationServerConnectionEvent&)>;

inline constexpr ClientEntityNetworkId make_client_entity_network_id(
    ClientId client,
    std::uint32_t wire_network_id,
    std::uint32_t version) noexcept {
    return ((static_cast<std::uint64_t>(client) & 0xffULL) << 52U) |
        ((static_cast<std::uint64_t>(wire_network_id) & 0xfffffULL) << 32U) |
        static_cast<std::uint64_t>(version);
}

inline constexpr ClientId client_entity_network_id_client(ClientEntityNetworkId id) noexcept {
    return static_cast<ClientId>((id >> 52U) & 0xffULL);
}

inline constexpr std::uint32_t client_entity_network_id_wire_id(ClientEntityNetworkId id) noexcept {
    return static_cast<std::uint32_t>((id >> 32U) & 0xfffffULL);
}

inline constexpr std::uint32_t client_entity_network_id_version(ClientEntityNetworkId id) noexcept {
    return static_cast<std::uint32_t>(id & 0xffffffffULL);
}

enum class SyncRole {
    Server,
    Client
};

struct SyncArchetypeId {
    std::uint32_t value = std::numeric_limits<std::uint32_t>::max();
};

inline constexpr SyncArchetypeId invalid_sync_archetype_id{};

struct SyncComponentSerializerId {
    std::uint32_t value = std::numeric_limits<std::uint32_t>::max();
};

inline constexpr SyncComponentSerializerId invalid_sync_component_serializer_id{};

constexpr bool operator==(SyncArchetypeId lhs, SyncArchetypeId rhs) noexcept {
    return lhs.value == rhs.value;
}

constexpr bool operator!=(SyncArchetypeId lhs, SyncArchetypeId rhs) noexcept {
    return !(lhs == rhs);
}

constexpr bool operator==(SyncComponentSerializerId lhs, SyncComponentSerializerId rhs) noexcept {
    return lhs.value == rhs.value;
}

constexpr bool operator!=(SyncComponentSerializerId lhs, SyncComponentSerializerId rhs) noexcept {
    return !(lhs == rhs);
}

enum class ReplicationAudience {
    Owner,
    All,
    EveryoneExceptOwner
};

inline constexpr bool replication_audience_matches(
    ReplicationAudience audience,
    ClientId owner,
    ClientId client) noexcept {
    switch (audience) {
        case ReplicationAudience::Owner:
            return owner != invalid_client_id && owner == client;
        case ReplicationAudience::EveryoneExceptOwner:
            return owner == invalid_client_id || owner != client;
        case ReplicationAudience::All:
            return true;
    }
    return false;
}

enum class ComponentInterpolation {
    Step,
    Interpolate
};

enum class ReplicationClientMode {
    Snap,
    BufferedInterpolation,
    Predict
};

enum class ReplicationRollbackPolicy {
    All,
    OnlyAffected
};

#ifdef ASHIATO_SYNC_ENABLE_TRACING
struct TraceOptions {
    bool enabled = false;
    std::string directory;
    std::size_t queue_capacity_bytes = 4 * 1024 * 1024;
    std::size_t flush_threshold_bytes = 64 * 1024;
    bool truncate_existing = true;
    bool frame_data = false;
    bool packet_logs = false;
    bool serialization_payloads = false;
    std::vector<ClientId> monitored_clients;
};

struct SyncTraceStringBuilder {
    std::string value;

    void clear() noexcept {
        value.clear();
    }

    void append(const char* text) {
        if (text != nullptr) {
            value += text;
        }
    }

    void append(const std::string& text) {
        value += text;
    }

    template <typename T>
    void append_number(T number) {
        value += std::to_string(number);
    }
};
#endif

struct ComponentReplication {
    ashiato::Entity component;
    ReplicationAudience audience = ReplicationAudience::All;
    ComponentInterpolation interpolation = ComponentInterpolation::Step;
    SyncComponentSerializerId serializer = invalid_sync_component_serializer_id;
};

struct SyncTagReplication {
    ashiato::Entity tag;
    ReplicationAudience audience = ReplicationAudience::All;
};

struct SyncArchetypeDesc {
    std::string name;
    std::vector<SyncTagReplication> tags;
    std::vector<ComponentReplication> components;
};

class QuantizedBytes {
public:
    static constexpr std::size_t inline_capacity = 64;
    static constexpr std::size_t max_size = 1200;

    QuantizedBytes() = default;

    QuantizedBytes(const QuantizedBytes& other) {
        assign(other.data(), other.size_);
    }

    QuantizedBytes& operator=(const QuantizedBytes& other) {
        if (this != &other) {
            assign(other.data(), other.size_);
        }
        return *this;
    }

    QuantizedBytes(QuantizedBytes&& other) noexcept {
        move_from(std::move(other));
    }

    QuantizedBytes& operator=(QuantizedBytes&& other) noexcept {
        if (this != &other) {
            move_from(std::move(other));
        }
        return *this;
    }

    std::size_t size() const noexcept {
        return size_;
    }

    bool empty() const noexcept {
        return size_ == 0;
    }

    std::uint8_t* data() noexcept {
        return overflow_ == nullptr ? inline_.data() : overflow_.get();
    }

    const std::uint8_t* data() const noexcept {
        return overflow_ == nullptr ? inline_.data() : overflow_.get();
    }

    std::uint8_t* begin() noexcept {
        return data();
    }

    std::uint8_t* end() noexcept {
        return data() + size_;
    }

    const std::uint8_t* begin() const noexcept {
        return data();
    }

    const std::uint8_t* end() const noexcept {
        return data() + size_;
    }

    void clear() noexcept {
        size_ = 0;
    }

    void resize(std::size_t size) {
        if (size > max_size) {
            throw std::length_error("sync quantized component payload exceeds maximum size");
        }
        if (size <= inline_capacity) {
            if (overflow_ != nullptr) {
                const std::size_t copy_size = std::min(size_, size);
                if (copy_size != 0U) {
                    std::memcpy(inline_.data(), overflow_.get(), copy_size);
                }
                overflow_.reset();
                overflow_capacity_ = 0;
            }
            size_ = size;
            return;
        }

        if (overflow_ == nullptr || overflow_capacity_ < size) {
            std::unique_ptr<std::uint8_t[]> next = std::make_unique<std::uint8_t[]>(size);
            const std::size_t copy_size = std::min(size_, size);
            if (copy_size != 0U) {
                std::memcpy(next.get(), data(), copy_size);
            }
            overflow_ = std::move(next);
            overflow_capacity_ = size;
        }
        size_ = size;
    }

    void assign(const void* data, std::size_t size) {
        resize(size);
        if (size != 0U) {
            std::memcpy(this->data(), data, size);
        }
    }

    friend bool operator==(const QuantizedBytes& lhs, const QuantizedBytes& rhs) noexcept {
        return lhs.size_ == rhs.size_ && std::equal(lhs.data(), lhs.data() + lhs.size_, rhs.data());
    }

    friend bool operator!=(const QuantizedBytes& lhs, const QuantizedBytes& rhs) noexcept {
        return !(lhs == rhs);
    }

private:
    void move_from(QuantizedBytes&& other) noexcept {
        size_ = other.size_;
        inline_ = other.inline_;
        overflow_ = std::move(other.overflow_);
        overflow_capacity_ = other.overflow_capacity_;
        other.size_ = 0;
        other.overflow_capacity_ = 0;
    }

    std::size_t size_ = 0;
    std::array<std::uint8_t, inline_capacity> inline_{};
    std::unique_ptr<std::uint8_t[]> overflow_;
    std::size_t overflow_capacity_ = 0;
};

struct EntityReference {
    ashiato::Entity entity;
    ClientEntityNetworkId client_entity_network_id = invalid_client_entity_network_id;
};

struct EntityReferenceContext {
    using ServerNetworkIdForEntityFn = std::uint32_t (*)(void*, ashiato::Entity);
    using ClientEntityNetworkIdForWireFn = ClientEntityNetworkId (*)(void*, std::uint32_t);
    using ClientLocalEntityFn = ashiato::Entity (*)(void*, ClientEntityNetworkId);

    void* userContext = nullptr;
    std::size_t network_entity_id_tier0_bits = protocol::default_network_entity_id_tier0_bits;
    ServerNetworkIdForEntityFn server_network_id_for_entity = nullptr;
    ClientEntityNetworkIdForWireFn client_entity_network_id_for_wire = nullptr;
    ClientLocalEntityFn client_local_entity = nullptr;

    std::uint32_t network_id_for_entity(ashiato::Entity entity) const {
        if (!entity || server_network_id_for_entity == nullptr) {
            return 0U;
        }
        return server_network_id_for_entity(userContext, entity);
    }

    ClientEntityNetworkId network_id_for_wire(std::uint32_t wire_network_id) const {
        if (wire_network_id == 0U || client_entity_network_id_for_wire == nullptr) {
            return invalid_client_entity_network_id;
        }
        return client_entity_network_id_for_wire(userContext, wire_network_id);
    }

    ashiato::Entity local_entity(ClientEntityNetworkId network_id) const {
        if (network_id == invalid_client_entity_network_id || client_local_entity == nullptr) {
            return ashiato::Entity{};
        }
        return client_local_entity(userContext, network_id);
    }
};

inline bool write_entity_reference(
    ashiato::BitBuffer& out,
    ashiato::Entity entity,
    const EntityReferenceContext& context) {
    if (!entity) {
        out.write_bool(false);
        return true;
    }
    const std::uint32_t network_id = context.network_id_for_entity(entity);
    out.write_bool(network_id != 0U);
    if (network_id == 0U) {
        return false;
    }
    protocol::write_network_entity_id(out, network_id, context.network_entity_id_tier0_bits);
    return true;
}

inline bool write_entity_reference(
    ashiato::BitBuffer& out,
    const EntityReference& reference,
    const EntityReferenceContext& context) {
    return write_entity_reference(out, reference.entity, context);
}

inline bool read_entity_reference(
    ashiato::BitBuffer& in,
    EntityReferenceContext& context,
    EntityReference& out) {
    detail::BitReader reader(in);
    bool has_reference = false;
    if (!reader.read_bits(1U, has_reference)) {
        return false;
    }
    if (!has_reference) {
        out = EntityReference{};
        return true;
    }

    std::uint32_t wire_network_id = 0;
    if (!protocol::read_network_entity_id(reader, wire_network_id, context.network_entity_id_tier0_bits) ||
        wire_network_id == 0U) {
        return false;
    }

    const ClientEntityNetworkId client_network_id = context.network_id_for_wire(wire_network_id);
    if (client_network_id == invalid_client_entity_network_id) {
        return false;
    }
    out.client_entity_network_id = client_network_id;
    out.entity = context.local_entity(client_network_id);
    return true;
}

struct SyncComponentOps {
    using QuantizedBytes = ashiato::sync::QuantizedBytes;
    using InterpolateFn = bool (*)(const std::uint8_t*, const std::uint8_t*, float, std::uint8_t*);
    using ComputeErrorFn = bool (*)(const std::uint8_t*, const std::uint8_t*, QuantizedBytes&);
    using ApplyErrorFn = bool (*)(const std::uint8_t*, const QuantizedBytes&, QuantizedBytes&);
    using BlendOutErrorFn = bool (*)(const QuantizedBytes&, float, QuantizedBytes&);
    using ShouldRollBackFn = bool (*)(const std::uint8_t*, const std::uint8_t*);
#if defined(ASHIATO_SYNC_ENABLE_TRACING) && defined(ASHIATO_SYNC_TRACE_COMPONENT_DATA)
    using TraceFn = void (*)(const std::uint8_t*, SyncTraceStringBuilder&);
#endif

    ashiato::ComponentSerializationOps serialization;
    std::size_t error_size = 0;
    bool references_entities = false;
    InterpolateFn interpolate = nullptr;
    ComputeErrorFn compute_error = nullptr;
    ApplyErrorFn apply_error = nullptr;
    BlendOutErrorFn blend_out_error = nullptr;
    ShouldRollBackFn should_roll_back = nullptr;
#if defined(ASHIATO_SYNC_ENABLE_TRACING) && defined(ASHIATO_SYNC_TRACE_COMPONENT_DATA)
    TraceFn trace = nullptr;
#endif
};

class CueValue {
public:
    using DestroyFn = void (*)(void*) noexcept;
    using CopyFn = void (*)(void*, const void*);
    using MoveFn = void (*)(void*, void*) noexcept;

    CueValue() = default;

    CueValue(const CueValue& other) {
        copy_from(other);
    }

    CueValue& operator=(const CueValue& other) {
        if (this != &other) {
            reset();
            copy_from(other);
        }
        return *this;
    }

    CueValue(CueValue&& other) noexcept {
        move_from(std::move(other));
    }

    CueValue& operator=(CueValue&& other) noexcept {
        if (this != &other) {
            reset();
            move_from(std::move(other));
        }
        return *this;
    }

    ~CueValue() {
        reset();
    }

    template <typename T, typename... Args>
    T& emplace(Args&&... args) {
        static_assert(sizeof(T) <= protocol::max_cue_value_bytes, "sync cue value exceeds max cue size");
        static_assert(alignof(T) <= alignof(std::max_align_t), "sync cue value alignment exceeds max cue alignment");
        reset();
        T* value = new (&storage_) T(std::forward<Args>(args)...);
        destroy_ = [](void* ptr) noexcept {
            static_cast<T*>(ptr)->~T();
        };
        copy_ = [](void* dst, const void* src) {
            new (dst) T(*static_cast<const T*>(src));
        };
        move_ = [](void* dst, void* src) noexcept {
            new (dst) T(std::move(*static_cast<T*>(src)));
            static_cast<T*>(src)->~T();
        };
        return *value;
    }

    void reset() noexcept {
        if (destroy_ != nullptr) {
            destroy_(data());
        }
        destroy_ = nullptr;
        copy_ = nullptr;
        move_ = nullptr;
    }

    bool has_value() const noexcept {
        return destroy_ != nullptr;
    }

    void* data() noexcept {
        return &storage_;
    }

    const void* data() const noexcept {
        return &storage_;
    }

private:
    void copy_from(const CueValue& other) {
        if (!other.has_value()) {
            return;
        }
        other.copy_(data(), other.data());
        destroy_ = other.destroy_;
        copy_ = other.copy_;
        move_ = other.move_;
    }

    void move_from(CueValue&& other) noexcept {
        if (!other.has_value()) {
            return;
        }
        other.move_(data(), other.data());
        destroy_ = other.destroy_;
        copy_ = other.copy_;
        move_ = other.move_;
        other.destroy_ = nullptr;
        other.copy_ = nullptr;
        other.move_ = nullptr;
    }

    using Storage = std::aligned_storage_t<protocol::max_cue_value_bytes, alignof(std::max_align_t)>;

    Storage storage_;
    DestroyFn destroy_ = nullptr;
    CopyFn copy_ = nullptr;
    MoveFn move_ = nullptr;
};

struct SyncCueOps {
    using SerializeFn = void (*)(const void*, ashiato::BitBuffer&, ashiato::ComponentSerializationContext&);
    using DeserializeIntoFn = bool (*)(
        SyncCueTypeId,
        void*,
        ashiato::BitBuffer&,
        CueValue&,
        ashiato::ComponentSerializationContext&);
    using PlayFn = bool (*)(
        SyncCueTypeId,
        void*,
        ashiato::Registry&,
        ashiato::Entity,
        const void*,
        float,
        SyncFrame);
    using RollbackFn = bool (*)(
        SyncCueTypeId,
        void*,
        ashiato::Registry&,
        ashiato::Entity,
        const void*);
    using EqualsFn = bool (*)(
        SyncCueTypeId,
        void*,
        const void*,
        const void*);
#if defined(ASHIATO_SYNC_ENABLE_TRACING) && defined(ASHIATO_SYNC_TRACE_COMPONENT_DATA)
    using TraceFn = bool (*)(SyncCueTypeId, void*, const ashiato::BitBuffer&, SyncTraceStringBuilder&);
    using TraceValueFn = bool (*)(SyncCueTypeId, void*, const void*, SyncTraceStringBuilder&);
#endif

    SerializeFn serialize = nullptr;
    DeserializeIntoFn deserialize_into = nullptr;
    PlayFn play = nullptr;
    RollbackFn rollback = nullptr;
    EqualsFn equals = nullptr;
    void* user_data = nullptr;
    std::string name;
    bool references_entities = false;
#if defined(ASHIATO_SYNC_ENABLE_TRACING) && defined(ASHIATO_SYNC_TRACE_COMPONENT_DATA)
    TraceFn trace = nullptr;
    TraceValueFn trace_value = nullptr;
#endif
};

struct QueuedSyncCue {
    ashiato::Entity entity;
    SyncFrame frame = 0;
    SyncCueTypeId type = 0;
    float relevance_seconds = 0.0f;
    ashiato::BitBuffer payload;
#if defined(ASHIATO_SYNC_ENABLE_TRACING) && defined(ASHIATO_SYNC_TRACE_COMPONENT_DATA)
    std::vector<ashiato::SerializationTraceScope> payload_trace_scopes;
#endif
    CueValue value;
    bool only_replicate_to_owner = false;
};

struct FrameInfo {
    SyncFrame frame = 0;
};

class CueDispatcher {
public:
    CueDispatcher() = default;
    CueDispatcher(const CueDispatcher& other);
    CueDispatcher& operator=(const CueDispatcher& other);

    CueDispatcher(CueDispatcher&& other) noexcept;
    CueDispatcher& operator=(CueDispatcher&& other) noexcept;

    template <typename T>
    bool emit(
        const SyncSettings& settings,
        const FrameInfo& frame,
        ashiato::Entity entity,
        const T& cue,
        float relevance_seconds,
        bool only_replicate_to_owner = false);

    template <typename T>
    bool emit(
        const SyncSettings& settings,
        SyncFrame frame,
        ashiato::Entity entity,
        const T& cue,
        float relevance_seconds,
        bool only_replicate_to_owner = false);

    bool emit_raw(
        const SyncSettings& settings,
        SyncFrame frame,
        ashiato::Entity entity,
        SyncCueTypeId type,
        ashiato::BitBuffer payload,
        float relevance_seconds,
        bool only_replicate_to_owner = false
#if defined(ASHIATO_SYNC_ENABLE_TRACING) && defined(ASHIATO_SYNC_TRACE_COMPONENT_DATA)
        ,
        std::vector<ashiato::SerializationTraceScope> payload_trace_scopes = {}
#endif
    );

    bool empty() const;
    std::size_t size() const;

private:
    friend class client_detail::ClientCueRuntime;
    friend class ReplicationClient;
    friend class ReplicationReplayStreamer;
    friend class ReplicationServer;

    bool enqueue(QueuedSyncCue cue);
    std::vector<QueuedSyncCue> drain();
    void drain_into(std::vector<QueuedSyncCue>& drained);
    mutable std::mutex mutex_;
    std::vector<QueuedSyncCue> cues_;
};

struct SyncArchetype {
    std::string name;
    std::vector<SyncTagReplication> tags;
    std::vector<ComponentReplication> components;
    std::vector<SyncComponentOps> component_ops;
    std::vector<std::uint32_t> component_offsets;
    std::uint32_t total_quantized_bytes = 0;
};

struct QuantizedFrameData {
    std::uint64_t tag_mask = 0;
    std::uint64_t present_mask = 0;
    std::vector<std::uint8_t> bytes;

    void clear() {
        tag_mask = 0;
        present_mask = 0;
        bytes.clear();
    }
};

struct SyncSettings {
    SyncSettings() = default;
    SyncSettings(const SyncSettings&) = default;
    SyncSettings& operator=(const SyncSettings&) = default;

    SyncSettings(SyncSettings&& other) noexcept {
        swap(other);
    }

    SyncSettings& operator=(SyncSettings&& other) noexcept {
        if (this != &other) {
            SyncSettings moved(std::move(other));
            swap(moved);
        }
        return *this;
    }

    void swap(SyncSettings& other) noexcept {
        using std::swap;
        swap(role, other.role);
        swap(local_client, other.local_client);
        swap(input_component, other.input_component);
        archetypes.swap(other.archetypes);
        component_ops.swap(other.component_ops);
        component_serializers.swap(other.component_serializers);
        cue_ops.swap(other.cue_ops);
        cue_type_ids.swap(other.cue_type_ids);
        runtime_cue_type_ids.swap(other.runtime_cue_type_ids);
        swap(fixed_dt_seconds, other.fixed_dt_seconds);
    }

    SyncRole role = SyncRole::Server;
    ClientId local_client = invalid_client_id;
    ashiato::Entity input_component;
    std::vector<SyncArchetype> archetypes;
    std::unordered_map<std::uint64_t, SyncComponentOps> component_ops;
    std::vector<SyncComponentOps> component_serializers;
    std::vector<SyncCueOps> cue_ops;
    std::unordered_map<std::type_index, SyncCueTypeId> cue_type_ids;
    std::unordered_map<std::string, SyncCueTypeId> runtime_cue_type_ids;
    double fixed_dt_seconds = 1.0 / 60.0;
};

struct QueuedSyncCueView {
    const QueuedSyncCue* data = nullptr;
    std::size_t size = 0;

    const QueuedSyncCue* begin() const noexcept { return data; }
    const QueuedSyncCue* end() const noexcept { return data == nullptr ? nullptr : data + size; }
    bool empty() const noexcept { return size == 0U; }
};

struct SyncAuthority {
    bool authoritative = true;

    bool is_authoritative() const noexcept {
        return authoritative;
    }
};

struct Replicated {
    SyncArchetypeId archetype;
};

struct NetworkOwner {
    ClientId client = invalid_client_id;
};

struct FractionalTickSampled {};
struct PredictiveSimulationJob {};
struct NoResim {};
struct NoSimulate {};

struct ReplicationBandwidthOptions {
    bool enabled = false;
    std::size_t min_bytes_per_second = std::size_t{8U} * 1024U;
    std::size_t initial_bytes_per_second = std::size_t{32U} * 1024U;
    std::size_t max_bytes_per_second = std::size_t{512U} * 1024U;
    std::size_t max_burst_bytes = 0;
    std::size_t transport_overhead_bytes_per_packet = 28U;
    SyncFrame sample_window_frames = 60;
    float loss_decrease_threshold = 0.02f;
    float rtt_inflation_decrease_threshold = 1.5f;
    float multiplicative_decrease = 0.75f;
    float additive_increase_bytes_per_second = 2400.0f;
};

using ReplicationBandwidthParticipantId = std::uint32_t;
inline constexpr ReplicationBandwidthParticipantId invalid_bandwidth_participant_id = 0;

struct ReplicationBandwidthParticipantOptions {
    std::size_t weight = 1;
    int priority = 0;
};

struct ReplicationServerOptions {
    std::size_t bandwidth_limit_bytes_per_tick = 1024;
    std::size_t mtu_bytes = 1200;
    ReplicationBandwidthOptions bandwidth;
    protocol::Descriptor protocol = protocol::default_descriptor;
    double fixed_dt_seconds = 1.0 / 60.0;
    double connect_resend_interval_seconds = 0.25;
    double idle_client_timeout_seconds = 0.0;
    std::size_t input_buffer_capacity_frames = 64;
    SyncFrame prioritizer_interval_frames = 4;
    // How many entity records one client's send loop may serialize and then refuse for
    // bandwidth in a single tick before it stops considering further candidates. A bound of
    // 0 and a bound of 1 both stop at the first refusal, because the loop has to serialize a
    // record to learn that it does not fit.
    //
    // The loop serializes a candidate -- quantize, delta-encode against the client's
    // baseline, retain the quantized frame -- before it can know whether the record fits
    // the remaining budget, and a record that does not fit is discarded. It keeps going on
    // purpose, so that a smaller record later in priority order can still fill the packet.
    // Under a tight budget that means server CPU per tick stays proportional to dirty
    // entities times clients while the bytes sent are capped, and almost all of that work
    // is thrown away.
    //
    // The default is unbounded, which is exactly the behaviour described above. Lowering it
    // trades packing for CPU: 0 stops at the first refusal, and a small bound keeps most of
    // the packing while capping the discarded work. A record refused for exceeding
    // mtu_bytes is not counted here, because that is a size refusal and not a budget one.
    std::size_t max_budget_refusals_per_client_tick = std::numeric_limits<std::size_t>::max();
    ReplicationPrioritizerFn prioritizer;
    ConnectHandlerFn connect_handler;
    TransportFn transport;
    ServerConnectionEventFn connection_event_handler;
    LoggingOptions logging;
    std::uint32_t max_fixed_steps_per_tick = 0;
#ifdef ASHIATO_SYNC_ENABLE_TRACING
    TraceOptions trace;
#endif
};

}  // namespace ashiato::sync

namespace ashiato {

template <>
struct is_singleton_component<ashiato::sync::SyncSettings> : std::true_type {};

template <>
struct is_singleton_component<ashiato::sync::FrameInfo> : std::true_type {};

template <>
struct is_singleton_component<ashiato::sync::CueDispatcher> : std::true_type {};

template <>
struct is_singleton_component<ashiato::sync::SyncAuthority> : std::true_type {};

}  // namespace ashiato
