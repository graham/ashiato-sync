#pragma once

#include "ashiato/sync/client_clock.hpp"
#include "ashiato/sync/components.hpp"
#include "ashiato/sync/logging.hpp"

#include <array>
#include <cstddef>
#include <cstdint>
#include <functional>
#include <limits>
#include <memory>
#include <stdexcept>
#include <string>
#include <type_traits>
#include <unordered_map>
#include <utility>
#include <vector>

namespace ashiato::sync {

class SyncTracer;
class KTraceDirectoryWriter;
class FractionalTickSampler;
enum class SyncTraceEventType : std::uint8_t;

namespace detail {
class BitReader;
struct FrameDataView;
}  // namespace detail

namespace client_detail {
class ClientAckQueue;
class ClientBufferedRuntime;
class ClientCueRuntime;
enum class FrameWriteSource : std::uint8_t;
class ClientEntityStore;
class ClientPredictionRuntime;
class ClientSessionTransport;
class ClientTimingStatsCalculator;
class ClientDisplaySampler;
class ClientUpdateRuntime;
struct EntityState;
struct EntityCue;
struct EntityPlayedCue;
struct EntityBufferedFrame;
struct EntityFrameView;
struct OriginalPredictionCapture;
struct WireNetworkIdState;
struct InputFrame;
struct PendingPing;
class ClientInputBuffer;
}  // namespace client_detail

struct ReplicatedComponentUpdate {
    ashiato::Entity component;
    SyncComponentSerializerId serializer = invalid_sync_component_serializer_id;
    SyncComponentOps::QuantizedBytes bytes;
};

struct ReplicatedEntityUpdateView {
    ClientEntityNetworkId client_entity_network_id = invalid_client_entity_network_id;
    ashiato::Entity local_entity;
    SyncArchetypeId archetype;
    SyncFrame frame = 0;
    std::uint64_t tag_mask = 0;

    template <typename T>
    bool try_get(const ashiato::Registry& registry, T& out) const {
        const ashiato::Entity component = registry.component<T>();
        return try_get(registry, component, &out);
    }

    bool try_get(const ashiato::Registry& registry, ashiato::Entity component, void* out) const;
    bool has_tag(const ashiato::Registry& registry, ashiato::Entity tag) const;

private:
    friend class ReplicationClient;
    friend class client_detail::ClientUpdateRuntime;

    const std::vector<ReplicatedComponentUpdate>* components = nullptr;
};

struct FractionalTickSample {
    enum class Source : std::uint8_t {
        Unknown,
        PreviousSample,
        LatestBuffered,
        Buffered,
        Predicted,
        Live
    };

    ClientEntityNetworkId client_entity_network_id = invalid_client_entity_network_id;
    ashiato::Entity local_entity;
    SyncFrame frame = 0;
    float alpha = 0.0f;
    Source source = Source::Unknown;
    ReplicationClientMode mode = ReplicationClientMode::Snap;
    double target_frame = 0.0;
    SyncFrame target_floor_frame = 0;
    float target_alpha = 0.0f;
    bool target_valid = false;
    bool floor_frame_present = false;
    bool next_frame_present = false;

    template <typename T>
    bool try_get_sampled_value(const ashiato::Registry& registry, T& out) const {
        const ashiato::Entity component = registry.component<T>();
        return try_get_sampled_value(registry, component, &out);
    }

    bool try_get_sampled_value(const ashiato::Registry& registry, ashiato::Entity component, void* out) const;

private:
    friend class ReplicationClient;
    friend class FractionalTickSampler;
    friend class client_detail::ClientDisplaySampler;

    std::vector<ReplicatedComponentUpdate> components_;
};

struct FractionalTickSampleBuffer {
    std::vector<FractionalTickSample> entities;

    void clear() {
        entities.clear();
    }
};

using EntityModeSelector = std::function<ReplicationClientMode(const ReplicatedEntityUpdateView&)>;

enum class ReplicationClientConnectionState {
    Disconnected,
    Connecting,
    Accepted,
    Ready,
    Rejected
};

struct ReplicationClientConnectionEvent {
    ReplicationClientConnectionState previous = ReplicationClientConnectionState::Disconnected;
    ReplicationClientConnectionState current = ReplicationClientConnectionState::Disconnected;
    ClientId client = invalid_client_id;
    std::string reason;
};

using ClientConnectionEventFn = std::function<void(const ReplicationClientConnectionEvent&)>;

enum class EntityReferenceStatus {
    Invalid,
    Pending,
    Alive,
    Destroyed
};

enum class ClientStatus {
    Ok,
    EntityNotFound,
    EntityUnavailable,
    ModeSwitchFailed,
    MissingPredictionRollbackTrait
};

class ClientError : public std::runtime_error {
public:
    ClientError(ClientStatus status, const char* message)
        : std::runtime_error(message), status_(status) {}

    ClientStatus status() const noexcept {
        return status_;
    }

private:
    ClientStatus status_;
};

struct ReplicationClientNetworkOptions {
    std::size_t mtu_bytes = 1200;
    protocol::Descriptor protocol = protocol::default_descriptor;
};

struct ReplicationClientEntityOptions {
    ReplicationClientMode default_mode = ReplicationClientMode::Snap;
    EntityModeSelector mode_selector;
};

struct ReplicationClientBufferedOptions {
    SyncFrame buffered_frame_lag = 2;
    bool auto_buffered_frame_lag = true;
    SyncFrame auto_buffered_frame_lag_min = 2;
    float auto_buffered_frame_lag_jitter_multiplier = 2.0f;
    float auto_buffered_frame_lag_smoothing = 0.1f;
    float auto_buffered_time_dilation_min = 0.95f;
    float auto_buffered_time_dilation_max = 1.05f;
    float auto_buffered_time_dilation_gain = 0.05f;
};

struct ReplicationClientPredictionOptions {
    ReplicationRollbackPolicy rollback_policy = ReplicationRollbackPolicy::All;
    std::size_t input_buffer_capacity_frames = 64;
    bool auto_lead_frames = true;
    SyncFrame lead_frames = 2;
    SyncFrame auto_min_frames = 1;
    SyncFrame auto_safety_frames = 1;
    float auto_jitter_multiplier = 2.0f;
    float auto_predicted_time_dilation_min = 0.95f;
    float auto_predicted_time_dilation_max = 1.10f;
    float auto_predicted_time_dilation_gain = 0.05f;
};

struct ReplicationClientRollbackPreparedEvent {
    // First frame in the rollback/resimulation window.
    SyncFrame rollback_frame = 0;
    // Last frame replayed by resimulation, inclusive.
    SyncFrame resim_end_frame = 0;
    ReplicationRollbackPolicy rollback_policy = ReplicationRollbackPolicy::All;
    std::vector<ashiato::Entity> resimulated_entities;
};

using ClientRollbackPreparedFn =
    std::function<void(ashiato::Registry&, const ReplicationClientRollbackPreparedEvent&)>;

// PredictedOnly's coupling rule. Called once per rollback, after every predicted entity is reset and before the first
// replayed frame, with the predicted local entities. Whatever buffered local entities it appends to `companions` are
// replayed with them: their buffered frame is re-applied on every replayed frame, as All re-applies every buffered
// entity, and the job graph runs over them too. A carrier with the predicted craft on its deck is the case it is for.
// Unset, or appending nothing, holds every buffered entity where the last tick left it.
using ClientPredictedOnlyCompanionsFn = std::function<void(
    ashiato::Registry&, const std::vector<ashiato::Entity>& predicted, std::vector<ashiato::Entity>& companions)>;

struct ReplicationClientSessionOptions {
    ClientId local_client = invalid_client_id;
    std::string connect_token;
    double connect_resend_interval_seconds = 0.25;
    double ping_interval_seconds = 3.0;
    double adaptive_ping_interval_seconds = 0.25;
    std::uint32_t adaptive_ping_stable_samples = 4;
    float adaptive_ping_stable_threshold_frames = 0.5f;
    float adaptive_ping_jump_threshold_frames = 3.0f;
};

struct ReplicationClientClockOptions {
    double fixed_dt_seconds = 1.0 / 60.0;
    std::uint32_t auto_timing_warmup_samples = 3;
    bool auto_timing_fast_recovery = true;
    SyncFrame auto_timing_fast_recovery_min_frame_gap = 4;
    std::uint32_t max_fixed_steps_per_tick = 0;
};

struct ReplicationClientOptions {
    ReplicationClientNetworkOptions network;
    ReplicationClientEntityOptions entities;
    ReplicationClientBufferedOptions buffered;
    ReplicationClientPredictionOptions prediction;
    ReplicationClientSessionOptions session;
    ReplicationClientClockOptions clock;
    ClientConnectionEventFn connection_event_handler;
    ClientRollbackPreparedFn rollback_prepared_handler;
    ClientPredictedOnlyCompanionsFn predicted_only_companions_handler;
    LoggingOptions logging;
#ifdef ASHIATO_SYNC_ENABLE_TRACING
    TraceOptions trace;
#endif
};

#ifdef ASHIATO_SYNC_ENABLE_INTERPOLATION_DIAGNOSTICS
inline constexpr std::size_t interpolation_diagnostics_window_frames = 120;

struct ReplicationClientInterpolationDiagnostics {
    struct FrameBucket {
        std::uint64_t checks = 0;
        std::uint64_t starvations = 0;
    };

    std::uint64_t total_interpolated_entity_frame_checks = 0;
    std::uint64_t total_interpolated_entity_frame_starvations = 0;
    std::uint64_t window_interpolated_entity_frame_checks = 0;
    std::uint64_t window_interpolated_entity_frame_starvations = 0;
    std::uint64_t sampled_interpolation_frames = 0;

    float interpolated_entity_starvation_rate() const noexcept {
        return window_interpolated_entity_frame_checks == 0
            ? 0.0f
            : static_cast<float>(window_interpolated_entity_frame_starvations) /
                static_cast<float>(window_interpolated_entity_frame_checks);
    }

    float interpolated_entity_starvation_percent() const noexcept {
        return interpolated_entity_starvation_rate() * 100.0f;
    }

    float lifetime_interpolated_entity_starvation_rate() const noexcept {
        return total_interpolated_entity_frame_checks == 0
            ? 0.0f
            : static_cast<float>(total_interpolated_entity_frame_starvations) /
                static_cast<float>(total_interpolated_entity_frame_checks);
    }

    float lifetime_interpolated_entity_starvation_percent() const noexcept {
        return lifetime_interpolated_entity_starvation_rate() * 100.0f;
    }

    void record_frame(std::uint64_t checks, std::uint64_t starvations) noexcept {
        if (checks == 0) {
            return;
        }

        total_interpolated_entity_frame_checks += checks;
        total_interpolated_entity_frame_starvations += starvations;
        ++sampled_interpolation_frames;

        FrameBucket& bucket = window_[window_next_];
        window_interpolated_entity_frame_checks -= bucket.checks;
        window_interpolated_entity_frame_starvations -= bucket.starvations;
        bucket.checks = checks;
        bucket.starvations = starvations;
        window_interpolated_entity_frame_checks += checks;
        window_interpolated_entity_frame_starvations += starvations;
        window_next_ = (window_next_ + 1U) % window_.size();
    }

private:
    std::array<FrameBucket, interpolation_diagnostics_window_frames> window_{};
    std::size_t window_next_ = 0;
};
#endif

namespace detail {

inline void register_predictive_simulation_job_tag(ashiato::Registry& registry) {
    registry.register_component<PredictiveSimulationJob>("ashiato.sync.PredictiveSimulationJob");
}

template <typename JobBuilder, bool IsCosmeticJob>
class ClientJobBuilder {
public:
    ClientJobBuilder(ashiato::Registry& registry, JobBuilder builder)
        : registry_(&registry), builder_(std::move(builder)) {}

    ClientJobBuilder& name(std::string value) {
        builder_.name(std::move(value));
        return *this;
    }

    ClientJobBuilder& max_threads(std::size_t count) {
        builder_.max_threads(count);
        return *this;
    }

    ClientJobBuilder& single_thread() {
        builder_.single_thread();
        return *this;
    }

    ClientJobBuilder& min_entities_per_thread(std::size_t count) {
        builder_.min_entities_per_thread(count);
        return *this;
    }

    template <typename Fn>
    ashiato::Entity each(Fn&& fn) {
        const ashiato::Entity job = builder_.each(std::forward<Fn>(fn));
        if constexpr (IsCosmeticJob) {
            registry_->template add<NoResim>(job);
        } else {
            register_predictive_simulation_job_tag(*registry_);
            registry_->template add<PredictiveSimulationJob>(job);
        }
        return job;
    }

    template <typename... AccessComponents>
    auto access_other_entities() const {
        return ClientJobBuilder<decltype(builder_.template access_other_entities<AccessComponents...>()), IsCosmeticJob>(
            *registry_,
            builder_.template access_other_entities<AccessComponents...>());
    }

    template <typename... OptionalComponents>
    auto optional() const {
        return ClientJobBuilder<decltype(builder_.template optional<OptionalComponents...>()), IsCosmeticJob>(
            *registry_,
            builder_.template optional<OptionalComponents...>());
    }

    template <typename... StructuralComponents>
    auto structural() const {
        return ClientJobBuilder<decltype(builder_.template structural<StructuralComponents...>()), IsCosmeticJob>(
            *registry_,
            builder_.template structural<StructuralComponents...>());
    }

    template <typename... Tags>
    auto with_tags() const {
        return ClientJobBuilder<decltype(builder_.template with_tags<Tags...>()), IsCosmeticJob>(
            *registry_,
            builder_.template with_tags<Tags...>());
    }

    template <typename... Tags>
    auto without_tags() const {
        return ClientJobBuilder<decltype(builder_.template without_tags<Tags...>()), IsCosmeticJob>(
            *registry_,
            builder_.template without_tags<Tags...>());
    }

    template <typename T>
    decltype(auto) get(ashiato::Entity entity) const {
        return builder_.template get<T>(entity);
    }

    template <typename T>
    decltype(auto) get() const {
        return builder_.template get<T>();
    }

    template <typename T>
    decltype(auto) write(ashiato::Entity entity) {
        return builder_.template write<T>(entity);
    }

    template <typename T>
    decltype(auto) write() {
        return builder_.template write<T>();
    }

private:
    ashiato::Registry* registry_;
    JobBuilder builder_;
};

}  // namespace detail

class ReplicationClient {
public:
    struct ObservabilityStats {
        std::uint64_t server_packet_warnings = 0;
        std::uint64_t suppressed_server_packet_warnings = 0;
        std::uint64_t client_errors = 0;
        std::uint64_t client_connects_accepted = 0;
        std::uint64_t client_connects_rejected = 0;
        // Input packets the MTU cut short of the protocol-sized window of newest unacknowledged frames,
        // and how many frames were cut.
        std::uint64_t input_packets_truncated = 0;
        std::uint64_t input_frames_truncated = 0;
    };

    static constexpr std::size_t buffered_frame_capacity = 64;
    static constexpr std::size_t prediction_frame_capacity = 64;

    explicit ReplicationClient(ashiato::Registry& registry, ReplicationClientOptions options = {});
    ~ReplicationClient();
    ReplicationClient(const ReplicationClient& other) = delete;
    ReplicationClient& operator=(const ReplicationClient& other) = delete;
    ReplicationClient(ReplicationClient&& other) noexcept;
    ReplicationClient& operator=(ReplicationClient&& other) noexcept;

    const ReplicationClientOptions& options() const noexcept {
        return options_;
    }
    double fixed_dt_seconds() const noexcept {
        return fixed_dt_seconds_;
    }
    void set_logger(std::shared_ptr<spdlog::logger> logger);
    void set_log_level(LogLevel level);
    LogLevel log_level() const noexcept {
        return options_.logging.level;
    }
#ifdef ASHIATO_SYNC_ENABLE_TRACING
    void set_tracer(SyncTracer* tracer) noexcept;
    void set_trace_options(TraceOptions options);
    void flush_trace();
    void close_trace();
#endif

    bool set_default_entity_mode(ReplicationClientMode mode) noexcept;
    void set_entity_mode(
        ashiato::Registry& registry,
        ClientEntityNetworkId client_entity_network_id,
        ReplicationClientMode mode);
    bool has_entity(ClientEntityNetworkId client_entity_network_id) const noexcept;
    ReplicationClientMode entity_mode(ClientEntityNetworkId client_entity_network_id) const noexcept;
    bool set_buffered_frame_lag(SyncFrame frames) noexcept;
    SyncFrame current_buffered_frame_lag() const noexcept;
    bool set_input_bytes(ashiato::Registry& registry, ashiato::Entity component, const void* input);
    template <typename T>
    bool set_input(ashiato::Registry& registry, const T& input) {
        const ashiato::Entity component = registry.template component<T>();
        const SyncSettings& settings = registry.template get<SyncSettings>();
        if (settings.input_component != component) {
            return false;
        }
        return set_input_bytes(registry, component, &input);
    }
    bool tick(ashiato::Registry& registry, double dt_seconds, const ashiato::RunJobsOptions& prediction_options = {});
    void set_packet_sender(std::function<void(const ashiato::BitBuffer&)> sender);
    void receive_packet(ashiato::BitBuffer packet);
    bool receive(ashiato::Registry& registry, ashiato::BitBuffer packet);
    template <typename... Components>
    auto cosmetic_job(ashiato::Registry& registry, int order) {
        auto builder = registry.template job<Components...>(order);
        return detail::ClientJobBuilder<decltype(builder), true>(registry, std::move(builder));
    }
    template <typename... Components>
    auto simulation_job(ashiato::Registry& registry, int order) {
        auto builder = registry.template job<Components...>(order).template without_tags<const NoSimulate>();
        return detail::ClientJobBuilder<decltype(builder), false>(registry, std::move(builder));
    }
    bool apply_frame(ashiato::Registry& registry, SyncFrame buffered_frame);
    bool sample_fractional_tick_frame(
        const ashiato::Registry& registry,
        double target_frame,
        FractionalTickSampleBuffer& out) const;
    const FractionalTickSampleBuffer& fractional_tick_frame(const ashiato::Registry& registry);
    std::vector<ashiato::BitBuffer> drain_packets();
    std::vector<ashiato::BitBuffer> drain_ack_packets();
    std::size_t pending_ack_count() const noexcept;
    bool is_alive_client_entity_network_id(ClientEntityNetworkId client_entity_network_id) const noexcept;
    ashiato::Entity local_entity(ClientEntityNetworkId client_entity_network_id) const;
    ClientEntityNetworkId client_entity_network_id(ashiato::Entity local) noexcept;
    EntityReferenceStatus resolve_entity_reference(EntityReference& reference) const noexcept;
    ClientId client_id() const noexcept {
        return client_id_;
    }
    ReplicationClientConnectionState connection_state() const noexcept;
    const std::string& connect_error() const noexcept;
    double local_time_seconds() const noexcept {
        return clock_.local_time_seconds();
    }
    SyncFrame buffered_frame() const noexcept {
        return clock_.buffered_frame();
    }
    double continuous_buffered_frame() const noexcept {
        return clock_.continuous_buffered_frame();
    }
    SyncFrame predicted_frame() const noexcept {
        return clock_.predicted_frame();
    }
    double continuous_predicted_frame() const noexcept {
        return clock_.continuous_predicted_frame();
    }
    double fractional_tick_target_frame() const noexcept {
        return clock_.display_target_frame();
    }
    bool has_applied_buffered_frame() const noexcept;
    SyncFrame last_applied_buffered_frame() const noexcept;
    double estimated_server_frame() const noexcept;
    double continuous_prediction_frames_ahead() const noexcept;
    double continuous_buffered_frames_behind() const noexcept;
    const ReplicationClientTimingStats& timing_stats() const noexcept {
        return clock_.stats();
    }
    ObservabilityStats observability_stats() const noexcept {
        return observability_stats_;
    }
#ifdef ASHIATO_SYNC_ENABLE_INTERPOLATION_DIAGNOSTICS
    const ReplicationClientInterpolationDiagnostics& interpolation_diagnostics() const noexcept {
        return interpolation_diagnostics_;
    }
    void reset_interpolation_diagnostics() noexcept {
        interpolation_diagnostics_ = {};
    }
#endif

protected:
    struct FrameHistoryCapacities {
        std::size_t buffered = buffered_frame_capacity;
        std::size_t predicted = prediction_frame_capacity;
    };

    ReplicationClient(ashiato::Registry& registry, ReplicationClientOptions options, FrameHistoryCapacities capacities);

private:
    friend class client_detail::ClientBufferedRuntime;
    friend class client_detail::ClientCueRuntime;
    friend class client_detail::ClientPredictionRuntime;
    friend class client_detail::ClientUpdateRuntime;

    static constexpr std::size_t invalid_ack_index = std::numeric_limits<std::size_t>::max();
    static constexpr std::uint32_t invalid_entity_index = std::numeric_limits<std::uint32_t>::max();

    using EntityState = client_detail::EntityState;
    using EntityCue = client_detail::EntityCue;
    using EntityPlayedCue = client_detail::EntityPlayedCue;
    using EntityBufferedFrame = client_detail::EntityBufferedFrame;
    using OriginalPredictionCapture = client_detail::OriginalPredictionCapture;
    struct RegistryFrameApplyInfo;
    struct ReceiveContext {
        double local_time_seconds = 0.0;
        double estimated_server_time_seconds = 0.0;
        double estimated_server_frame = 0.0;
        SyncFrame buffered_frame = 0;
        double continuous_buffered_frame = 0.0;
        double continuous_predicted_frame = 0.0;
    };

    EntityState* find_entity_state(ClientEntityNetworkId client_entity_network_id) noexcept;
    const EntityState* find_entity_state(ClientEntityNetworkId client_entity_network_id) const noexcept;
    EntityState* find_entity_state_by_wire_id(std::uint32_t wire_network_id) noexcept;
    const EntityState* find_entity_state_by_wire_id(std::uint32_t wire_network_id) const noexcept;
    EntityState* find_entity_state_for_local(ashiato::Entity local) noexcept;
    void register_local_entity_index(const EntityState& state);
    void unregister_local_entity_index(const EntityState& state);
    EntityState* ensure_entity_state(
        ashiato::Registry& registry,
        ClientEntityNetworkId client_entity_network_id,
        std::uint32_t wire_network_id);
    void erase_entity_state(ashiato::Registry& registry, std::uint32_t entity_index, bool destroy_local);
    void sync_entity_memberships(EntityState& state);
    bool destroy_tombstone_blocks(std::uint32_t wire_network_id, SyncFrame frame) const;
    void record_destroy_tombstone(std::uint32_t wire_network_id, SyncFrame frame);
    const QuantizedFrameData* find_baseline(const EntityState& state, SyncFrame frame) const noexcept;
    bool validate_buffered_archetype(const SyncSettings& settings, SyncArchetypeId archetype) const;
    bool ensure_local_entity(ashiato::Registry& registry, EntityState& state);
    std::uint64_t registry_tag_mask(
        const ashiato::Registry& registry,
        ashiato::Entity entity,
        const SyncArchetype& archetype) const;
    void trace_applied_tag_delta(
        const SyncArchetype& archetype,
        const EntityState& state,
        SyncFrame frame,
        std::uint64_t previous_tag_mask,
        std::uint64_t next_tag_mask);
    bool apply_registry_tags(
        ashiato::Registry& registry,
        const SyncArchetype& archetype,
        EntityState& state,
        SyncFrame frame,
        std::uint64_t tag_mask,
        bool verify_tag_apply);
    bool remove_missing_registry_components(
        ashiato::Registry& registry,
        const SyncArchetype& archetype,
        const EntityState& state,
        SyncFrame frame,
        std::uint64_t previous_present_mask,
        std::uint64_t next_present_mask);
    bool apply_registry_frame(
        ashiato::Registry& registry,
        const SyncArchetype& archetype,
        EntityState& state,
        const RegistryFrameApplyInfo& input);
    bool fill_buffered_frames(
        const SyncSettings& settings,
        EntityState& state,
        SyncFrame frame,
        bool entity_present,
        QuantizedFrameData& decoded);
    bool write_buffered_frame(
        const SyncSettings& settings,
        EntityState& state,
        SyncFrame frame,
        bool entity_present,
        const QuantizedFrameData* from,
        const QuantizedFrameData* to,
        SyncFrame from_frame,
        SyncFrame to_frame);
    bool apply_buffered_sample(
        ashiato::Registry& registry,
        const SyncSettings& settings,
        EntityState& state,
        const client_detail::EntityFrameView& sample);
    bool apply_frame_data(
        ashiato::Registry& registry,
        const SyncSettings& settings,
        EntityState& state,
        SyncFrame frame,
        bool entity_present,
        const QuantizedFrameData& baseline);
    bool apply_snap_sample(
        ashiato::Registry& registry,
        const SyncSettings& settings,
        EntityState& state,
        const QuantizedFrameData& authoritative,
        std::uint64_t component_apply_mask);
    bool validate_predicted_archetype(const SyncSettings& settings, SyncArchetypeId archetype) const;
    bool quantize_predicted_entity(
        const ashiato::Registry& registry,
        const SyncSettings& settings,
        EntityState& state,
        SyncFrame frame,
        client_detail::FrameWriteSource source);
    void apply_buffered_frames_to_ashiato(
        ashiato::Registry& registry,
        const ReplicationClientClock::FrameRange& frames);
    bool run_predicted_frames(
        ashiato::Registry& registry,
        const ReplicationClientClock::FrameRange& frames,
        const ashiato::RunJobsOptions& options);
#ifdef ASHIATO_SYNC_ENABLE_TRACING
    void trace_local_time_frames(ashiato::Registry& registry, const ReplicationClientClock::FrameRange& frames);
#endif
    bool compare_predicted_frame(
        const SyncSettings& settings,
        EntityState& state,
        SyncFrame frame,
        const QuantizedFrameData& authoritative) const;
    const ashiato::JobGraph& resim_job_graph(ashiato::Registry& registry);
    bool apply_latest_snap(ashiato::Registry& registry, const SyncSettings& settings, EntityState& state);
    bool switch_entity_mode(
        ashiato::Registry& registry,
        const SyncSettings& settings,
        EntityState& state,
        ReplicationClientMode mode);
    enum class BaselineUpdate {
        KeepExisting,
        ReplaceAndApplyMask
    };
    void mark_mode_auto_selected(EntityState& state, ReplicationClientMode mode) noexcept;
    void mark_mode_user_selected(EntityState& state, ReplicationClientMode mode) noexcept;
    void reset_absent_entity_state(EntityState& state, SyncArchetypeId archetype) noexcept;
    void record_authoritative_present(
        EntityState& state,
        SyncFrame frame,
        SyncArchetypeId archetype,
        QuantizedFrameData baseline,
        BaselineUpdate baseline_update);
    void record_authoritative_absent(EntityState& state, SyncFrame frame);
    void protect_referenced_baseline(EntityState& state, SyncFrame frame) noexcept;
    bool transition_to_snap(ashiato::Registry& registry, const SyncSettings& settings, EntityState& state);
    bool transition_to_buffered(const SyncSettings& settings, EntityState& state);
    bool transition_to_predict(ashiato::Registry& registry, const SyncSettings& settings, EntityState& state);
    void finish_immediate_mode_transition(
        ashiato::Registry& registry,
        const SyncSettings& settings,
        EntityState& state,
        ReplicationClientMode previous_mode);
    bool has_buffered_entities() const noexcept;
    bool has_predicted_entities() const noexcept;
    void blend_snap_errors(const SyncSettings& settings, float dt_seconds);
#ifdef ASHIATO_SYNC_ENABLE_INTERPOLATION_DIAGNOSTICS
    void record_interpolation_frame(std::uint64_t checks, std::uint64_t starvations) noexcept;
#endif
    void remember_baseline(EntityState& state);
    void queue_ack(std::uint32_t packet_id);
    bool receive_connect_response(ashiato::Registry& registry, ashiato::BitBuffer& packet);
    bool receive_pong(ashiato::Registry& registry, ashiato::BitBuffer& packet, const ReceiveContext& context);
    bool receive_entity_update(ashiato::Registry& registry, ashiato::BitBuffer& packet, const ReceiveContext& context);
    bool update_prediction_input_prefill_from_entity_update(
        ashiato::Registry& registry,
        SyncFrame server_frame,
        const ReceiveContext& context);
    bool apply_prediction_input_prefill(
        ashiato::Registry& registry,
        SyncFrame server_frame,
        SyncFrame prefill_input_frame,
        SyncFrame prediction_snap_lead_frames);
    void drain_connect_packets(std::vector<ashiato::BitBuffer>& packets);
    void drain_ping_packets(std::vector<ashiato::BitBuffer>& packets);
    void drain_ack_packets_into(std::vector<ashiato::BitBuffer>& packets);
    void drain_input_packets_into(std::vector<ashiato::BitBuffer>& packets);
    void report_input_truncation(std::uint64_t truncated_packets_before);
    void process_inbound_packets(ashiato::Registry& registry);
    void send_pending_packets();
    void log_info(const char* event, const std::string& fields) const;
    void log_server_packet_warning(std::uint8_t message, const char* reason_code, const char* reason_detail);
    void log_client_error(std::uint8_t message, const char* event, const char* reason);
    ClientEntityNetworkId client_entity_network_id_for_wire(std::uint32_t wire_network_id);
    void advance_wire_network_id_version(std::uint32_t wire_network_id);
#ifdef ASHIATO_SYNC_ENABLE_TRACING
    enum class TraceFrameComponentScope {
        All,
        NonPredicted,
        Predicted
    };

    void trace_frame_components(
        const ashiato::Registry& registry,
        const SyncSettings& settings,
        SyncFrame frame,
        bool resimulated = false,
        bool only_pending_rollback = false,
        TraceFrameComponentScope scope = TraceFrameComponentScope::All);
    void trace_input_components(
        ashiato::Registry& registry,
        const SyncSettings& settings,
        SyncFrame frame,
        ashiato::Entity component,
        const std::uint8_t* quantized);
    void trace_clock_skew(
        const char* stage,
        SyncFrame local_frame,
        SyncFrame server_frame,
        double observed_server_frame,
        SyncFrame observed_buffered_frame,
        SyncFrame last_recorded_input_frame,
        SyncFrame prefill_input_frame) const;
    void trace_cue_event(
        SyncTraceEventType type,
        const SyncSettings& settings,
        const EntityState& state,
        const EntityCue& cue,
        const char* rollback_reason = nullptr,
        const char* cue_source = nullptr) const;
    void trace_cue_event(
        SyncTraceEventType type,
        const SyncSettings& settings,
        const EntityState& state,
        const EntityPlayedCue& cue,
        const char* rollback_reason = nullptr,
        const char* cue_source = nullptr) const;
#ifdef ASHIATO_SYNC_TRACE_PACKET_LOGS
    void trace_outgoing_ack_packet(const std::vector<std::uint32_t>& acks) const;
    void trace_outgoing_ping_packet(std::uint32_t sequence) const;
    void trace_incoming_pong_packet(
        std::uint32_t sequence,
        SyncFrame local_frame,
        SyncFrame server_receive_frame,
        SyncFrame server_send_frame) const;
    void trace_outgoing_input_packet(
        const std::vector<std::uint32_t>& acks,
        SyncFrame baseline_frame,
        SyncFrame first_input_frame,
        SyncFrame last_input_frame) const;
    void trace_incoming_update_packet(
        SyncFrame local_frame,
        SyncFrame server_frame,
        std::uint32_t packet_id,
        SyncFrame input_ack_frame,
        std::uint16_t record_count,
        bool applied,
        const char* apply_failure_reason) const;
#endif
#endif
    void set_client_id(ashiato::Registry& registry, ClientId client);
    void set_connection_state(
        ReplicationClientConnectionState state,
        ClientId client = invalid_client_id,
        std::string reason = {});
    bool record_input_frame(ashiato::Registry& registry, const SyncSettings& settings, SyncFrame frame);
    bool fill_input_frames_through(ashiato::Registry& registry, const SyncSettings& settings, SyncFrame frame);
    bool apply_input_frame(ashiato::Registry& registry, const SyncSettings& settings, SyncFrame frame);

    ReplicationClientOptions options_;
    double fixed_dt_seconds_ = 1.0 / 60.0;
    ReplicationClientClock clock_;
#ifdef ASHIATO_SYNC_ENABLE_INTERPOLATION_DIAGNOSTICS
    ReplicationClientInterpolationDiagnostics interpolation_diagnostics_;
#endif
    std::vector<ashiato::Entity> resim_job_graph_jobs_;
    ashiato::JobGraph resim_job_graph_;
    bool resim_job_graph_valid_ = false;
    std::unique_ptr<client_detail::ClientEntityStore> entity_store_;
    std::unique_ptr<client_detail::ClientPredictionRuntime> prediction_;
    std::unique_ptr<client_detail::ClientSessionTransport> session_transport_;
    std::unique_ptr<client_detail::ClientCueRuntime> cue_runtime_;
    std::unique_ptr<client_detail::ClientBufferedRuntime> buffered_runtime_;
    std::unique_ptr<client_detail::ClientAckQueue> ack_queue_;
    std::unique_ptr<client_detail::ClientTimingStatsCalculator> timing_stats_;
    std::unique_ptr<client_detail::ClientUpdateRuntime> update_runtime_;
    std::unique_ptr<client_detail::ClientInputBuffer> input_;
    std::shared_ptr<spdlog::logger> logger_;
    ObservabilityStats observability_stats_;
    std::unordered_map<std::uint8_t, std::uint32_t> warning_logs_by_message_;
    SyncFrame last_server_update_frame_ = 0;
    bool has_received_server_update_ = false;
    FractionalTickSampleBuffer fractional_tick_frame_;
    FractionalTickSampleBuffer fractional_tick_scratch_;
    ClientId client_id_ = invalid_client_id;
#ifdef ASHIATO_SYNC_ENABLE_TRACING
    SyncTracer* tracer_ = nullptr;
    std::unique_ptr<KTraceDirectoryWriter> trace_writer_;
#endif
};

enum class SimulationJobEndpointRole {
    DedicatedClient,
    DedicatedServer,
    ListenServer
};

enum class SimulationJobKind {
    Shared,
    ClientOnly,
    ServerOnly
};

struct JobRunContext {
    SimulationJobEndpointRole endpoint_role = SimulationJobEndpointRole::DedicatedClient;
    SimulationJobKind job_kind = SimulationJobKind::Shared;

    bool is_dedicated_client() const noexcept {
        return endpoint_role == SimulationJobEndpointRole::DedicatedClient;
    }

    bool is_dedicated_server() const noexcept {
        return endpoint_role == SimulationJobEndpointRole::DedicatedServer;
    }

    bool is_listen_server() const noexcept {
        return endpoint_role == SimulationJobEndpointRole::ListenServer;
    }

    bool is_authority() const noexcept {
        return endpoint_role != SimulationJobEndpointRole::DedicatedClient;
    }

    bool is_predicting_client() const noexcept {
        return endpoint_role == SimulationJobEndpointRole::DedicatedClient && job_kind == SimulationJobKind::Shared;
    }

    bool is_client_only() const noexcept {
        return job_kind == SimulationJobKind::ClientOnly;
    }

    bool is_server_only() const noexcept {
        return job_kind == SimulationJobKind::ServerOnly;
    }
};

namespace detail {

template <typename Fn, typename... Args>
void invoke_simulation_job_callback(Fn& fn, const JobRunContext& context, ashiato::Entity entity, Args&&... args) {
    if constexpr (std::is_invocable<Fn&, const JobRunContext&, ashiato::Entity, Args&&...>::value) {
        fn(context, entity, std::forward<Args>(args)...);
    } else {
        fn(entity, std::forward<Args>(args)...);
    }
}

template <typename Fn, typename StructuralContext, typename... Args>
void invoke_structural_simulation_job_callback(
    Fn& fn,
    const JobRunContext& context,
    StructuralContext& structural_context,
    ashiato::Entity entity,
    Args&&... args) {
    if constexpr (std::is_invocable<Fn&, StructuralContext&, const JobRunContext&, ashiato::Entity, Args&&...>::value) {
        fn(structural_context, context, entity, std::forward<Args>(args)...);
    } else {
        fn(structural_context, entity, std::forward<Args>(args)...);
    }
}

template <typename Fn, typename ViewType, typename... Args>
void invoke_view_simulation_job_callback(
    Fn& fn,
    ViewType& view,
    const JobRunContext& context,
    ashiato::Entity entity,
    Args&&... args) {
    if constexpr (std::is_invocable<Fn&, ViewType&, const JobRunContext&, ashiato::Entity, Args&&...>::value) {
        fn(view, context, entity, std::forward<Args>(args)...);
    } else if constexpr (std::is_invocable<Fn&, ViewType&, ashiato::Entity, Args&&...>::value) {
        fn(view, entity, std::forward<Args>(args)...);
    } else {
        invoke_simulation_job_callback(fn, context, entity, std::forward<Args>(args)...);
    }
}

template <typename Fn, typename First, typename... Rest>
void invoke_simulation_job_callback(Fn& fn, const JobRunContext& context, First&& first, Rest&&... rest) {
    using FirstType = typename std::decay<First>::type;
    if constexpr (std::is_same<FirstType, ashiato::Entity>::value) {
        invoke_simulation_job_callback(fn, context, first, std::forward<Rest>(rest)...);
    } else {
        invoke_structural_simulation_job_callback(fn, context, first, std::forward<Rest>(rest)...);
    }
}

template <typename Callback>
struct SimulationJobCallbackAdapter {
    std::shared_ptr<Callback> callback;
    JobRunContext context;

    template <typename... Args>
    void operator()(ashiato::Entity entity, Args&... args) {
        invoke_simulation_job_callback(*callback, context, entity, args...);
    }

    template <typename ViewType, typename... Args>
    void operator()(ViewType& view, ashiato::Entity entity, Args&... args) {
        invoke_view_simulation_job_callback(*callback, view, context, entity, args...);
    }

    template <bool DispatchHooks, bool AnyEntity, typename ViewType, typename... StructuralComponents, typename... Args>
    void operator()(
        ashiato::Registry::JobStructuralContext<DispatchHooks, AnyEntity, ViewType, StructuralComponents...>&
            structural_context,
        ashiato::Entity entity,
        Args&... args) {
        invoke_structural_simulation_job_callback(*callback, context, structural_context, entity, args...);
    }
};

}  // namespace detail

template <typename... Components>
struct SimulationJobBaseFactory {
    static constexpr bool uses_access = false;

    auto make_authority(ashiato::Registry& registry, int order) const {
        return registry.template job<Components...>(order);
    }

    auto make_predictive(ashiato::Registry& registry, int order) const {
        return registry.template job<Components...>(order).template without_tags<const NoSimulate>();
    }
};

template <typename PreviousFactory, typename... Tags>
struct SimulationJobWithTagsFactory {
    static constexpr bool uses_access = PreviousFactory::uses_access;

    PreviousFactory previous;

    auto make_authority(ashiato::Registry& registry, int order) const {
        return previous.make_authority(registry, order).template with_tags<Tags...>();
    }

    auto make_predictive(ashiato::Registry& registry, int order) const {
        return previous.make_predictive(registry, order).template with_tags<Tags...>();
    }
};

template <typename PreviousFactory, typename... Tags>
struct SimulationJobWithoutTagsFactory {
    static constexpr bool uses_access = PreviousFactory::uses_access;

    PreviousFactory previous;

    auto make_authority(ashiato::Registry& registry, int order) const {
        return previous.make_authority(registry, order).template without_tags<Tags...>();
    }

    auto make_predictive(ashiato::Registry& registry, int order) const {
        return previous.make_predictive(registry, order).template without_tags<Tags...>();
    }
};

template <typename PreviousFactory, typename... AccessComponents>
struct SimulationJobAccessFactory {
    static constexpr bool uses_access = true;

    PreviousFactory previous;

    auto make_authority(ashiato::Registry& registry, int order) const {
        auto builder = previous.make_authority(registry, order);
        builder.single_thread();
        return builder.template access_other_entities<AccessComponents...>();
    }

    auto make_predictive(ashiato::Registry& registry, int order) const {
        auto builder = previous.make_predictive(registry, order);
        builder.single_thread();
        return builder.template access_other_entities<AccessComponents...>();
    }
};

template <typename PreviousFactory, typename... OptionalComponents>
struct SimulationJobOptionalFactory {
    static constexpr bool uses_access = PreviousFactory::uses_access;

    PreviousFactory previous;

    auto make_authority(ashiato::Registry& registry, int order) const {
        return previous.make_authority(registry, order).template optional<OptionalComponents...>();
    }

    auto make_predictive(ashiato::Registry& registry, int order) const {
        return previous.make_predictive(registry, order).template optional<OptionalComponents...>();
    }
};

template <typename PreviousFactory, typename... StructuralComponents>
struct SimulationJobStructuralFactory {
    static constexpr bool uses_access = PreviousFactory::uses_access;

    PreviousFactory previous;

    auto make_authority(ashiato::Registry& registry, int order) const {
        return previous.make_authority(registry, order).template structural<StructuralComponents...>();
    }

    auto make_predictive(ashiato::Registry& registry, int order) const {
        return previous.make_predictive(registry, order).template structural<StructuralComponents...>();
    }
};

template <typename Builder, typename = void>
struct has_job_builder_name : std::false_type {};

template <typename Builder>
struct has_job_builder_name<
    Builder,
    typename std::enable_if<std::is_same<
        decltype(std::declval<Builder&>().name(std::declval<std::string>())),
        Builder&>::value>::type> : std::true_type {};

template <typename Builder, typename = void>
struct has_job_builder_max_threads : std::false_type {};

template <typename Builder>
struct has_job_builder_max_threads<
    Builder,
    typename std::enable_if<std::is_same<
        decltype(std::declval<Builder&>().max_threads(std::declval<std::size_t>())),
        Builder&>::value>::type> : std::true_type {};

template <typename Builder, typename = void>
struct has_job_builder_single_thread : std::false_type {};

template <typename Builder>
struct has_job_builder_single_thread<
    Builder,
    typename std::enable_if<std::is_same<
        decltype(std::declval<Builder&>().single_thread()),
        Builder&>::value>::type> : std::true_type {};

template <typename Builder, typename = void>
struct has_job_builder_min_entities_per_thread : std::false_type {};

template <typename Builder>
struct has_job_builder_min_entities_per_thread<
    Builder,
    typename std::enable_if<std::is_same<
        decltype(std::declval<Builder&>().min_entities_per_thread(std::declval<std::size_t>())),
        Builder&>::value>::type> : std::true_type {};

template <typename Factory>
class SimulationJobBuilder {
public:
    SimulationJobBuilder(
        ashiato::Registry& registry,
        SimulationJobEndpointRole role,
        SimulationJobKind kind,
        int order,
        Factory factory = {})
        : registry_(&registry), context_{role, kind}, order_(order), factory_(std::move(factory)) {}

    SimulationJobBuilder& name(std::string value) {
        name_ = std::move(value);
        return *this;
    }

    SimulationJobBuilder& max_threads(std::size_t count) {
        max_threads_ = count;
        single_thread_ = false;
        has_threading_option_ = true;
        return *this;
    }

    SimulationJobBuilder& single_thread() {
        single_thread_ = true;
        has_threading_option_ = true;
        return *this;
    }

    SimulationJobBuilder& min_entities_per_thread(std::size_t count) {
        min_entities_per_thread_ = count;
        return *this;
    }

    template <typename... Tags>
    auto with_tags() const {
        return rebind(SimulationJobWithTagsFactory<Factory, Tags...>{factory_});
    }

    template <typename... Tags>
    auto without_tags() const {
        return rebind(SimulationJobWithoutTagsFactory<Factory, Tags...>{factory_});
    }

    template <typename... AccessComponents>
    auto access_other_entities() const {
        return rebind(SimulationJobAccessFactory<Factory, AccessComponents...>{factory_});
    }

    template <typename... OptionalComponents>
    auto optional() const {
        return rebind(SimulationJobOptionalFactory<Factory, OptionalComponents...>{factory_});
    }

    template <typename... StructuralComponents>
    auto structural() const {
        return rebind(SimulationJobStructuralFactory<Factory, StructuralComponents...>{factory_});
    }

    template <typename Fn>
    ashiato::Entity each(Fn&& fn) {
        if (!should_register()) {
            return {};
        }

        using Callback = typename std::decay<Fn>::type;
        auto callback = std::make_shared<Callback>(std::forward<Fn>(fn));
        const JobRunContext context = context_;
        detail::SimulationJobCallbackAdapter<Callback> wrapped{callback, context};

        if (context_.job_kind == SimulationJobKind::Shared && context_.endpoint_role == SimulationJobEndpointRole::DedicatedClient) {
            detail::register_predictive_simulation_job_tag(*registry_);
            auto builder = factory_.make_predictive(*registry_, order_);
            apply_options(builder);
            const ashiato::Entity job = builder.each(std::move(wrapped));
            registry_->template add<PredictiveSimulationJob>(job);
            return job;
        }

        auto builder = factory_.make_authority(*registry_, order_);
        apply_options(builder);
        const ashiato::Entity job = builder.each(std::move(wrapped));
        if (context_.job_kind == SimulationJobKind::ClientOnly) {
            registry_->template add<NoResim>(job);
        }
        return job;
    }

private:
    bool should_register() const noexcept {
        switch (context_.job_kind) {
        case SimulationJobKind::Shared:
            return true;
        case SimulationJobKind::ClientOnly:
            return context_.endpoint_role != SimulationJobEndpointRole::DedicatedServer;
        case SimulationJobKind::ServerOnly:
            return context_.endpoint_role != SimulationJobEndpointRole::DedicatedClient;
        }
        return false;
    }

    template <typename Builder>
    void apply_options(Builder& builder) const {
        if constexpr (has_job_builder_name<Builder>::value) {
            if (!name_.empty()) {
                builder.name(name_);
            }
        }
        if constexpr (Factory::uses_access) {
            if constexpr (has_job_builder_single_thread<Builder>::value) {
                builder.single_thread();
            }
        } else if constexpr (has_job_builder_single_thread<Builder>::value && has_job_builder_max_threads<Builder>::value) {
            if (has_threading_option_) {
                if (single_thread_) {
                    builder.single_thread();
                } else {
                    builder.max_threads(max_threads_);
                }
            }
        } else if constexpr (has_job_builder_single_thread<Builder>::value) {
            if (has_threading_option_ && single_thread_) {
                builder.single_thread();
            }
        } else if constexpr (has_job_builder_max_threads<Builder>::value) {
            if (has_threading_option_ && !single_thread_) {
                builder.max_threads(max_threads_);
            }
        }
        if constexpr (has_job_builder_min_entities_per_thread<Builder>::value) {
            if (min_entities_per_thread_ > 0U) {
                builder.min_entities_per_thread(min_entities_per_thread_);
            }
        }
    }

    template <typename NextFactory>
    SimulationJobBuilder<NextFactory> rebind(NextFactory factory) const {
        SimulationJobBuilder<NextFactory> next(*registry_, context_.endpoint_role, context_.job_kind, order_, std::move(factory));
        next.name_ = name_;
        next.max_threads_ = max_threads_;
        next.min_entities_per_thread_ = min_entities_per_thread_;
        next.single_thread_ = single_thread_;
        next.has_threading_option_ = has_threading_option_;
        return next;
    }

    template <typename>
    friend class SimulationJobBuilder;

    ashiato::Registry* registry_;
    JobRunContext context_;
    int order_ = 0;
    Factory factory_;
    std::string name_;
    std::size_t max_threads_ = 1;
    std::size_t min_entities_per_thread_ = 0;
    bool single_thread_ = false;
    bool has_threading_option_ = false;
};
class SimulationJobs {
public:
    SimulationJobs(ashiato::Registry& registry, SimulationJobEndpointRole role)
        : registry_(&registry), role_(role) {
        register_components(registry);
    }

    ashiato::Registry& registry() const noexcept {
        return *registry_;
    }

    SimulationJobEndpointRole endpoint_role() const noexcept {
        return role_;
    }

    bool is_dedicated_client() const noexcept {
        return role_ == SimulationJobEndpointRole::DedicatedClient;
    }

    bool is_authority() const noexcept {
        return role_ != SimulationJobEndpointRole::DedicatedClient;
    }

    template <typename... Components>
    SimulationJobBuilder<SimulationJobBaseFactory<Components...>> shared(int order) {
        return SimulationJobBuilder<SimulationJobBaseFactory<Components...>>(
            *registry_,
            role_,
            SimulationJobKind::Shared,
            order);
    }

    template <typename... Components>
    SimulationJobBuilder<SimulationJobBaseFactory<Components...>> client_only(int order) {
        return SimulationJobBuilder<SimulationJobBaseFactory<Components...>>(
            *registry_,
            role_,
            SimulationJobKind::ClientOnly,
            order);
    }

    template <typename... Components>
    SimulationJobBuilder<SimulationJobBaseFactory<Components...>> server_only(int order) {
        return SimulationJobBuilder<SimulationJobBaseFactory<Components...>>(
            *registry_,
            role_,
            SimulationJobKind::ServerOnly,
            order);
    }

private:
    ashiato::Registry* registry_;
    SimulationJobEndpointRole role_;
};

namespace detail {

constexpr bool client_frame_capacity_power_of_two(std::size_t value) noexcept {
    return value != 0U && (value & (value - 1U)) == 0U;
}

}  // namespace detail

template <
    std::size_t NetworkEntityIdTier0Bits = protocol::default_network_entity_id_tier0_bits,
    std::size_t BufferedFrameCapacity = ReplicationClient::buffered_frame_capacity,
    std::size_t PredictionFrameCapacity = ReplicationClient::prediction_frame_capacity>
class ReplicationClientT : public ReplicationClient {
    static_assert(
        protocol::valid_network_entity_id_tier0_bits(NetworkEntityIdTier0Bits),
        "NetworkEntityIdTier0Bits must be in [1, 22]");
    static_assert(
        detail::client_frame_capacity_power_of_two(BufferedFrameCapacity),
        "BufferedFrameCapacity must be a nonzero power of two");
    static_assert(
        detail::client_frame_capacity_power_of_two(PredictionFrameCapacity),
        "PredictionFrameCapacity must be a nonzero power of two");

public:
    static constexpr std::size_t network_entity_id_tier0_bits = NetworkEntityIdTier0Bits;

    explicit ReplicationClientT(ashiato::Registry& registry, ReplicationClientOptions options = {})
        : ReplicationClient(
              registry,
              configure(std::move(options)),
              FrameHistoryCapacities{BufferedFrameCapacity, PredictionFrameCapacity}) {}

private:
    static ReplicationClientOptions configure(ReplicationClientOptions options) {
        options.network.protocol.network_entity_id_tier0_bits = NetworkEntityIdTier0Bits;
        return options;
    }
};

}  // namespace ashiato::sync
