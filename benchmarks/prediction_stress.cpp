#include "ashiato/sync/sync.hpp"

#include <algorithm>
#include <chrono>
#include <cmath>
#include <cstdint>
#include <exception>
#include <iomanip>
#include <iostream>
#include <limits>
#include <random>
#include <stdexcept>
#include <string>
#include <vector>

namespace ashiato::sync::prediction_stress {

struct PredPosition {
    float x = 0.0f;
    float y = 0.0f;
};

struct PredVelocity {
    float x = 0.0f;
    float y = 0.0f;
};

struct PredAcceleration {
    float x = 0.0f;
    float y = 0.0f;
};

struct PredEnergy {
    std::int32_t value = 0;
};

struct PredFlags {
    std::uint32_t bits = 0;
};

struct Schema {
    SyncArchetypeId actor;
};

struct Config {
    std::uint32_t entities = 2048;
    std::uint32_t ticks = 1800;
    std::uint32_t latency_frames = 10;
    double misprediction_percent = 5.0;
    std::uint32_t seed = 0xC0FFEEU;
    ReplicationRollbackPolicy rollback_policy = ReplicationRollbackPolicy::All;
    bool json = false;
};

struct Timing {
    double server_simulation_seconds = 0.0;
    double server_replication_seconds = 0.0;
    double client_receive_seconds = 0.0;
    double client_tick_seconds = 0.0;
    double ack_processing_seconds = 0.0;
    double wall_seconds = 0.0;
};

struct Report {
    Config config;
    Timing timing;
    std::uint64_t server_packets = 0;
    std::uint64_t server_bytes = 0;
    std::uint64_t client_packets = 0;
    std::uint64_t client_bytes = 0;
    std::uint64_t misprediction_events = 0;
    std::uint64_t delivered_server_packets = 0;
    std::uint64_t predicted_entities = 0;
};

class ScopedTimer {
public:
    explicit ScopedTimer(double& out)
        : out_(&out), begin_(std::chrono::steady_clock::now()) {}

    ~ScopedTimer() {
        const auto end = std::chrono::steady_clock::now();
        *out_ += std::chrono::duration<double>(end - begin_).count();
    }

private:
    double* out_;
    std::chrono::steady_clock::time_point begin_;
};

}  // namespace ashiato::sync::prediction_stress

namespace ashiato::sync {

template <>
struct SyncComponentTraits<prediction_stress::PredPosition> {
    using Quantized = prediction_stress::PredPosition;
    using Error = prediction_stress::PredPosition;

    static void quantize(const prediction_stress::PredPosition& value, Quantized& out) {
        out = value;
    }

    static prediction_stress::PredPosition dequantize(const Quantized& value) {
        return value;
    }

    static void serialize(const Quantized*, const Quantized& current, ashiato::BitBuffer& out, ashiato::ComponentSerializationContext&) {
        out.write_bytes(reinterpret_cast<const char*>(&current), sizeof(Quantized));
    }

    static bool deserialize(ashiato::BitBuffer& in, const Quantized*, Quantized& out, ashiato::ComponentSerializationContext&) {
        in.read_bytes(reinterpret_cast<char*>(&out), sizeof(Quantized));
        return true;
    }

    static Quantized interpolate(const Quantized& from, const Quantized& to, float alpha) {
        return Quantized{
            from.x + (to.x - from.x) * alpha,
            from.y + (to.y - from.y) * alpha,
        };
    }

    static bool should_roll_back(const Quantized& predicted, const Quantized& authoritative) {
        const float dx = predicted.x - authoritative.x;
        const float dy = predicted.y - authoritative.y;
        return dx * dx + dy * dy > 0.000001f;
    }

    static Error compute_error(const Quantized& current, const Quantized& previous) {
        return Error{previous.x - current.x, previous.y - current.y};
    }

    static Quantized apply_error(const Quantized& current, const Error& error) {
        return Quantized{current.x + error.x, current.y + error.y};
    }

    static Error blend_out_error(const Error& error, float dt_seconds) {
        const float scale = dt_seconds >= 1.0f ? 0.0f : std::max(0.0f, 1.0f - dt_seconds * 12.0f);
        return Error{error.x * scale, error.y * scale};
    }
};

template <>
struct SyncComponentTraits<prediction_stress::PredVelocity> {
    using Quantized = prediction_stress::PredVelocity;

    static void quantize(const prediction_stress::PredVelocity& value, Quantized& out) {
        out = value;
    }

    static prediction_stress::PredVelocity dequantize(const Quantized& value) {
        return value;
    }

    static void serialize(const Quantized*, const Quantized& current, ashiato::BitBuffer& out, ashiato::ComponentSerializationContext&) {
        out.write_bytes(reinterpret_cast<const char*>(&current), sizeof(Quantized));
    }

    static bool deserialize(ashiato::BitBuffer& in, const Quantized*, Quantized& out, ashiato::ComponentSerializationContext&) {
        in.read_bytes(reinterpret_cast<char*>(&out), sizeof(Quantized));
        return true;
    }

    static bool should_roll_back(const Quantized& predicted, const Quantized& authoritative) {
        const float dx = predicted.x - authoritative.x;
        const float dy = predicted.y - authoritative.y;
        return dx * dx + dy * dy > 0.000001f;
    }
};

template <>
struct SyncComponentTraits<prediction_stress::PredAcceleration> {
    using Quantized = prediction_stress::PredAcceleration;

    static void quantize(const prediction_stress::PredAcceleration& value, Quantized& out) {
        out = value;
    }

    static prediction_stress::PredAcceleration dequantize(const Quantized& value) {
        return value;
    }

    static void serialize(const Quantized*, const Quantized& current, ashiato::BitBuffer& out, ashiato::ComponentSerializationContext&) {
        out.write_bytes(reinterpret_cast<const char*>(&current), sizeof(Quantized));
    }

    static bool deserialize(ashiato::BitBuffer& in, const Quantized*, Quantized& out, ashiato::ComponentSerializationContext&) {
        in.read_bytes(reinterpret_cast<char*>(&out), sizeof(Quantized));
        return true;
    }

    static bool should_roll_back(const Quantized& predicted, const Quantized& authoritative) {
        const float dx = predicted.x - authoritative.x;
        const float dy = predicted.y - authoritative.y;
        return dx * dx + dy * dy > 0.000001f;
    }
};

template <>
struct SyncComponentTraits<prediction_stress::PredEnergy> {
    using Quantized = std::int32_t;

    static void quantize(const prediction_stress::PredEnergy& value, Quantized& out) {
        out = value.value;
    }

    static prediction_stress::PredEnergy dequantize(const Quantized& value) {
        return prediction_stress::PredEnergy{value};
    }

    static void serialize(const Quantized*, const Quantized& current, ashiato::BitBuffer& out, ashiato::ComponentSerializationContext&) {
        out.write_bits(current, 32U);
    }

    static bool deserialize(ashiato::BitBuffer& in, const Quantized*, Quantized& out, ashiato::ComponentSerializationContext&) {
        out = static_cast<std::int32_t>(in.read_bits(32U));
        return true;
    }

    static bool should_roll_back(const Quantized& predicted, const Quantized& authoritative) {
        return predicted != authoritative;
    }
};

template <>
struct SyncComponentTraits<prediction_stress::PredFlags> {
    using Quantized = std::uint32_t;

    static void quantize(const prediction_stress::PredFlags& value, Quantized& out) {
        out = value.bits;
    }

    static prediction_stress::PredFlags dequantize(const Quantized& value) {
        return prediction_stress::PredFlags{value};
    }

    static void serialize(const Quantized*, const Quantized& current, ashiato::BitBuffer& out, ashiato::ComponentSerializationContext&) {
        out.write_unsigned_bits(current, 32U);
    }

    static bool deserialize(ashiato::BitBuffer& in, const Quantized*, Quantized& out, ashiato::ComponentSerializationContext&) {
        out = static_cast<std::uint32_t>(in.read_unsigned_bits(32U));
        return true;
    }

    static bool should_roll_back(const Quantized& predicted, const Quantized& authoritative) {
        return predicted != authoritative;
    }
};

}  // namespace ashiato::sync

namespace ashiato::sync::prediction_stress {
namespace {

Schema define_schema(ashiato::Registry& registry) {
    const ashiato::Entity position = register_sync_component<PredPosition>(registry, "PredPosition");
    const ashiato::Entity velocity = register_sync_component<PredVelocity>(registry, "PredVelocity");
    const ashiato::Entity acceleration = register_sync_component<PredAcceleration>(registry, "PredAcceleration");
    const ashiato::Entity energy = register_sync_component<PredEnergy>(registry, "PredEnergy");
    const ashiato::Entity flags = register_sync_component<PredFlags>(registry, "PredFlags");
    (void)set_fractional_tick_sampled(registry, position);
    return Schema{
        define_archetype(
            registry,
            "PredictedActor",
            {
                {position, ReplicationAudience::All, ComponentInterpolation::Interpolate},
                {velocity, ReplicationAudience::All},
                {acceleration, ReplicationAudience::All},
                {energy, ReplicationAudience::All},
                {flags, ReplicationAudience::All},
            })};
}

template <typename JobBuilder>
void register_prediction_job(JobBuilder&& job) {
    job.each([](
        ashiato::Entity,
        PredPosition& position,
        PredVelocity& velocity,
        const PredAcceleration& acceleration,
        PredEnergy& energy) {
        velocity.x += acceleration.x;
        velocity.y += acceleration.y;
        position.x += velocity.x;
        position.y += velocity.y;
        energy.value = std::max(0, energy.value - 1);
    });
}

void register_prediction_jobs(ashiato::Registry& registry) {
    register_prediction_job(registry.job<PredPosition, PredVelocity, const PredAcceleration, PredEnergy>(0));
}

void register_prediction_jobs(ashiato::Registry& registry, ReplicationClient& client) {
    register_prediction_job(client.simulation_job<PredPosition, PredVelocity, const PredAcceleration, PredEnergy>(registry, 0));
}

std::vector<ashiato::Entity> create_entities(ashiato::Registry& registry, const Schema& schema, std::uint32_t count) {
    std::vector<ashiato::Entity> entities;
    entities.reserve(count);
    for (std::uint32_t index = 0; index < count; ++index) {
        const ashiato::Entity entity = registry.create();
        const float lane = static_cast<float>(static_cast<int>(index % 64U) - 32);
        const float phase = static_cast<float>(index) * 0.017f;
        registry.add<PredPosition>(entity, PredPosition{lane * 0.1f, phase});
        registry.add<PredVelocity>(entity, PredVelocity{0.01f + static_cast<float>(index % 7U) * 0.0005f, 0.005f});
        registry.add<PredAcceleration>(entity, PredAcceleration{0.0f, 0.0f});
        registry.add<PredEnergy>(entity, PredEnergy{1000 + static_cast<std::int32_t>(index % 100U)});
        registry.add<PredFlags>(entity, PredFlags{index & 0xffU});
        registry.add<Replicated>(entity, Replicated{schema.actor});
        entities.push_back(entity);
    }
    return entities;
}

void maybe_mispredict(
    ashiato::Registry& registry,
    const std::vector<ashiato::Entity>& entities,
    double percent,
    std::mt19937& rng,
    std::uint64_t& events) {
    if (percent <= 0.0) {
        return;
    }

    std::uniform_real_distribution<double> chance(0.0, 100.0);
    std::uniform_real_distribution<float> impulse(-0.035f, 0.035f);
    for (std::size_t index = 0; index < entities.size(); ++index) {
        if (chance(rng) >= percent) {
            continue;
        }

        const ashiato::Entity entity = entities[index];
        PredAcceleration& acceleration = registry.write<PredAcceleration>(entity);
        acceleration.x = impulse(rng);
        acceleration.y = impulse(rng);

        if ((events & 1U) == 0U) {
            PredVelocity& velocity = registry.write<PredVelocity>(entity);
            velocity.x += impulse(rng);
            velocity.y += impulse(rng);
        }
        if ((events & 3U) == 0U) {
            registry.write<PredFlags>(entity).bits ^= 1U << (index & 15U);
        }
        ++events;
    }
}

std::uint32_t parse_u32(const std::string& arg, const std::string& value) {
    const unsigned long parsed = std::stoul(value);
    if (parsed > std::numeric_limits<std::uint32_t>::max()) {
        throw std::invalid_argument(arg + " is too large");
    }
    return static_cast<std::uint32_t>(parsed);
}

double parse_double(const std::string& arg, const std::string& value) {
    const double parsed = std::stod(value);
    if (!std::isfinite(parsed)) {
        throw std::invalid_argument(arg + " must be finite");
    }
    return parsed;
}

const char* rollback_policy_name(ReplicationRollbackPolicy policy) {
    switch (policy) {
    case ReplicationRollbackPolicy::All:
        return "all";
    case ReplicationRollbackPolicy::OnlyAffected:
        return "only-affected";
    case ReplicationRollbackPolicy::PredictedOnly:
        return "predicted-only";
    }
    return "all";
}

}  // namespace

Config parse_args(int argc, char** argv) {
    Config config;
    for (int index = 1; index < argc; ++index) {
        const std::string arg = argv[index];
        auto require_value = [&]() -> std::string {
            if (index + 1 >= argc) {
                throw std::runtime_error("missing value for " + arg);
            }
            return argv[++index];
        };

        if (arg == "--help" || arg == "-h") {
            throw std::runtime_error("help");
        } else if (arg == "--entities") {
            config.entities = parse_u32(arg, require_value());
        } else if (arg == "--ticks" || arg == "--duration-frames") {
            config.ticks = parse_u32(arg, require_value());
        } else if (arg == "--latency-frames") {
            config.latency_frames = parse_u32(arg, require_value());
        } else if (arg == "--misprediction-percent") {
            config.misprediction_percent = parse_double(arg, require_value());
        } else if (arg == "--seed") {
            config.seed = parse_u32(arg, require_value());
        } else if (arg == "--rollback-policy") {
            const std::string value = require_value();
            if (value == "all") {
                config.rollback_policy = ReplicationRollbackPolicy::All;
            } else if (value == "only-affected") {
                config.rollback_policy = ReplicationRollbackPolicy::OnlyAffected;
            } else if (value == "predicted-only") {
                config.rollback_policy = ReplicationRollbackPolicy::PredictedOnly;
            } else {
                throw std::invalid_argument("--rollback-policy must be all, only-affected or predicted-only");
            }
        } else if (arg == "--report") {
            const std::string value = require_value();
            if (value == "text") {
                config.json = false;
            } else if (value == "json") {
                config.json = true;
            } else {
                throw std::invalid_argument("--report must be text or json");
            }
        } else {
            throw std::invalid_argument("unknown argument: " + arg);
        }
    }

    if (config.entities == 0U) {
        throw std::invalid_argument("--entities must be greater than zero");
    }
    if (config.ticks == 0U) {
        throw std::invalid_argument("--ticks must be greater than zero");
    }
    if (config.misprediction_percent < 0.0 || config.misprediction_percent > 100.0) {
        throw std::invalid_argument("--misprediction-percent must be in [0, 100]");
    }
    return config;
}

Report run(const Config& config) {
    Report report;
    report.config = config;

    ashiato::Registry server_registry;
    const Schema server_schema = define_schema(server_registry);
    register_prediction_jobs(server_registry);
    const std::vector<ashiato::Entity> server_entities = create_entities(server_registry, server_schema, config.entities);

    ashiato::Registry client_registry;
    (void)define_schema(client_registry);

    std::vector<std::vector<ashiato::BitBuffer>> downstream(static_cast<std::size_t>(config.latency_frames) + 1U);
    std::uint32_t current_tick = 0;
    ReplicationServerOptions server_options;
    server_options.bandwidth_limit_bytes_per_tick = static_cast<std::size_t>(config.entities) * 512U;
    server_options.mtu_bytes = 1200;
    server_options.fixed_dt_seconds = 1.0 / 60.0;
    server_options.transport = [&](ClientId, const ashiato::BitBuffer& packet) {
        const std::uint32_t due_tick = current_tick + config.latency_frames;
        downstream[due_tick % downstream.size()].push_back(packet);
        ++report.server_packets;
        report.server_bytes += packet.byte_size();
    };

    ReplicationServer server(server_registry, server_options);
    server.add_client(1);

    ReplicationClientOptions client_options;
    client_options.entities.default_mode = ReplicationClientMode::Predict;
    client_options.prediction.rollback_policy = config.rollback_policy;
    client_options.clock.fixed_dt_seconds = 1.0 / 60.0;
    client_options.session.local_client = 1;
    ReplicationClient client(client_registry, client_options);
    register_prediction_jobs(client_registry, client);

    std::mt19937 rng(config.seed);
    const auto wall_begin = std::chrono::steady_clock::now();
    const std::uint32_t total_ticks = config.ticks + config.latency_frames + 1U;
    for (current_tick = 0; current_tick < total_ticks; ++current_tick) {
        {
            ScopedTimer timer(report.timing.client_receive_seconds);
            std::vector<ashiato::BitBuffer>& due = downstream[current_tick % downstream.size()];
            for (const ashiato::BitBuffer& packet : due) {
                if (client.receive(client_registry, packet)) {
                    ++report.delivered_server_packets;
                }
            }
            due.clear();
        }

        {
            ScopedTimer timer(report.timing.client_tick_seconds);
            client.tick(client_registry, 1.0 / 60.0);
        }

        {
            ScopedTimer timer(report.timing.ack_processing_seconds);
            for (const ashiato::BitBuffer& packet : client.drain_packets()) {
                ++report.client_packets;
                report.client_bytes += packet.byte_size();
                server.process_packet(server_registry, 1, packet);
            }
        }

        if (current_tick >= config.ticks) {
            continue;
        }

        {
            ScopedTimer timer(report.timing.server_simulation_seconds);
            maybe_mispredict(
                server_registry,
                server_entities,
                config.misprediction_percent,
                rng,
                report.misprediction_events);
            server_registry.run_jobs();
        }

        {
            ScopedTimer timer(report.timing.server_replication_seconds);
            server.tick(server_registry, server.options().fixed_dt_seconds);
        }
    }

    report.timing.wall_seconds =
        std::chrono::duration<double>(std::chrono::steady_clock::now() - wall_begin).count();

    client_registry.view<const PredPosition>().each([&](ashiato::Entity, const PredPosition&) {
        ++report.predicted_entities;
    });
    return report;
}

void write_report_text(std::ostream& out, const Report& report) {
    out << std::fixed << std::setprecision(6)
        << "prediction stress\n"
        << "  entities=" << report.config.entities
        << " ticks=" << report.config.ticks
        << " latency_frames=" << report.config.latency_frames
        << " misprediction_percent=" << report.config.misprediction_percent
        << " rollback_policy=" << rollback_policy_name(report.config.rollback_policy)
        << " seed=" << report.config.seed << '\n'
        << "  wall=" << report.timing.wall_seconds
        << " server_simulation=" << report.timing.server_simulation_seconds
        << " server_replication=" << report.timing.server_replication_seconds
        << " client_receive=" << report.timing.client_receive_seconds
        << " client_tick=" << report.timing.client_tick_seconds
        << " ack_processing=" << report.timing.ack_processing_seconds << '\n'
        << "  server_packets=" << report.server_packets
        << " server_bytes=" << report.server_bytes
        << " delivered_server_packets=" << report.delivered_server_packets
        << " client_packets=" << report.client_packets
        << " client_bytes=" << report.client_bytes << '\n'
        << "  misprediction_events=" << report.misprediction_events
        << " predicted_entities=" << report.predicted_entities << '\n';
}

void write_report_json(std::ostream& out, const Report& report) {
    out << std::fixed << std::setprecision(6)
        << "{"
        << "\"entities\":" << report.config.entities
        << ",\"ticks\":" << report.config.ticks
        << ",\"latency_frames\":" << report.config.latency_frames
        << ",\"misprediction_percent\":" << report.config.misprediction_percent
        << ",\"rollback_policy\":\"" << rollback_policy_name(report.config.rollback_policy) << "\""
        << ",\"seed\":" << report.config.seed
        << ",\"wall_seconds\":" << report.timing.wall_seconds
        << ",\"server_simulation_seconds\":" << report.timing.server_simulation_seconds
        << ",\"server_replication_seconds\":" << report.timing.server_replication_seconds
        << ",\"client_receive_seconds\":" << report.timing.client_receive_seconds
        << ",\"client_tick_seconds\":" << report.timing.client_tick_seconds
        << ",\"ack_processing_seconds\":" << report.timing.ack_processing_seconds
        << ",\"server_packets\":" << report.server_packets
        << ",\"server_bytes\":" << report.server_bytes
        << ",\"delivered_server_packets\":" << report.delivered_server_packets
        << ",\"client_packets\":" << report.client_packets
        << ",\"client_bytes\":" << report.client_bytes
        << ",\"misprediction_events\":" << report.misprediction_events
        << ",\"predicted_entities\":" << report.predicted_entities
        << "}\n";
}

void write_usage(std::ostream& out) {
    out << "usage: ashiato_sync_prediction_stress [options]\n"
        << "  --entities N\n"
        << "  --ticks N | --duration-frames N\n"
        << "  --latency-frames N       default: 10\n"
        << "  --misprediction-percent P\n"
        << "  --rollback-policy all|only-affected\n"
        << "  --seed N\n"
        << "  --report text|json\n";
}

}  // namespace ashiato::sync::prediction_stress

int main(int argc, char** argv) {
    try {
        const ashiato::sync::prediction_stress::Config config =
            ashiato::sync::prediction_stress::parse_args(argc, argv);
        const ashiato::sync::prediction_stress::Report report =
            ashiato::sync::prediction_stress::run(config);
        if (config.json) {
            ashiato::sync::prediction_stress::write_report_json(std::cout, report);
        } else {
            ashiato::sync::prediction_stress::write_report_text(std::cout, report);
        }
        return 0;
    } catch (const std::runtime_error& error) {
        if (std::string(error.what()) == "help") {
            ashiato::sync::prediction_stress::write_usage(std::cout);
            return 0;
        }
        std::cerr << "error: " << error.what() << '\n';
    } catch (const std::exception& error) {
        std::cerr << "error: " << error.what() << '\n';
    }

    ashiato::sync::prediction_stress::write_usage(std::cerr);
    return 1;
}
