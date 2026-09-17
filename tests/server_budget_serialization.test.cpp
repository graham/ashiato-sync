// How much work the per-client send loop does on records the bandwidth budget then refuses,
// and what options.max_budget_refusals_per_client_tick bounds. The loop must serialize a
// candidate before it can know whether the record fits, so a starved client can pay to
// quantize and delta-encode every dirty entity in order to send one of them. These tests
// count the quantize calls, which is the work the benchmarks measure in wall-clock.

#include "test_protocol.hpp"

#include <catch2/catch_test_macros.hpp>

#include <array>
#include <cstddef>
#include <cstdint>
#include <limits>
#include <memory>
#include <vector>

namespace ashiato_sync_tests {

// A record that nearly fills a small budget on its own.
struct BulkyProbe {
    std::int32_t value = 0;
};

// A record small enough to fit in the room a bulky one leaves behind.
struct SlightProbe {
    std::int32_t value = 0;
};

inline std::size_t& probe_quantize_calls() {
    static std::size_t calls = 0;
    return calls;
}

}  // namespace ashiato_sync_tests

namespace ashiato::sync {

template <>
struct SyncComponentTraits<ashiato_sync_tests::BulkyProbe> {
    using Quantized = std::array<std::int32_t, 8>;

    static void quantize(const ashiato_sync_tests::BulkyProbe& value, Quantized& out) {
        ++ashiato_sync_tests::probe_quantize_calls();
        out.fill(value.value);
    }

    static ashiato_sync_tests::BulkyProbe dequantize(const Quantized& value) {
        return ashiato_sync_tests::BulkyProbe{value[0]};
    }

    static void serialize(const Quantized*, const Quantized& current, ashiato::BitBuffer& out, ashiato::ComponentSerializationContext&) {
        for (const std::int32_t word : current) {
            out.write_bits(word, 32U);
        }
    }

    static bool deserialize(ashiato::BitBuffer& in, const Quantized*, Quantized& out, ashiato::ComponentSerializationContext&) {
        for (std::int32_t& word : out) {
            word = static_cast<std::int32_t>(in.read_bits(32U));
        }
        return true;
    }
};

template <>
struct SyncComponentTraits<ashiato_sync_tests::SlightProbe> {
    using Quantized = std::int32_t;

    static void quantize(const ashiato_sync_tests::SlightProbe& value, Quantized& out) {
        ++ashiato_sync_tests::probe_quantize_calls();
        out = value.value;
    }

    static ashiato_sync_tests::SlightProbe dequantize(const Quantized& value) {
        return ashiato_sync_tests::SlightProbe{value};
    }

    static void serialize(const Quantized*, const Quantized& current, ashiato::BitBuffer& out, ashiato::ComponentSerializationContext&) {
        out.write_bits(current, 8U);
    }

    static bool deserialize(ashiato::BitBuffer& in, const Quantized*, Quantized& out, ashiato::ComponentSerializationContext&) {
        out = static_cast<std::int32_t>(in.read_bits(8U));
        return true;
    }
};

}  // namespace ashiato::sync

using namespace ashiato_sync_tests;

// The record count out of an update packet's header. The shared reader in test_protocol.hpp
// decodes component payloads too, and it does not know these probe components.
std::size_t read_update_record_count(ashiato::BitBuffer packet) {
    packet.read_bits(ashiato::sync::protocol::message_bits);
    packet.read_bits(32U);
    packet.read_bits(ashiato::sync::protocol::server_packet_id_bits);
    packet.read_bits(32U);
    return static_cast<std::size_t>(packet.read_bits(16U));
}

namespace {

constexpr std::size_t bulky_entity_count = 64;

// A budget that carries one bulky record and still has room for the slight one, so a test
// can tell the cost of the loop apart from the packing the loop exists to do.
constexpr std::size_t starved_budget_bytes = 64;

struct StarvedServer {
    ashiato::Registry registry;
    std::vector<ashiato::Entity> bulky;
    ashiato::Entity slight{};
    std::size_t records = 0;
    std::size_t packets = 0;
    std::unique_ptr<ashiato::sync::ReplicationServer> server;

    explicit StarvedServer(std::size_t budget_bytes, std::size_t refusal_bound) {
        const ashiato::Entity bulky_component =
            ashiato::sync::register_sync_component<BulkyProbe>(registry, "BulkyProbe");
        const ashiato::Entity slight_component =
            ashiato::sync::register_sync_component<SlightProbe>(registry, "SlightProbe");
        const ashiato::sync::SyncArchetypeId bulky_archetype = ashiato::sync::define_archetype(
            registry, "Bulky", {{bulky_component, ashiato::sync::ReplicationAudience::All}});
        const ashiato::sync::SyncArchetypeId slight_archetype = ashiato::sync::define_archetype(
            registry, "Slight", {{slight_component, ashiato::sync::ReplicationAudience::All}});

        for (std::size_t index = 0; index < bulky_entity_count; ++index) {
            const ashiato::Entity entity = registry.create();
            REQUIRE(registry.add<BulkyProbe>(entity, BulkyProbe{static_cast<std::int32_t>(index) + 1}) != nullptr);
            bulky.push_back(entity);
        }
        slight = registry.create();
        REQUIRE(registry.add<SlightProbe>(slight, SlightProbe{1}) != nullptr);

        ashiato::sync::ReplicationServerOptions options;
        options.bandwidth_limit_bytes_per_tick = budget_bytes;
        options.mtu_bytes = 4096;
        options.prioritizer_interval_frames = 0;
        options.max_budget_refusals_per_client_tick = refusal_bound;
        // The slight entity is last in priority order, which is the case the loop's refusal
        // handling exists for: a small record behind many records that do not fit.
        options.prioritizer = [this](ashiato::sync::ClientId, ashiato::sync::ReplicationPriorityObject object) {
            ashiato::sync::ReplicationPriorityDecision decision;
            decision.priority = object.entity == slight ? 1.0f : 100.0f;
            return decision;
        };
        options.transport = [this](ashiato::sync::ClientId, const ashiato::BitBuffer& payload) {
            ++packets;
            records += read_update_record_count(payload);
        };

        server = std::make_unique<ashiato::sync::ReplicationServer>(registry, options);
        REQUIRE(server->add_client(1));
        for (const ashiato::Entity entity : bulky) {
            REQUIRE(start_sync(registry, entity, bulky_archetype));
        }
        REQUIRE(start_sync(registry, slight, slight_archetype));
    }

    void tick() {
        probe_quantize_calls() = 0;
        server->tick(registry, server->options().fixed_dt_seconds);
    }
};

constexpr std::size_t unbounded_refusals = std::numeric_limits<std::size_t>::max();

}  // namespace

TEST_CASE("a starved client serializes every dirty candidate the budget then refuses") {
    StarvedServer starved(starved_budget_bytes, unbounded_refusals);
    starved.tick();

    // Two records went out, and the loop quantized all sixty-five candidates to find them:
    // every entity after the first refusal was quantized, delta-encoded and retained, then
    // dropped. This is the server CPU the benchmarks measure, and it is proportional to the
    // dirty entities rather than to the budget.
    REQUIRE(starved.records == 2);
    REQUIRE(probe_quantize_calls() == bulky_entity_count + 1U);
}

TEST_CASE("a refusal bound caps what a starved client serializes and throws away") {
    StarvedServer starved(starved_budget_bytes, 4U);
    starved.tick();

    // Five candidates quantized instead of sixty-five: the one that fit, and the four
    // refusals the bound allows before the loop stops looking.
    REQUIRE(probe_quantize_calls() == 5U);


    // And the cost of that: the loop never reaches the slight record at the back of the
    // priority order, so the packet carries one record where it used to carry two. That is
    // the trade this option exposes, and the reason its default leaves it unbounded.
    REQUIRE(starved.records == 1);
}

TEST_CASE("a refusal bound changes nothing for a client whose budget is not starved") {
    // No candidate is ever refused for budget, so the bound is never consulted, even at its
    // strictest setting.
    StarvedServer generous(64U * 1024U, 0U);
    generous.tick();

    REQUIRE(generous.records == bulky_entity_count + 1U);
    REQUIRE(probe_quantize_calls() == bulky_entity_count + 1U);
}
