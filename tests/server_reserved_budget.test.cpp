// What EntityReplicationDecision::reserved and ReplicationServerOptions::reserved_bandwidth_bytes_per_tick
// promise a client: a reserved record is sent ahead of every unreserved one whatever the priorities,
// the reserve pays for it so the shared budget carries what it carried before, and the client is
// never sent more than the shared budget plus the reserve in a tick. The case it exists for is a
// predicting client whose own entities lose the priority race to a crowd of others once the budget
// binds, and roll the client back when they arrive late.

#include "test_protocol.hpp"

#include <catch2/catch_test_macros.hpp>

#include <algorithm>
#include <array>
#include <cstddef>
#include <cstdint>
#include <limits>
#include <memory>
#include <vector>

namespace ashiato_sync_tests {

// A crowd member's record: one of these nearly fills the tight budget below on its own.
struct CrowdProbe {
    std::int32_t value = 0;
};

// The client's own entity: a small record at the lowest priority, which loses every race.
struct OwnProbe {
    std::int32_t value = 0;
};

}  // namespace ashiato_sync_tests

namespace ashiato::sync {

template <>
struct SyncComponentTraits<ashiato_sync_tests::CrowdProbe> {
    using Quantized = std::array<std::int32_t, 8>;

    static void quantize(const ashiato_sync_tests::CrowdProbe& value, Quantized& out) {
        out.fill(value.value);
    }

    static ashiato_sync_tests::CrowdProbe dequantize(const Quantized& value) {
        return ashiato_sync_tests::CrowdProbe{value[0]};
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
struct SyncComponentTraits<ashiato_sync_tests::OwnProbe> {
    using Quantized = std::int32_t;

    static void quantize(const ashiato_sync_tests::OwnProbe& value, Quantized& out) {
        out = value.value;
    }

    static ashiato_sync_tests::OwnProbe dequantize(const Quantized& value) {
        return ashiato_sync_tests::OwnProbe{value};
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

namespace {

// The record count out of an update packet's header (as server_budget_serialization.test.cpp reads it).
std::size_t reserved_test_record_count(ashiato::BitBuffer packet) {
    packet.read_bits(ashiato::sync::protocol::message_bits);
    packet.read_bits(32U);
    packet.read_bits(ashiato::sync::protocol::server_packet_id_bits);
    packet.read_bits(32U);
    return static_cast<std::size_t>(packet.read_bits(16U));
}

constexpr std::size_t generous_budget_bytes = 64U * 1024U;
constexpr std::size_t unbounded_refusals = std::numeric_limits<std::size_t>::max();

struct ReservedBudgetServer {
    ashiato::Registry registry;
    std::vector<ashiato::Entity> crowd;
    ashiato::Entity own{};
    std::size_t tick_records = 0;
    std::size_t tick_bytes = 0;
    std::size_t largest_packet_bytes = 0;
    std::unique_ptr<ashiato::sync::ReplicationServer> server;

    ReservedBudgetServer(
        std::size_t crowd_count,
        std::size_t budget_bytes,
        std::size_t reserve_bytes,
        bool own_reserved,
        float own_priority,
        std::size_t refusal_bound = unbounded_refusals) {
        const ashiato::Entity crowd_component =
            ashiato::sync::register_sync_component<CrowdProbe>(registry, "CrowdProbe");
        const ashiato::Entity own_component =
            ashiato::sync::register_sync_component<OwnProbe>(registry, "OwnProbe");
        const ashiato::sync::SyncArchetypeId crowd_archetype = ashiato::sync::define_archetype(
            registry, "Crowd", {{crowd_component, ashiato::sync::ReplicationAudience::All}});
        const ashiato::sync::SyncArchetypeId own_archetype = ashiato::sync::define_archetype(
            registry, "Own", {{own_component, ashiato::sync::ReplicationAudience::All}});

        for (std::size_t index = 0; index < crowd_count; ++index) {
            const ashiato::Entity entity = registry.create();
            REQUIRE(registry.add<CrowdProbe>(entity, CrowdProbe{static_cast<std::int32_t>(index) + 1}) != nullptr);
            crowd.push_back(entity);
        }
        own = registry.create();
        REQUIRE(registry.add<OwnProbe>(own, OwnProbe{1}) != nullptr);

        ashiato::sync::ReplicationServerOptions options;
        options.bandwidth_limit_bytes_per_tick = budget_bytes;
        options.reserved_bandwidth_bytes_per_tick = reserve_bytes;
        options.mtu_bytes = 4096U;
        options.entity_replication_decision_interval_frames = 0;
        options.max_budget_refusals_per_client_tick = refusal_bound;
        options.entity_replication_decider = [this, own_reserved, own_priority](
            ashiato::sync::ClientId,
            ashiato::sync::EntityReplicationDecisionContext context) {
            ashiato::sync::EntityReplicationDecision decision;
            if (context.entity == own) {
                decision.priority = own_priority;
                decision.reserved = own_reserved;
            } else {
                decision.priority = 100.0f;
            }
            return decision;
        };
        options.transport = [this](ashiato::sync::ClientId, const ashiato::BitBuffer& payload) {
            tick_records += reserved_test_record_count(payload);
            tick_bytes += payload.byte_size();
            largest_packet_bytes = std::max(largest_packet_bytes, payload.byte_size());
        };

        server = std::make_unique<ashiato::sync::ReplicationServer>(registry, options);
        REQUIRE(server->add_client(1));
        for (const ashiato::Entity entity : crowd) {
            REQUIRE(start_sync(registry, entity, crowd_archetype));
        }
        REQUIRE(start_sync(registry, own, own_archetype));
    }

    // Every entity changes every tick, as a flying crowd and a predicted craft do.
    void tick(std::int32_t step) {
        for (std::size_t index = 0; index < crowd.size(); ++index) {
            registry.write<CrowdProbe>(crowd[index]) = CrowdProbe{static_cast<std::int32_t>(index) + step * 100};
        }
        registry.write<OwnProbe>(own) = OwnProbe{step};
        tick_records = 0;
        tick_bytes = 0;
        server->tick(registry, server->options().fixed_dt_seconds);
    }
};

// The bytes of an update packet carrying one crowd record and nothing else: the tight budget.
std::size_t one_crowd_record_packet_bytes() {
    // The own entity filtered (priority 0), so the only packet is the crowd member's.
    ReservedBudgetServer calibration(1U, generous_budget_bytes, 0U, false, 0.0f);
    calibration.tick(1);
    REQUIRE(calibration.tick_records == 1U);
    return calibration.tick_bytes;
}

}  // namespace

TEST_CASE("an unreserved own entity at the lowest priority waits behind the crowd once the budget binds") {
    const std::size_t tight = one_crowd_record_packet_bytes();
    ReservedBudgetServer starved(8U, tight, 0U, false, 1.0f);
    for (std::int32_t step = 1; step <= 4; ++step) {
        starved.tick(step);
        // One crowd record a tick and never the own entity: the crowd's accumulators always lead.
        REQUIRE(starved.tick_records == 1U);
        REQUIRE(starved.tick_bytes == tight);
    }
}

TEST_CASE("a reserved record is sent first every tick and the reserve pays for it") {
    const std::size_t tight = one_crowd_record_packet_bytes();
    constexpr std::size_t reserve = 64U;
    ReservedBudgetServer reserved(8U, tight, reserve, true, 1.0f);
    for (std::int32_t step = 1; step <= 4; ++step) {
        reserved.tick(step);
        // The own record AND the crowd record the shared budget carried without the reservation.
        REQUIRE(reserved.tick_records == 2U);
        // Bounded: never more than the shared budget plus the reserve, and in fact only what the
        // own record added to the packet.
        REQUIRE(reserved.tick_bytes > tight);
        REQUIRE(reserved.tick_bytes <= tight + reserve);
    }
}

TEST_CASE("the reserve rides on top of a client's own bandwidth limit") {
    // A generous server whose one client is held to the tight budget by set_client_bandwidth_limit
    // (a WAN client beside an unlimited LAN one): the reserve is added to that client's limit.
    const std::size_t tight = one_crowd_record_packet_bytes();
    constexpr std::size_t reserve = 64U;
    ReservedBudgetServer wan(8U, generous_budget_bytes, reserve, true, 1.0f);
    REQUIRE(wan.server->set_client_bandwidth_limit(1, tight));
    for (std::int32_t step = 1; step <= 3; ++step) {
        wan.tick(step);
        REQUIRE(wan.tick_records == 2U);
        REQUIRE(wan.tick_bytes > tight);
        REQUIRE(wan.tick_bytes <= tight + reserve);
    }
}

TEST_CASE("with no reserve a reserved record still goes first but the shared budget pays for it") {
    const std::size_t tight = one_crowd_record_packet_bytes();
    ReservedBudgetServer unfunded(8U, tight, 0U, true, 1.0f);
    for (std::int32_t step = 1; step <= 4; ++step) {
        unfunded.tick(step);
        // The own record took the room, so no crowd record fits behind it.
        REQUIRE(unfunded.tick_records == 1U);
        REQUIRE(unfunded.tick_bytes < tight);
    }
}

TEST_CASE("a reserved record is not cut off by the refusal bound") {
    const std::size_t tight = one_crowd_record_packet_bytes();
    // A bound of one: after the first refusal the loop stops, which used to leave the own
    // entity at the back of the order unsent for good.
    ReservedBudgetServer bounded_unreserved(8U, tight, 64U, false, 1.0f, 1U);
    bounded_unreserved.tick(1);
    REQUIRE(bounded_unreserved.tick_records == 1U);

    ReservedBudgetServer bounded_reserved(8U, tight, 64U, true, 1.0f, 1U);
    bounded_reserved.tick(1);
    REQUIRE(bounded_reserved.tick_records == 2U);
}

TEST_CASE("an unused reserve is not handed to the crowd") {
    const std::size_t tight = one_crowd_record_packet_bytes();
    // The own entity filtered: nothing reserved is dirty, so the reserve stays unspent and the
    // crowd gets exactly the shared budget.
    ReservedBudgetServer idle(8U, tight, 4096U, true, 0.0f);
    for (std::int32_t step = 1; step <= 3; ++step) {
        idle.tick(step);
        REQUIRE(idle.tick_records == 1U);
        REQUIRE(idle.tick_bytes == tight);
    }
}

TEST_CASE("a reservation changes nothing for a client whose budget is not starved") {
    ReservedBudgetServer generous_unreserved(8U, generous_budget_bytes, 0U, false, 1.0f);
    ReservedBudgetServer generous_reserved(8U, generous_budget_bytes, 64U, true, 1.0f);
    generous_unreserved.tick(1);
    generous_reserved.tick(1);
    REQUIRE(generous_unreserved.tick_records == 9U);
    REQUIRE(generous_reserved.tick_records == 9U);
    REQUIRE(generous_reserved.tick_bytes == generous_unreserved.tick_bytes);
}
