#include "test_protocol.hpp"

#include "ashiato/sync/simulated_link.hpp"

#include <catch2/catch_test_macros.hpp>

#include <algorithm>
#include <array>
#include <cstddef>
#include <cstdint>
#include <limits>
#include <stdexcept>
#include <string>
#include <utility>
#include <vector>

using namespace ashiato_sync_tests;

TEST_CASE("replication prioritizer refreshes cached decisions by replicated slot bucket") {
    ashiato::Registry registry;
    const ashiato::sync::SyncArchetypeId archetype = define_position_archetype(registry);
    const ashiato::Entity entity = registry.create();
    REQUIRE(registry.add<ashiato_sync_tests::Position>(entity, ashiato_sync_tests::Position{1.0f, 1.0f}) != nullptr);
    REQUIRE(start_sync(registry, entity, archetype));

    std::size_t prioritizer_calls = 0;
    std::vector<ashiato::BitBuffer> payloads;
    ashiato::sync::ReplicationServerOptions options;
    options.prioritizer_interval_frames = 3;
    options.prioritizer = [&](ashiato::sync::ClientId, ashiato::sync::ReplicationPriorityObject object) {
        ++prioritizer_calls;
        REQUIRE(object.entity == entity);
        ashiato::sync::ReplicationPriorityDecision decision;
        decision.priority = prioritizer_calls == 1 ? 10.0f : 100.0f;
        return decision;
    };
    options.transport = [&](ashiato::sync::ClientId, const ashiato::BitBuffer& payload) {
        payloads.push_back(payload);
    };

    ashiato::sync::ReplicationServer server(registry, options);
    REQUIRE(server.add_client(1));

    server.tick(registry, server.options().fixed_dt_seconds);
    REQUIRE(prioritizer_calls == 1);
    REQUIRE(payloads.size() == 1);

    server.tick(registry, server.options().fixed_dt_seconds);
    REQUIRE(prioritizer_calls == 1);

    server.tick(registry, server.options().fixed_dt_seconds);
    REQUIRE(prioritizer_calls == 2);

    server.tick(registry, server.options().fixed_dt_seconds);
    REQUIRE(prioritizer_calls == 2);

    server.tick(registry, server.options().fixed_dt_seconds);
    REQUIRE(prioritizer_calls == 2);

    server.tick(registry, server.options().fixed_dt_seconds);
    REQUIRE(prioritizer_calls == 3);
    REQUIRE(read_server_update(payloads.back(), 2U, sizeof(ashiato_sync_tests::Position) * 8U).entities.size() == 1);
}

TEST_CASE("replication prioritizer priority affects serialized send order") {
    ashiato::Registry registry;
    const ashiato::Entity position_component =
        ashiato::sync::register_sync_component<NetworkedPosition>(registry, "NetworkedPosition");
    const ashiato::sync::SyncArchetypeId archetype = ashiato::sync::define_archetype(
        registry,
        "NetworkedActor",
        {{position_component, ashiato::sync::ReplicationAudience::All}});
    const ashiato::Entity low = registry.create();
    const ashiato::Entity middle = registry.create();
    const ashiato::Entity high = registry.create();
    REQUIRE(registry.add<NetworkedPosition>(low, NetworkedPosition{1.0f, 1.0f}) != nullptr);
    REQUIRE(registry.add<NetworkedPosition>(middle, NetworkedPosition{2.0f, 2.0f}) != nullptr);
    REQUIRE(registry.add<NetworkedPosition>(high, NetworkedPosition{3.0f, 3.0f}) != nullptr);
    REQUIRE(start_sync(registry, low, archetype));
    REQUIRE(start_sync(registry, middle, archetype));
    REQUIRE(start_sync(registry, high, archetype));

    std::vector<ashiato::BitBuffer> payloads;
    ashiato::sync::ReplicationServerOptions options;
    options.bandwidth_limit_bytes_per_tick = 1024;
    options.mtu_bytes = 21;
    options.prioritizer_interval_frames = 1;
    options.prioritizer = [&](ashiato::sync::ClientId, ashiato::sync::ReplicationPriorityObject object) {
        ashiato::sync::ReplicationPriorityDecision decision;
        if (object.entity == high) {
            decision.priority = 100.0f;
        } else if (object.entity == middle) {
            decision.priority = 50.0f;
        }
        return decision;
    };
    options.transport = [&](ashiato::sync::ClientId, const ashiato::BitBuffer& payload) {
        payloads.push_back(payload);
    };

    ashiato::sync::ReplicationServer server(registry, options);
    REQUIRE(server.add_client(1));
    server.tick(registry, server.options().fixed_dt_seconds);

    REQUIRE(payloads.size() == 3);
    const NetworkedPayload first = read_first_networked_payload(payloads[0]);
    REQUIRE_FALSE(first.delta);
    REQUIRE(first.x == 30);
    REQUIRE(first.y == 30);
}

TEST_CASE("entity references boost visible low priority targets on the next tick") {
    ashiato::Registry registry;
    const ashiato::Entity position_component =
        ashiato::sync::register_sync_component<NetworkedPosition>(registry, "NetworkedPosition");
    const ashiato::Entity reference_component =
        ashiato::sync::register_sync_component<TargetReference>(registry, "TargetReference");
    const ashiato::sync::SyncArchetypeId position_archetype = ashiato::sync::define_archetype(
        registry,
        "ReferencedActor",
        {{position_component, ashiato::sync::ReplicationAudience::All}});
    const ashiato::sync::SyncArchetypeId reference_archetype = ashiato::sync::define_archetype(
        registry,
        "ReferenceActor",
        {{reference_component, ashiato::sync::ReplicationAudience::All}});

    const ashiato::Entity source = registry.create();
    const ashiato::Entity target = registry.create();
    const ashiato::Entity medium = registry.create();
    REQUIRE(registry.add<TargetReference>(
                source,
                TargetReference{ashiato::sync::EntityReference{target}}) != nullptr);
    REQUIRE(registry.add<NetworkedPosition>(target, NetworkedPosition{2.0f, 2.0f}) != nullptr);
    REQUIRE(registry.add<NetworkedPosition>(medium, NetworkedPosition{3.0f, 3.0f}) != nullptr);
    REQUIRE(start_sync(registry, source, reference_archetype));
    REQUIRE(start_sync(registry, target, position_archetype));
    REQUIRE(start_sync(registry, medium, position_archetype));

    std::vector<ashiato::BitBuffer> payloads;
    ashiato::sync::ReplicationServerOptions options;
    options.bandwidth_limit_bytes_per_tick = 21;
    options.mtu_bytes = 21;
    options.prioritizer_interval_frames = 1;
    options.prioritizer = [&](ashiato::sync::ClientId, ashiato::sync::ReplicationPriorityObject object) {
        ashiato::sync::ReplicationPriorityDecision decision;
        if (object.entity == source) {
            decision.priority = 100.0f;
        } else if (object.entity == medium) {
            decision.priority = 50.0f;
        }
        return decision;
    };
    options.transport = [&](ashiato::sync::ClientId, const ashiato::BitBuffer& payload) {
        payloads.push_back(payload);
    };

    ashiato::sync::ReplicationServer server(registry, options);
    REQUIRE(server.add_client(1));
    server.tick(registry, server.options().fixed_dt_seconds);

    REQUIRE(payloads.size() == 1);
    ServerUpdatePacket update = read_server_update(
        payloads.back(),
        2U,
        1U + ashiato::sync::protocol::network_entity_id_encoded_bits(2U));
    REQUIRE(update.entities.size() == 1);
    REQUIRE(update.entities[0].archetype == reference_archetype);
    REQUIRE(update.entities[0].components.size() == 1);
    REQUIRE(update.entities[0].components[0].payload.read_bool());
    std::uint32_t referenced_network_id = 0;
    REQUIRE(ashiato::sync::protocol::read_network_entity_id(
        update.entities[0].components[0].payload,
        referenced_network_id));
    REQUIRE(referenced_network_id != 0U);

    payloads.clear();
    server.tick(registry, server.options().fixed_dt_seconds);

    REQUIRE(payloads.size() == 1);
    update = read_server_update(payloads.back(), 2U);
    REQUIRE(update.entities.size() == 1);
    REQUIRE(update.entities[0].archetype == position_archetype);
    REQUIRE(update.entities[0].network_id == referenced_network_id);
}

TEST_CASE("replication prioritizer component masks apply to delta updates") {
    ashiato::Registry registry;
    const ashiato::Entity position_component =
        ashiato::sync::register_sync_component<NetworkedPosition>(registry, "NetworkedPosition");
    const ashiato::Entity health_component = ashiato::sync::register_sync_component<Health>(registry, "Health");
    const ashiato::sync::SyncArchetypeId archetype = ashiato::sync::define_archetype(
        registry,
        "MaskedActor",
        {
            {position_component, ashiato::sync::ReplicationAudience::All},
            {health_component, ashiato::sync::ReplicationAudience::All},
        });
    const ashiato::Entity entity = registry.create();
    REQUIRE(registry.add<NetworkedPosition>(entity, NetworkedPosition{1.0f, 2.0f}) != nullptr);
    REQUIRE(registry.add<Health>(entity, Health{25}) != nullptr);
    REQUIRE(start_sync(registry, entity, archetype));

    std::uint64_t component_mask = std::numeric_limits<std::uint64_t>::max();
    std::vector<ashiato::BitBuffer> payloads;
    ashiato::sync::ReplicationServerOptions options;
    options.bandwidth_limit_bytes_per_tick = 1024;
    options.prioritizer_interval_frames = 1;
    options.prioritizer = [&](ashiato::sync::ClientId, ashiato::sync::ReplicationPriorityObject) {
        ashiato::sync::ReplicationPriorityDecision decision;
        decision.component_mask = component_mask;
        return decision;
    };
    options.transport = [&](ashiato::sync::ClientId, const ashiato::BitBuffer& payload) {
        payloads.push_back(payload);
    };

    ashiato::sync::ReplicationServer server(registry, options);
    REQUIRE(server.add_client(1));
    server.tick(registry, server.options().fixed_dt_seconds);
    REQUIRE(payloads.size() == 1);
    ServerUpdatePacket update = read_server_update(payloads.back(), 3U);
    REQUIRE(update.entities.size() == 1);
    REQUIRE(update.entities[0].full);
    REQUIRE(update.entities[0].components.size() == 2);
    REQUIRE(server.acknowledge_entity(1, entity, update.frame));

    registry.write<NetworkedPosition>(entity) = NetworkedPosition{4.0f, 6.0f};
    registry.write<Health>(entity) = Health{75};
    component_mask = std::uint64_t{1} << 0U;
    payloads.clear();
    server.tick(registry, server.options().fixed_dt_seconds);

    REQUIRE(payloads.size() == 1);
    update = read_server_update(payloads.back(), 3U);
    REQUIRE(update.entities.size() == 1);
    REQUIRE_FALSE(update.entities[0].full);
    REQUIRE(update.entities[0].components.size() == 1);
    REQUIRE(update.entities[0].components[0].component_index == 1);
    const NetworkedPayload fields = read_networked_payload(update.entities[0].components[0].payload);
    REQUIRE(fields.delta);
    REQUIRE(fields.x == 30);
    REQUIRE(fields.y == 40);

    // Re-opening a component invalidates this client's whole-frame baseline:
    // it never received Health=75 while that bit was masked out.
    REQUIRE(server.acknowledge_entity(1, entity, update.frame));
    component_mask = std::numeric_limits<std::uint64_t>::max();
    registry.write<NetworkedPosition>(entity) = NetworkedPosition{7.0f, 8.0f};
    payloads.clear();
    server.tick(registry, server.options().fixed_dt_seconds);
    REQUIRE(payloads.size() == 1);
    update = read_server_update(payloads.back(), 3U);
    REQUIRE(update.entities.size() == 1);
    REQUIRE(update.entities[0].full);
    REQUIRE(update.entities[0].components.size() == 2);
}

TEST_CASE("replication prioritizer can emit an entity record with an all-zero component mask") {
    ashiato::Registry registry;
    const ashiato::Entity position_component =
        ashiato::sync::register_sync_component<NetworkedPosition>(registry, "NetworkedPosition");
    const ashiato::sync::SyncArchetypeId archetype = ashiato::sync::define_archetype(
        registry,
        "ZeroMaskActor",
        {{position_component, ashiato::sync::ReplicationAudience::All}});
    const ashiato::Entity entity = registry.create();
    REQUIRE(registry.add<NetworkedPosition>(entity, NetworkedPosition{1.0f, 2.0f}) != nullptr);
    REQUIRE(start_sync(registry, entity, archetype));

    std::vector<ashiato::BitBuffer> payloads;
    ashiato::sync::ReplicationServerOptions options;
    options.bandwidth_limit_bytes_per_tick = 1024;
    options.prioritizer_interval_frames = 1;
    options.prioritizer = [&](ashiato::sync::ClientId, ashiato::sync::ReplicationPriorityObject) {
        ashiato::sync::ReplicationPriorityDecision decision;
        decision.component_mask = 0;
        return decision;
    };
    options.transport = [&](ashiato::sync::ClientId, const ashiato::BitBuffer& payload) {
        payloads.push_back(payload);
    };

    ashiato::sync::ReplicationServer server(registry, options);
    REQUIRE(server.add_client(1));
    server.tick(registry, server.options().fixed_dt_seconds);

    REQUIRE(payloads.size() == 1);
    const ServerUpdatePacket update = read_server_update(payloads.back());
    REQUIRE(update.entities.size() == 1);
    REQUIRE(update.entities[0].full);
    REQUIRE(update.entities[0].components.empty());
}

TEST_CASE("replication prioritizer applies independent decisions per client") {
    ashiato::Registry registry;
    const ashiato::Entity position_component =
        ashiato::sync::register_sync_component<NetworkedPosition>(registry, "NetworkedPosition");
    const ashiato::sync::SyncArchetypeId archetype = ashiato::sync::define_archetype(
        registry,
        "PerClientActor",
        {{position_component, ashiato::sync::ReplicationAudience::All}});
    const ashiato::Entity first = registry.create();
    const ashiato::Entity second = registry.create();
    REQUIRE(registry.add<NetworkedPosition>(first, NetworkedPosition{1.0f, 1.0f}) != nullptr);
    REQUIRE(registry.add<NetworkedPosition>(second, NetworkedPosition{2.0f, 2.0f}) != nullptr);
    REQUIRE(start_sync(registry, first, archetype));
    REQUIRE(start_sync(registry, second, archetype));

    std::vector<std::pair<ashiato::sync::ClientId, ashiato::BitBuffer>> payloads;
    ashiato::sync::ReplicationServerOptions options;
    options.bandwidth_limit_bytes_per_tick = 1024;
    options.prioritizer_interval_frames = 1;
    options.prioritizer = [&](ashiato::sync::ClientId client, ashiato::sync::ReplicationPriorityObject object) {
        ashiato::sync::ReplicationPriorityDecision decision;
        decision.priority =
            client == 1
            ? (object.entity == first ? 100.0f : 10.0f)
            : (object.entity == second ? 100.0f : 10.0f);
        return decision;
    };
    options.transport = [&](ashiato::sync::ClientId client, const ashiato::BitBuffer& payload) {
        payloads.push_back({client, payload});
    };

    ashiato::sync::ReplicationServer server(registry, options);
    REQUIRE(server.add_client(1));
    REQUIRE(server.add_client(2));
    server.tick(registry, server.options().fixed_dt_seconds);

    REQUIRE(payloads.size() == 2);
    for (const auto& sent : payloads) {
        const ServerUpdatePacket update = read_server_update(sent.second);
        REQUIRE(update.entities.size() == 2);
        REQUIRE(update.entities[0].components.size() == 1);
        const NetworkedPayload fields = read_networked_payload(update.entities[0].components[0].payload);
        REQUIRE_FALSE(fields.delta);
        if (sent.first == 1) {
            REQUIRE(fields.x == 10);
        } else {
            REQUIRE(sent.first == 2);
            REQUIRE(fields.x == 20);
        }
    }
}

TEST_CASE("replication prioritizer decisions are honored by serialized updates") {
    ashiato::Registry registry;
    const ashiato::Entity position_component =
        ashiato::sync::register_sync_component<NetworkedPosition>(registry, "NetworkedPosition");
    const ashiato::sync::SyncArchetypeId archetype = ashiato::sync::define_archetype(
        registry,
        "ParallelPrioritizedActor",
        {{position_component, ashiato::sync::ReplicationAudience::All}});
    const ashiato::Entity first = registry.create();
    const ashiato::Entity second = registry.create();
    REQUIRE(registry.add<NetworkedPosition>(first, NetworkedPosition{1.0f, 1.0f}) != nullptr);
    REQUIRE(registry.add<NetworkedPosition>(second, NetworkedPosition{2.0f, 2.0f}) != nullptr);
    REQUIRE(start_sync(registry, first, archetype));
    REQUIRE(start_sync(registry, second, archetype));

    std::vector<std::pair<ashiato::sync::ClientId, ashiato::BitBuffer>> payloads;
    ashiato::sync::ReplicationServerOptions options;
    options.bandwidth_limit_bytes_per_tick = 1024;
    options.prioritizer_interval_frames = 1;
    options.prioritizer = [&](ashiato::sync::ClientId client, ashiato::sync::ReplicationPriorityObject object) {
        ashiato::sync::ReplicationPriorityDecision decision;
        decision.priority =
            client == 1
            ? (object.entity == first ? 100.0f : 10.0f)
            : (object.entity == second ? 100.0f : 10.0f);
        return decision;
    };
    options.transport = [&](ashiato::sync::ClientId client, const ashiato::BitBuffer& payload) {
        payloads.push_back({client, payload});
    };

    ashiato::sync::ReplicationServer server(registry, options);
    REQUIRE(server.add_client(1));
    REQUIRE(server.add_client(2));
    server.tick(registry, server.options().fixed_dt_seconds);

    REQUIRE(payloads.size() == 2);
    for (const auto& sent : payloads) {
        const ServerUpdatePacket update = read_server_update(sent.second);
        REQUIRE(update.entities.size() == 2);
        REQUIRE(update.entities[0].components.size() == 1);
        const NetworkedPayload fields = read_networked_payload(update.entities[0].components[0].payload);
        REQUIRE_FALSE(fields.delta);
        if (sent.first == 1) {
            REQUIRE(fields.x == 10);
        } else {
            REQUIRE(sent.first == 2);
            REQUIRE(fields.x == 20);
        }
    }
}
