#include "test_protocol.hpp"

#ifndef ASHIATO_ENABLE_DEBUG_SERVER
#define ASHIATO_ENABLE_DEBUG_SERVER 0
#endif

#if ASHIATO_ENABLE_DEBUG_SERVER
#include "ashiato/debug_server.hpp"
#endif

#include <catch2/catch_approx.hpp>
#include <catch2/catch_test_macros.hpp>
#include <catch2/generators/catch_generators.hpp>

#include <algorithm>
#include <cmath>
#include <cstdint>
#include <limits>
#include <stdexcept>
#include <utility>
#include <vector>

using namespace ashiato_sync_tests;

namespace {

constexpr std::size_t client_destroy_tombstone_capacity = 65536;

ashiato::BitBuffer make_destroy_packet_for_wire_ids(
    ashiato::sync::SyncFrame frame,
    std::uint32_t packet_id,
    std::uint32_t first_wire_id,
    std::uint16_t count) {
    ashiato::BitBuffer packet;
    packet.write_bits(ashiato::sync::protocol::server_update_message, ashiato::sync::protocol::message_bits);
    packet.write_bits(frame, 32U);
    packet.write_bits(packet_id, ashiato::sync::protocol::server_packet_id_bits);
    packet.write_bits(0, 32U);
    packet.write_bits(count, 16U);
    for (std::uint16_t offset = 0; offset < count; ++offset) {
        packet.write_bool(true);
        ashiato::sync::protocol::write_network_entity_id(packet, first_wire_id + offset);
    }
    return packet;
}

}  // namespace

TEST_CASE("replication client applies full updates and queues ACKs") {
    ashiato::Registry server_registry;
    const ashiato::sync::SyncArchetypeId server_archetype = ashiato_sync_tests::define_position_archetype(server_registry);
    const ashiato::Entity server_entity = server_registry.create();
    REQUIRE(server_registry.add<Position>(server_entity, Position{1.0f, 2.0f}) != nullptr);

    std::vector<ashiato::BitBuffer> packets;
    ashiato::sync::ReplicationServerOptions server_options;
    server_options.transport = [&](ashiato::sync::ClientId, const ashiato::BitBuffer& packet) {
        packets.push_back(packet);
    };
    ashiato::sync::ReplicationServer server(server_registry, server_options);
    REQUIRE(server.add_client(1));
    REQUIRE(start_sync(server_registry, server_entity, server_archetype));
    server.tick(server_registry, server.options().fixed_dt_seconds);
    REQUIRE(packets.size() == 1);

    ashiato::Registry client_registry;
    const ashiato::sync::SyncArchetypeId client_archetype = ashiato_sync_tests::define_position_archetype(client_registry);
    REQUIRE(client_archetype == server_archetype);
    ashiato_sync_tests::configure_test_client_registry(client_registry, 1);

    ashiato::sync::ReplicationClient client(client_registry, ashiato_sync_tests::make_test_client_options(client_registry, {}));
    REQUIRE(client.receive(client_registry, packets[0]));

    const ashiato::Entity local = client.local_entity(first_allocated_client_entity_network_id(1));
    REQUIRE(local);
    REQUIRE(client.client_entity_network_id(local) == first_allocated_client_entity_network_id(1));
    REQUIRE(client_registry.alive(local));
    REQUIRE(client_registry.contains<Position>(local));
    REQUIRE(client_registry.get<Position>(local).x == 1.0f);
    REQUIRE(client_registry.get<Position>(local).y == 2.0f);
#if ASHIATO_ENABLE_DEBUG_SERVER
    const ashiato::DebugName* debug_name = client_registry.try_get<ashiato::DebugName>(local);
    REQUIRE(debug_name != nullptr);
    REQUIRE(debug_name->str() == "PositionActor");
#endif

    std::vector<ashiato::BitBuffer> ack_packets = client.drain_ack_packets();
    REQUIRE(ack_packets.size() == 1);
    std::vector<AckRecord> acks = read_acks(ack_packets[0]);
    REQUIRE(acks.size() == 1);
    REQUIRE(acks[0].packet_id != 0);
    REQUIRE(server.process_packet(server_registry, 1, ack_packets[0]));
}

TEST_CASE("replication client receives entities when replication starts and stops after server initialization") {
    ashiato::Registry server_registry;
    const ashiato::sync::SyncArchetypeId server_archetype = ashiato_sync_tests::define_position_archetype(server_registry);

    std::vector<ashiato::BitBuffer> packets;
    ashiato::sync::ReplicationServerOptions server_options;
    server_options.transport = [&](ashiato::sync::ClientId client, const ashiato::BitBuffer& packet) {
        REQUIRE(client == 1);
        packets.push_back(packet);
    };
    ashiato::sync::ReplicationServer server(server_registry, server_options);
    REQUIRE(server.add_client(1));

    REQUIRE(server.tick(server_registry, server.options().fixed_dt_seconds));
    REQUIRE(server.replicated_count() == 0);
    packets.clear();

    ashiato::Registry client_registry;
    const ashiato::sync::SyncArchetypeId client_archetype = ashiato_sync_tests::define_position_archetype(client_registry);
    REQUIRE(client_archetype == server_archetype);
    ashiato_sync_tests::configure_test_client_registry(client_registry, 1);
    ashiato::sync::ReplicationClient client(
        client_registry,
        ashiato_sync_tests::make_test_client_options(client_registry, {}));

    const ashiato::Entity server_entity = server_registry.create();
    REQUIRE(server_registry.add<Position>(server_entity, Position{3.0f, 4.0f}) != nullptr);
    REQUIRE(start_sync(server_registry, server_entity, server_archetype));

    REQUIRE(server.tick(server_registry, server.options().fixed_dt_seconds));
    REQUIRE(server.is_replicated(server_entity));
    REQUIRE(server.replicated_count() == 1);
    REQUIRE(packets.size() == 1);
    REQUIRE(client.receive(client_registry, packets.back()));

    const ashiato::sync::ClientEntityNetworkId client_network_id = first_allocated_client_entity_network_id(1);
    const ashiato::Entity local = client.local_entity(client_network_id);
    REQUIRE(local);
    REQUIRE(client_registry.alive(local));
    REQUIRE(client_registry.contains<Position>(local));
    REQUIRE(client_registry.get<Position>(local).x == 3.0f);
    REQUIRE(client_registry.get<Position>(local).y == 4.0f);

    for (ashiato::BitBuffer ack : client.drain_ack_packets()) {
        REQUIRE(server.process_packet(server_registry, 1, ack));
    }
    packets.clear();

    REQUIRE(server_registry.remove<ashiato::sync::Replicated>(server_entity));
    REQUIRE(server.tick(server_registry, server.options().fixed_dt_seconds));
    REQUIRE_FALSE(server.is_replicated(server_entity));
    REQUIRE(server.replicated_count() == 0);
    REQUIRE(packets.size() == 1);
    REQUIRE(client.receive(client_registry, packets.back()));
    REQUIRE_FALSE(client.local_entity(client_network_id));
    REQUIRE_FALSE(client_registry.alive(local));
}

TEST_CASE("replication client receives existing replicated entities when connecting late") {
    ashiato::Registry server_registry;
    const ashiato::sync::SyncArchetypeId server_archetype = ashiato_sync_tests::define_position_archetype(server_registry);
    const ashiato::Entity server_entity = server_registry.create();
    REQUIRE(server_registry.add<Position>(server_entity, Position{7.0f, 8.0f}) != nullptr);
    REQUIRE(start_sync(server_registry, server_entity, server_archetype));

    std::vector<ashiato::BitBuffer> packets;
    ashiato::sync::ReplicationServerOptions server_options;
    server_options.transport = [&](ashiato::sync::ClientId client, const ashiato::BitBuffer& packet) {
        REQUIRE(client == 1);
        packets.push_back(packet);
    };
    ashiato::sync::ReplicationServer server(server_registry, server_options);

    REQUIRE(server.tick(server_registry, server.options().fixed_dt_seconds));
    REQUIRE(server.is_replicated(server_entity));
    REQUIRE(server.replicated_count() == 1);
    REQUIRE(packets.empty());

    REQUIRE(server.add_client(1));
    REQUIRE(server.tick(server_registry, server.options().fixed_dt_seconds));
    REQUIRE(packets.size() == 1);

    ashiato::Registry client_registry;
    const ashiato::sync::SyncArchetypeId client_archetype = ashiato_sync_tests::define_position_archetype(client_registry);
    REQUIRE(client_archetype == server_archetype);
    ashiato_sync_tests::configure_test_client_registry(client_registry, 1);
    ashiato::sync::ReplicationClient client(
        client_registry,
        ashiato_sync_tests::make_test_client_options(client_registry, {}));

    REQUIRE(client.receive(client_registry, packets.back()));
    const ashiato::Entity local = client.local_entity(first_allocated_client_entity_network_id(1));
    REQUIRE(local);
    REQUIRE(client_registry.alive(local));
    REQUIRE(client_registry.contains<Position>(local));
    REQUIRE(client_registry.get<Position>(local).x == 7.0f);
    REQUIRE(client_registry.get<Position>(local).y == 8.0f);
}

TEST_CASE("replication client exposes server timing estimates after updates") {
    ashiato::Registry server_registry;
    const ashiato::sync::SyncArchetypeId server_archetype = ashiato_sync_tests::define_position_archetype(server_registry);
    const ashiato::Entity server_entity = server_registry.create();
    REQUIRE(server_registry.add<Position>(server_entity, Position{1.0f, 2.0f}) != nullptr);

    std::vector<ashiato::BitBuffer> packets;
    ashiato::sync::ReplicationServerOptions server_options;
    server_options.transport = [&](ashiato::sync::ClientId, const ashiato::BitBuffer& packet) {
        packets.push_back(packet);
    };
    ashiato::sync::ReplicationServer server(server_registry, server_options);
    REQUIRE(server.add_client(1));
    REQUIRE(start_sync(server_registry, server_entity, server_archetype));
    server.tick(server_registry, server.options().fixed_dt_seconds);
    REQUIRE(packets.size() == 1);

    ashiato::Registry client_registry;
    const ashiato::sync::SyncArchetypeId client_archetype = ashiato_sync_tests::define_position_archetype(client_registry);
    REQUIRE(client_archetype == server_archetype);
    ashiato_sync_tests::configure_test_client_registry(client_registry, 1);

    ashiato::sync::ReplicationClient client(client_registry, ashiato_sync_tests::make_test_client_options(client_registry, {}));
    REQUIRE(client.estimated_server_frame() == 0.0);
    REQUIRE(client.continuous_prediction_frames_ahead() == 0.0);
    REQUIRE(client.continuous_buffered_frames_behind() == 0.0);

    REQUIRE(receive_at_local_frame(client, client_registry, packets[0], 10));
    REQUIRE(client.estimated_server_frame() == Catch::Approx(10.0));
    REQUIRE(std::isfinite(client.continuous_prediction_frames_ahead()));
    REQUIRE(std::isfinite(client.continuous_buffered_frames_behind()));
}

TEST_CASE("replication client decodes ACKed delta updates") {
    ashiato::Registry server_registry;
    const ashiato::Entity position_component =
        ashiato::sync::register_sync_component<NetworkedPosition>(server_registry, "NetworkedPosition");
    const ashiato::sync::SyncArchetypeId server_archetype = ashiato::sync::define_archetype(
        server_registry,
        "NetworkedActor",
        {{position_component, ashiato::sync::ReplicationAudience::All}});
    const ashiato::Entity server_entity = server_registry.create();
    REQUIRE(server_registry.add<NetworkedPosition>(server_entity, NetworkedPosition{1.0f, 2.0f}) != nullptr);

    std::vector<ashiato::BitBuffer> packets;
    ashiato::sync::ReplicationServerOptions server_options;
    server_options.transport = [&](ashiato::sync::ClientId, const ashiato::BitBuffer& packet) {
        packets.push_back(packet);
    };
    ashiato::sync::ReplicationServer server(server_registry, server_options);
    REQUIRE(server.add_client(1));
    REQUIRE(start_sync(server_registry, server_entity, server_archetype));

    ashiato::Registry client_registry;
    const ashiato::Entity client_position =
        ashiato::sync::register_sync_component<NetworkedPosition>(client_registry, "NetworkedPosition");
    const ashiato::sync::SyncArchetypeId client_archetype = ashiato::sync::define_archetype(
        client_registry,
        "NetworkedActor",
        {{client_position, ashiato::sync::ReplicationAudience::All}});
    REQUIRE(client_archetype == server_archetype);
    ashiato_sync_tests::configure_test_client_registry(client_registry, 1);

    ashiato::sync::ReplicationClient client(client_registry, ashiato_sync_tests::make_test_client_options(client_registry, {}));
    server.tick(server_registry, server.options().fixed_dt_seconds);
    REQUIRE(client.receive(client_registry, packets.back()));
    for (const ashiato::BitBuffer& ack : client.drain_ack_packets()) {
        REQUIRE(server.process_packet(server_registry, 1, ack));
    }

    server_registry.write<NetworkedPosition>(server_entity) = NetworkedPosition{2.0f, 3.0f};
    server.tick(server_registry, server.options().fixed_dt_seconds);
    REQUIRE(client.receive(client_registry, packets.back()));

    const ashiato::Entity local = client.local_entity(first_allocated_client_entity_network_id(1));
    REQUIRE(local);
    REQUIRE(client_registry.get<NetworkedPosition>(local).x == 2.0f);
    REQUIRE(client_registry.get<NetworkedPosition>(local).y == 3.0f);
}

TEST_CASE("replication client decodes deltas against the encoded baseline frame") {
    ashiato::Registry server_registry;
    const ashiato::Entity position_component =
        ashiato::sync::register_sync_component<NetworkedPosition>(server_registry, "NetworkedPosition");
    const ashiato::sync::SyncArchetypeId server_archetype = ashiato::sync::define_archetype(
        server_registry,
        "NetworkedActor",
        {{position_component, ashiato::sync::ReplicationAudience::All}});
    const ashiato::Entity server_entity = server_registry.create();
    REQUIRE(server_registry.add<NetworkedPosition>(server_entity, NetworkedPosition{1.0f, 2.0f}) != nullptr);

    std::vector<ashiato::BitBuffer> packets;
    ashiato::sync::ReplicationServerOptions server_options;
    server_options.transport = [&](ashiato::sync::ClientId, const ashiato::BitBuffer& packet) {
        packets.push_back(packet);
    };
    ashiato::sync::ReplicationServer server(server_registry, server_options);
    REQUIRE(server.add_client(1));
    REQUIRE(start_sync(server_registry, server_entity, server_archetype));

    ashiato::Registry client_registry;
    const ashiato::Entity client_position =
        ashiato::sync::register_sync_component<NetworkedPosition>(client_registry, "NetworkedPosition");
    const ashiato::sync::SyncArchetypeId client_archetype = ashiato::sync::define_archetype(
        client_registry,
        "NetworkedActor",
        {{client_position, ashiato::sync::ReplicationAudience::All}});
    REQUIRE(client_archetype == server_archetype);
    ashiato_sync_tests::configure_test_client_registry(client_registry, 1);

    ashiato::sync::ReplicationClient client(client_registry, ashiato_sync_tests::make_test_client_options(client_registry, {}));
    server.tick(server_registry, server.options().fixed_dt_seconds);
    REQUIRE(client.receive(client_registry, packets.back()));
    for (const ashiato::BitBuffer& ack : client.drain_ack_packets()) {
        REQUIRE(server.process_packet(server_registry, 1, ack));
    }

    server_registry.write<NetworkedPosition>(server_entity) = NetworkedPosition{2.0f, 3.0f};
    server.tick(server_registry, server.options().fixed_dt_seconds);
    REQUIRE(client.receive(client_registry, packets.back()));
    REQUIRE(client.drain_ack_packets().size() == 1);

    server_registry.write<NetworkedPosition>(server_entity) = NetworkedPosition{3.0f, 4.0f};
    server.tick(server_registry, server.options().fixed_dt_seconds);
    REQUIRE(client.receive(client_registry, packets.back()));

    const ashiato::Entity local = client.local_entity(first_allocated_client_entity_network_id(1));
    REQUIRE(local);
    REQUIRE(client_registry.get<NetworkedPosition>(local).x == 3.0f);
    REQUIRE(client_registry.get<NetworkedPosition>(local).y == 4.0f);
}

TEST_CASE("snap mode applies authoritative values omitted by a delta against an older baseline") {
    ashiato::Registry server_registry;
    const ashiato::Entity server_position =
        ashiato::sync::register_sync_component<NetworkedPosition>(server_registry, "NetworkedPosition");
    const ashiato::Entity server_health =
        ashiato::sync::register_sync_component<Health>(server_registry, "Health");
    const ashiato::sync::SyncArchetypeId server_archetype = ashiato::sync::define_archetype(
        server_registry,
        "NetworkedActor",
        {
            {server_position, ashiato::sync::ReplicationAudience::All},
            {server_health, ashiato::sync::ReplicationAudience::All},
        });
    const ashiato::Entity server_entity = server_registry.create();
    REQUIRE(server_registry.add<NetworkedPosition>(server_entity, NetworkedPosition{}) != nullptr);
    REQUIRE(server_registry.add<Health>(server_entity, Health{107}) != nullptr);

    std::vector<ashiato::BitBuffer> packets;
    ashiato::sync::ReplicationServerOptions server_options;
    server_options.transport = [&](ashiato::sync::ClientId, const ashiato::BitBuffer& packet) {
        packets.push_back(packet);
    };
    ashiato::sync::ReplicationServer server(server_registry, server_options);
    REQUIRE(server.add_client(1));
    REQUIRE(start_sync(server_registry, server_entity, server_archetype));

    ashiato::Registry client_registry;
    const ashiato::Entity client_position =
        ashiato::sync::register_sync_component<NetworkedPosition>(client_registry, "NetworkedPosition");
    const ashiato::Entity client_health =
        ashiato::sync::register_sync_component<Health>(client_registry, "Health");
    const ashiato::sync::SyncArchetypeId client_archetype = ashiato::sync::define_archetype(
        client_registry,
        "NetworkedActor",
        {
            {client_position, ashiato::sync::ReplicationAudience::All},
            {client_health, ashiato::sync::ReplicationAudience::All},
        });
    REQUIRE(client_archetype == server_archetype);
    REQUIRE(configure_test_client_registry(client_registry, 1));
    ashiato::sync::ReplicationClient client(
        client_registry,
        make_test_client_options(client_registry, {}));

    REQUIRE(server.tick(server_registry, server.options().fixed_dt_seconds));
    REQUIRE(client.receive(client_registry, packets.back()));
    for (const ashiato::BitBuffer& ack : client.drain_ack_packets()) {
        REQUIRE(server.process_packet(server_registry, 1, ack));
    }

    server_registry.write<NetworkedPosition>(server_entity) = NetworkedPosition{1.0f, 1.0f};
    server_registry.write<Health>(server_entity) = Health{106};
    REQUIRE(server.tick(server_registry, server.options().fixed_dt_seconds));
    REQUIRE(client.receive(client_registry, packets.back()));
    REQUIRE(client.drain_ack_packets().size() == 1U);

    server_registry.write<NetworkedPosition>(server_entity) = NetworkedPosition{2.0f, 2.0f};
    server_registry.write<Health>(server_entity) = Health{107};
    REQUIRE(server.tick(server_registry, server.options().fixed_dt_seconds));
    REQUIRE(client.receive(client_registry, packets.back()));

    const ashiato::Entity local = client.local_entity(first_allocated_client_entity_network_id(1));
    REQUIRE(local);
    CHECK(client_registry.get<NetworkedPosition>(local).x == 2.0f);
    CHECK(client_registry.get<NetworkedPosition>(local).y == 2.0f);
    CHECK(client_registry.get<Health>(local).value == 107);

    REQUIRE(client_registry.clear_dirty<NetworkedPosition>(local));
    REQUIRE(client_registry.clear_dirty<Health>(local));
    for (const ashiato::BitBuffer& ack : client.drain_ack_packets()) {
        REQUIRE(server.process_packet(server_registry, 1, ack));
    }

    server_registry.write<NetworkedPosition>(server_entity) = NetworkedPosition{3.0f, 3.0f};
    REQUIRE(server.tick(server_registry, server.options().fixed_dt_seconds));
    REQUIRE(client.receive(client_registry, packets.back()));

    CHECK(client_registry.is_dirty<NetworkedPosition>(local));
    CHECK_FALSE(client_registry.is_dirty<Health>(local));
    CHECK(client_registry.get<Health>(local).value == 107);
}

TEST_CASE("replication client reports missing prediction rollback traits for default predict mode") {
    ashiato::Registry client_registry;
    const ashiato::sync::SyncArchetypeId client_archetype = ashiato_sync_tests::define_position_archetype(client_registry);
    REQUIRE(client_archetype.value == 0);
    ashiato_sync_tests::configure_test_client_registry(client_registry, 1);

    ashiato::sync::ReplicationClientOptions options;
    options.entities.default_mode = ashiato::sync::ReplicationClientMode::Predict;
    ashiato::sync::ReplicationClient client(client_registry, ashiato_sync_tests::make_test_client_options(client_registry, options));

    const ashiato::Entity server_entity{42};
    try {
        (void)client.receive(client_registry, make_position_packet(1, {{server_entity, Position{1.0f, 2.0f}}}));
        FAIL("expected missing prediction rollback trait error");
    } catch (const ashiato::sync::ClientError& error) {
        REQUIRE(error.status() == ashiato::sync::ClientStatus::MissingPredictionRollbackTrait);
    }
}

TEST_CASE("replication client reports missing prediction rollback traits for selector predict mode") {
    ashiato::Registry client_registry;
    const ashiato::sync::SyncArchetypeId client_archetype = ashiato_sync_tests::define_position_archetype(client_registry);
    REQUIRE(client_archetype.value == 0);
    ashiato_sync_tests::configure_test_client_registry(client_registry, 1);

    ashiato::sync::ReplicationClientOptions options;
    options.entities.default_mode = ashiato::sync::ReplicationClientMode::Snap;
    options.entities.mode_selector = [](const ashiato::sync::ReplicatedEntityUpdateView&) {
        return ashiato::sync::ReplicationClientMode::Predict;
    };
    ashiato::sync::ReplicationClient client(client_registry, ashiato_sync_tests::make_test_client_options(client_registry, options));

    const ashiato::Entity server_entity{42};
    try {
        (void)client.receive(client_registry, make_position_packet(1, {{server_entity, Position{1.0f, 2.0f}}}));
        FAIL("expected missing prediction rollback trait error");
    } catch (const ashiato::sync::ClientError& error) {
        REQUIRE(error.status() == ashiato::sync::ClientStatus::MissingPredictionRollbackTrait);
    }
}

TEST_CASE("replication client decodes mixed-baseline deltas in one packet") {
    ashiato::Registry server_registry;
    const ashiato::Entity position_component =
        ashiato::sync::register_sync_component<NetworkedPosition>(server_registry, "NetworkedPosition");
    const ashiato::sync::SyncArchetypeId server_archetype = ashiato::sync::define_archetype(
        server_registry,
        "NetworkedActor",
        {{position_component, ashiato::sync::ReplicationAudience::All}});
    const ashiato::Entity first = server_registry.create();
    const ashiato::Entity second = server_registry.create();
    REQUIRE(server_registry.add<NetworkedPosition>(first, NetworkedPosition{1.0f, 1.0f}) != nullptr);
    REQUIRE(server_registry.add<NetworkedPosition>(second, NetworkedPosition{10.0f, 10.0f}) != nullptr);

    std::vector<ashiato::BitBuffer> packets;
    ashiato::sync::ReplicationServerOptions server_options;
    server_options.bandwidth_limit_bytes_per_tick = 1024;
    server_options.mtu_bytes = 1200;
    server_options.transport = [&](ashiato::sync::ClientId, const ashiato::BitBuffer& packet) {
        packets.push_back(packet);
    };
    ashiato::sync::ReplicationServer server(server_registry, server_options);
    REQUIRE(server.add_client(1));
    REQUIRE(start_sync(server_registry, first, server_archetype));
    REQUIRE(start_sync(server_registry, second, server_archetype));

    ashiato::Registry client_registry;
    const ashiato::Entity client_position =
        ashiato::sync::register_sync_component<NetworkedPosition>(client_registry, "NetworkedPosition");
    const ashiato::sync::SyncArchetypeId client_archetype = ashiato::sync::define_archetype(
        client_registry,
        "NetworkedActor",
        {{client_position, ashiato::sync::ReplicationAudience::All}});
    REQUIRE(client_archetype == server_archetype);
    ashiato_sync_tests::configure_test_client_registry(client_registry, 1);

    ashiato::sync::ReplicationClient client(client_registry, ashiato_sync_tests::make_test_client_options(client_registry, {}));
    server.tick(server_registry, server.options().fixed_dt_seconds);
    REQUIRE(packets.size() == 1);
    const UpdatePacket full_update = read_update(packets.back());
    REQUIRE(full_update.records.size() == 2);
    std::uint32_t first_wire_network_id = 0;
    std::uint32_t second_wire_network_id = 0;
    first_wire_network_id = full_update.records[0].network_id;
    second_wire_network_id = full_update.records[1].network_id;
    REQUIRE(first_wire_network_id != 0);
    REQUIRE(second_wire_network_id != 0);
    REQUIRE(client.receive(client_registry, packets.back()));
    for (const ashiato::BitBuffer& ack : client.drain_ack_packets()) {
        REQUIRE(server.process_packet(server_registry, 1, ack));
    }

    server_registry.write<NetworkedPosition>(first) = NetworkedPosition{2.0f, 2.0f};
    server_registry.write<NetworkedPosition>(second) = NetworkedPosition{11.0f, 11.0f};
    packets.clear();
    server.tick(server_registry, server.options().fixed_dt_seconds);
    REQUIRE(packets.size() == 1);
    const UpdatePacket frame83 = read_update(packets.back());
    REQUIRE(frame83.records.size() == 2);
    REQUIRE(client.receive(client_registry, packets.back()));
    REQUIRE(client.drain_ack_packets().size() == 1);

    REQUIRE(server.acknowledge_entity(1, first, frame83.frame));

    server_registry.write<NetworkedPosition>(first) = NetworkedPosition{3.0f, 3.0f};
    server_registry.write<NetworkedPosition>(second) = NetworkedPosition{12.0f, 12.0f};
    packets.clear();
    server.tick(server_registry, server.options().fixed_dt_seconds);
    REQUIRE(packets.size() == 1);
    const UpdatePacket frame84 = read_update(packets.back());
    REQUIRE(frame84.records.size() == 2);

    bool saw_first_from_83 = false;
    bool saw_second_from_full = false;
    for (const UpdateRecord& record : frame84.records) {
        REQUIRE_FALSE(record.destroy);
        REQUIRE_FALSE(record.full);
        if (record.network_id == first_wire_network_id) {
            saw_first_from_83 = record.baseline_frame == frame83.frame;
        } else if (record.network_id == second_wire_network_id) {
            saw_second_from_full = record.baseline_frame == full_update.frame;
        }
    }
    REQUIRE(saw_first_from_83);
    REQUIRE(saw_second_from_full);

    REQUIRE(client.receive(client_registry, packets.back()));
    const ashiato::Entity first_local = client.local_entity(test_client_entity_network_id(1, first_wire_network_id));
    const ashiato::Entity second_local = client.local_entity(test_client_entity_network_id(1, second_wire_network_id));
    REQUIRE(client_registry.get<NetworkedPosition>(first_local).x == 3.0f);
    REQUIRE(client_registry.get<NetworkedPosition>(first_local).y == 3.0f);
    REQUIRE(client_registry.get<NetworkedPosition>(second_local).x == 12.0f);
    REQUIRE(client_registry.get<NetworkedPosition>(second_local).y == 12.0f);
}

TEST_CASE("replication client rejects invalid deltas without ACKing") {
    ashiato::Registry registry;
    const ashiato::Entity position =
        ashiato::sync::register_sync_component<NetworkedPosition>(registry, "NetworkedPosition");
    const ashiato::sync::SyncArchetypeId archetype = ashiato::sync::define_archetype(
        registry,
        "NetworkedActor",
        {{position, ashiato::sync::ReplicationAudience::All}});
    REQUIRE(archetype.value == 0);
    ashiato_sync_tests::configure_test_client_registry(registry, 1);

    ashiato::BitBuffer packet;
    packet.write_bits(ashiato::sync::protocol::server_update_message, ashiato::sync::protocol::message_bits);
    packet.write_bits(1, 32U);
    packet.write_bits(1, ashiato::sync::protocol::server_packet_id_bits);
    packet.write_bits(0, 32U);
    packet.write_bits(1, 16U);
    packet.write_bool(false);
    ashiato::sync::protocol::write_network_entity_id(packet, 1);
    packet.write_bool(false);
    packet.write_bits(1, 16U);
    packet.write_bits(0, ashiato::sync::protocol::bits_for_range(2U));
    packet.write_bits(17, 32U);
    packet.write_bool(true);
    packet.write_bits(10, 8U);
    packet.write_bits(10, 8U);

    ashiato::sync::ReplicationClient client(registry, ashiato_sync_tests::make_test_client_options(registry, {}));
    REQUIRE_FALSE(client.receive(registry, packet));
    REQUIRE(client.pending_ack_count() == 0);
    REQUIRE(client.drain_ack_packets().empty());
}

TEST_CASE("replication client rejects malformed update packets without ACKing") {
    ashiato::Registry registry;
    const ashiato::sync::SyncArchetypeId archetype = ashiato_sync_tests::define_position_archetype(registry);
    REQUIRE(archetype.value == 0);
    ashiato_sync_tests::configure_test_client_registry(registry, 1);
    ashiato::sync::ReplicationClient client(registry, ashiato_sync_tests::make_test_client_options(registry, {}));

    ashiato::BitBuffer empty;
    REQUIRE_FALSE(client.receive(registry, empty));

    ashiato::BitBuffer wrong_message;
    wrong_message.write_bits(ashiato::sync::protocol::client_ack_message, ashiato::sync::protocol::message_bits);
    REQUIRE_FALSE(client.receive(registry, wrong_message));

    ashiato::BitBuffer truncated_header;
    truncated_header.write_bits(ashiato::sync::protocol::server_update_message, ashiato::sync::protocol::message_bits);
    truncated_header.write_bits(1, 32U);
    REQUIRE_FALSE(client.receive(registry, truncated_header));

    ashiato::BitBuffer invalid_archetype;
    invalid_archetype.write_bits(ashiato::sync::protocol::server_update_message, ashiato::sync::protocol::message_bits);
    invalid_archetype.write_bits(1, 32U);
    invalid_archetype.write_bits(1, ashiato::sync::protocol::server_packet_id_bits);
    invalid_archetype.write_bits(0, 32U);
    invalid_archetype.write_bits(1, 16U);
    invalid_archetype.write_bool(false);
    ashiato::sync::protocol::write_network_entity_id(invalid_archetype, 1);
    invalid_archetype.write_bool(true);
    invalid_archetype.write_bits(99, 32U);
    REQUIRE_FALSE(client.receive(registry, invalid_archetype));

    REQUIRE(client.pending_ack_count() == 0);
    REQUIRE(client.drain_ack_packets().empty());
}

TEST_CASE("replication client rejects malformed entity records without ACKing") {
    ashiato::Registry registry;
    const ashiato::sync::SyncArchetypeId archetype = ashiato_sync_tests::define_position_archetype(registry);
    REQUIRE(archetype.value == 0);
    ashiato_sync_tests::configure_test_client_registry(registry, 1);

    auto make_update_prefix = [](std::uint16_t record_count = 1) {
        ashiato::BitBuffer packet;
        packet.write_bits(ashiato::sync::protocol::server_update_message, ashiato::sync::protocol::message_bits);
        packet.write_bits(1, 32U);
        packet.write_bits(1, ashiato::sync::protocol::server_packet_id_bits);
        packet.write_bits(0, 32U);
        packet.write_bits(record_count, 16U);
        return packet;
    };

    {
        ashiato::BitBuffer packet = make_update_prefix();
        packet.write_bool(false);
        ashiato::sync::protocol::write_network_entity_id(packet, 0);
        packet.write_bool(true);
        packet.write_bits(0, 32U);

        ashiato::sync::ReplicationClient client(registry, ashiato_sync_tests::make_test_client_options(registry, {}));
        REQUIRE_FALSE(client.receive(registry, packet));
        REQUIRE(client.pending_ack_count() == 0);
    }

    {
        ashiato::BitBuffer packet = make_update_prefix();
        packet.write_bool(false);
        ashiato::sync::protocol::write_network_entity_id(packet, 1);
        packet.write_bool(true);
        packet.write_bits(0, 32U);
        packet.write_bool(false);
        packet.write_bits(1, 16U);
        packet.write_bits(2, ashiato::sync::protocol::bits_for_range(2U));

        ashiato::sync::ReplicationClient client(registry, ashiato_sync_tests::make_test_client_options(registry, {}));
        REQUIRE_FALSE(client.receive(registry, packet));
        REQUIRE(client.pending_ack_count() == 0);
    }

    {
        ashiato::BitBuffer packet = make_update_prefix();
        packet.write_bool(false);
        ashiato::sync::protocol::write_network_entity_id(packet, 1);
        packet.write_bool(true);
        packet.write_bits(0, 32U);
        packet.write_bool(false);
        packet.write_bits(1, 16U);
        packet.write_bits(1, ashiato::sync::protocol::bits_for_range(2U));
        packet.write_bits(1, 8U);

        ashiato::sync::ReplicationClient client(registry, ashiato_sync_tests::make_test_client_options(registry, {}));
        REQUIRE_FALSE(client.receive(registry, packet));
        REQUIRE(client.pending_ack_count() == 0);
    }
}

TEST_CASE("replication client applies destroy records and ACKs tombstones") {
    ashiato::Registry server_registry;
    const ashiato::sync::SyncArchetypeId server_archetype = ashiato_sync_tests::define_position_archetype(server_registry);
    const ashiato::Entity server_entity = server_registry.create();
    REQUIRE(server_registry.add<Position>(server_entity, Position{1.0f, 2.0f}) != nullptr);

    std::vector<ashiato::BitBuffer> packets;
    ashiato::sync::ReplicationServerOptions server_options;
    server_options.transport = [&](ashiato::sync::ClientId, const ashiato::BitBuffer& packet) {
        packets.push_back(packet);
    };
    ashiato::sync::ReplicationServer server(server_registry, server_options);
    REQUIRE(server.add_client(1));
    REQUIRE(start_sync(server_registry, server_entity, server_archetype));

    ashiato::Registry client_registry;
    const ashiato::sync::SyncArchetypeId client_archetype = ashiato_sync_tests::define_position_archetype(client_registry);
    REQUIRE(client_archetype == server_archetype);
    ashiato_sync_tests::configure_test_client_registry(client_registry, 1);
    ashiato::sync::ReplicationClient client(client_registry, ashiato_sync_tests::make_test_client_options(client_registry, {}));

    server.tick(server_registry, server.options().fixed_dt_seconds);
    REQUIRE(client.receive(client_registry, packets.back()));
    const ashiato::Entity local = client.local_entity(first_allocated_client_entity_network_id(1));
    REQUIRE(local);
    for (const ashiato::BitBuffer& ack : client.drain_ack_packets()) {
        REQUIRE(server.process_packet(server_registry, 1, ack));
    }

    REQUIRE(server_registry.destroy(server_entity));
    server.tick(server_registry, server.options().fixed_dt_seconds);
    REQUIRE(client.receive(client_registry, packets.back()));
    REQUIRE_FALSE(client_registry.alive(local));

    std::vector<ashiato::BitBuffer> destroy_acks = client.drain_ack_packets();
    REQUIRE(destroy_acks.size() == 1);
    std::vector<AckRecord> acks = read_acks(destroy_acks[0]);
    REQUIRE(acks.size() == 1);
    REQUIRE(acks[0].packet_id != 0);

    server.tick(server_registry, server.options().fixed_dt_seconds);
    REQUIRE(server.process_packet(server_registry, 1, destroy_acks[0]));
    const std::size_t packets_after_ack = packets.size();
    server.tick(server_registry, server.options().fixed_dt_seconds);
    REQUIRE(packets.size() == packets_after_ack);
}

TEST_CASE("replication client packs ACKs within the configured MTU") {
    ashiato::Registry client_registry;
    ashiato::sync::ReplicationClient client(client_registry, ashiato_sync_tests::make_test_client_options(client_registry, ashiato::sync::ReplicationClientOptions{ashiato::sync::ReplicationClientNetworkOptions{16}}));
    ashiato::Registry server_registry;
    const ashiato::sync::SyncArchetypeId server_archetype = ashiato_sync_tests::define_position_archetype(server_registry);

    std::vector<ashiato::Entity> server_entities;
    for (int i = 0; i < 3; ++i) {
        const ashiato::Entity entity = server_registry.create();
        REQUIRE(server_registry.add<Position>(entity, Position{static_cast<float>(i), 0.0f}) != nullptr);
        REQUIRE(start_sync(server_registry, entity, server_archetype));
        server_entities.push_back(entity);
    }

    std::vector<ashiato::BitBuffer> packets;
    ashiato::sync::ReplicationServerOptions server_options;
    server_options.mtu_bytes = 29;
    server_options.transport = [&](ashiato::sync::ClientId, const ashiato::BitBuffer& packet) {
        packets.push_back(packet);
    };
    ashiato::sync::ReplicationServer server(server_registry, server_options);
    REQUIRE(server.add_client(1));
    server.tick(server_registry, server.options().fixed_dt_seconds);

    const ashiato::sync::SyncArchetypeId client_archetype = ashiato_sync_tests::define_position_archetype(client_registry);
    REQUIRE(client_archetype == server_archetype);
    ashiato_sync_tests::configure_test_client_registry(client_registry, 1);

    for (const ashiato::BitBuffer& packet : packets) {
        REQUIRE(client.receive(client_registry, packet));
    }

    std::vector<ashiato::BitBuffer> acks = client.drain_ack_packets();
    REQUIRE(acks.size() == 1);
    for (const ashiato::BitBuffer& ack : acks) {
        REQUIRE(ack.byte_size() <= 16);
        REQUIRE(read_acks(ack).size() == 3);
    }
}

TEST_CASE("replication client retains ACKs that cannot fit the configured MTU") {
    ashiato::Registry client_registry;
    ashiato::sync::ReplicationClient client(client_registry, ashiato_sync_tests::make_test_client_options(client_registry, ashiato::sync::ReplicationClientOptions{ashiato::sync::ReplicationClientNetworkOptions{1}}));
    ashiato::Registry server_registry;
    const ashiato::sync::SyncArchetypeId server_archetype = ashiato_sync_tests::define_position_archetype(server_registry);
    const ashiato::Entity server_entity = server_registry.create();
    REQUIRE(server_registry.add<Position>(server_entity, Position{1.0f, 2.0f}) != nullptr);

    std::vector<ashiato::BitBuffer> packets;
    ashiato::sync::ReplicationServerOptions server_options;
    server_options.transport = [&](ashiato::sync::ClientId, const ashiato::BitBuffer& packet) {
        packets.push_back(packet);
    };
    ashiato::sync::ReplicationServer server(server_registry, server_options);
    REQUIRE(server.add_client(1));
    REQUIRE(start_sync(server_registry, server_entity, server_archetype));
    server.tick(server_registry, server.options().fixed_dt_seconds);

    const ashiato::sync::SyncArchetypeId client_archetype = ashiato_sync_tests::define_position_archetype(client_registry);
    REQUIRE(client_archetype == server_archetype);
    ashiato_sync_tests::configure_test_client_registry(client_registry, 1);

    REQUIRE(client.receive(client_registry, packets.back()));
    REQUIRE(client.pending_ack_count() == 1);
    REQUIRE(client.drain_ack_packets().empty());
    REQUIRE(client.pending_ack_count() == 1);
}

TEST_CASE("replication client rejects duplicate full updates without ACKing again") {
    ashiato::Registry server_registry;
    const ashiato::sync::SyncArchetypeId server_archetype = ashiato_sync_tests::define_position_archetype(server_registry);
    const ashiato::Entity server_entity = server_registry.create();
    REQUIRE(server_registry.add<Position>(server_entity, Position{1.0f, 2.0f}) != nullptr);

    std::vector<ashiato::BitBuffer> packets;
    ashiato::sync::ReplicationServerOptions server_options;
    server_options.transport = [&](ashiato::sync::ClientId, const ashiato::BitBuffer& packet) {
        packets.push_back(packet);
    };
    ashiato::sync::ReplicationServer server(server_registry, server_options);
    REQUIRE(server.add_client(1));
    REQUIRE(start_sync(server_registry, server_entity, server_archetype));
    server.tick(server_registry, server.options().fixed_dt_seconds);

    ashiato::Registry client_registry;
    const ashiato::sync::SyncArchetypeId client_archetype = ashiato_sync_tests::define_position_archetype(client_registry);
    REQUIRE(client_archetype == server_archetype);
    ashiato_sync_tests::configure_test_client_registry(client_registry, 1);

    ashiato::sync::ReplicationClient client(client_registry, ashiato_sync_tests::make_test_client_options(client_registry, {}));
    REQUIRE(client.receive(client_registry, packets.back()));
    REQUIRE(client.drain_ack_packets().size() == 1);
    REQUIRE_FALSE(client.receive(client_registry, packets.back()));
    REQUIRE(client.pending_ack_count() == 0);
}

TEST_CASE("replication clients recover independently when one misses ACK processing") {
    ashiato::Registry server_registry;
    const ashiato::Entity server_position =
        ashiato::sync::register_sync_component<NetworkedPosition>(server_registry, "NetworkedPosition");
    const ashiato::sync::SyncArchetypeId server_archetype = ashiato::sync::define_archetype(
        server_registry,
        "NetworkedActor",
        {{server_position, ashiato::sync::ReplicationAudience::All}});
    const ashiato::Entity server_entity = server_registry.create();
    REQUIRE(server_registry.add<NetworkedPosition>(server_entity, NetworkedPosition{1.0f, 2.0f}) != nullptr);

    std::vector<std::pair<ashiato::sync::ClientId, ashiato::BitBuffer>> packets;
    ashiato::sync::ReplicationServerOptions server_options;
    server_options.bandwidth_limit_bytes_per_tick = 1024;
    server_options.transport = [&](ashiato::sync::ClientId client, const ashiato::BitBuffer& packet) {
        packets.push_back({client, packet});
    };
    ashiato::sync::ReplicationServer server(server_registry, server_options);
    REQUIRE(server.add_client(1));
    REQUIRE(server.add_client(2));
    REQUIRE(start_sync(server_registry, server_entity, server_archetype));

    ashiato::Registry client_one_registry;
    const ashiato::Entity client_one_position =
        ashiato::sync::register_sync_component<NetworkedPosition>(client_one_registry, "NetworkedPosition");
    const ashiato::sync::SyncArchetypeId client_one_archetype = ashiato::sync::define_archetype(
        client_one_registry,
        "NetworkedActor",
        {{client_one_position, ashiato::sync::ReplicationAudience::All}});
    REQUIRE(client_one_archetype == server_archetype);
    ashiato_sync_tests::configure_test_client_registry(client_one_registry, 1);

    ashiato::Registry client_two_registry;
    const ashiato::Entity client_two_position =
        ashiato::sync::register_sync_component<NetworkedPosition>(client_two_registry, "NetworkedPosition");
    const ashiato::sync::SyncArchetypeId client_two_archetype = ashiato::sync::define_archetype(
        client_two_registry,
        "NetworkedActor",
        {{client_two_position, ashiato::sync::ReplicationAudience::All}});
    REQUIRE(client_two_archetype == server_archetype);
    ashiato_sync_tests::configure_test_client_registry(client_two_registry, 2);

    ashiato::sync::ReplicationClient client_one(client_one_registry, ashiato_sync_tests::make_test_client_options(client_one_registry, {}));
    ashiato::sync::ReplicationClient client_two(client_two_registry, ashiato_sync_tests::make_test_client_options(client_two_registry, {}));

    server.tick(server_registry, server.options().fixed_dt_seconds);
    REQUIRE(packets.size() == 2);
    REQUIRE(client_one.receive(client_one_registry, packet_for(packets, 1)));
    REQUIRE(client_two.receive(client_two_registry, packet_for(packets, 2)));
    for (const ashiato::BitBuffer& ack : client_one.drain_ack_packets()) {
        REQUIRE(server.process_packet(server_registry, 1, ack));
    }
    REQUIRE(client_two.drain_ack_packets().size() == 1);

    server_registry.write<NetworkedPosition>(server_entity) = NetworkedPosition{2.0f, 3.0f};
    packets.clear();
    server.tick(server_registry, server.options().fixed_dt_seconds);
    REQUIRE(packets.size() == 2);

    const UpdatePacket client_one_update = read_update(packet_for(packets, 1));
    const UpdatePacket client_two_update = read_update(packet_for(packets, 2));
    REQUIRE(client_one_update.records.size() == 1);
    REQUIRE(client_two_update.records.size() == 1);
    REQUIRE_FALSE(client_one_update.records[0].full);
    REQUIRE(client_two_update.records[0].full);
    REQUIRE(client_one.receive(client_one_registry, packet_for(packets, 1)));
    REQUIRE(client_two.receive(client_two_registry, packet_for(packets, 2)));

    for (const ashiato::BitBuffer& ack : client_one.drain_ack_packets()) {
        REQUIRE(server.process_packet(server_registry, 1, ack));
    }
    for (const ashiato::BitBuffer& ack : client_two.drain_ack_packets()) {
        REQUIRE(server.process_packet(server_registry, 2, ack));
    }

    const ashiato::Entity client_one_local = client_one.local_entity(first_allocated_client_entity_network_id(1));
    const ashiato::Entity client_two_local = client_two.local_entity(first_allocated_client_entity_network_id(2));
    REQUIRE(client_one_registry.get<NetworkedPosition>(client_one_local).x == 2.0f);
    REQUIRE(client_one_registry.get<NetworkedPosition>(client_one_local).y == 3.0f);
    REQUIRE(client_two_registry.get<NetworkedPosition>(client_two_local).x == 2.0f);
    REQUIRE(client_two_registry.get<NetworkedPosition>(client_two_local).y == 3.0f);

    server_registry.write<NetworkedPosition>(server_entity) = NetworkedPosition{4.0f, 5.0f};
    packets.clear();
    server.tick(server_registry, server.options().fixed_dt_seconds);
    REQUIRE(packets.size() == 2);
    REQUIRE_FALSE(read_update(packet_for(packets, 1)).records[0].full);
    REQUIRE_FALSE(read_update(packet_for(packets, 2)).records[0].full);
}

TEST_CASE("replication clients ACK destroy records independently") {
    ashiato::Registry server_registry;
    const ashiato::sync::SyncArchetypeId server_archetype = ashiato_sync_tests::define_position_archetype(server_registry);
    const ashiato::Entity server_entity = server_registry.create();
    REQUIRE(server_registry.add<Position>(server_entity, Position{1.0f, 2.0f}) != nullptr);

    std::vector<std::pair<ashiato::sync::ClientId, ashiato::BitBuffer>> packets;
    ashiato::sync::ReplicationServerOptions server_options;
    server_options.bandwidth_limit_bytes_per_tick = 1024;
    server_options.transport = [&](ashiato::sync::ClientId client, const ashiato::BitBuffer& packet) {
        packets.push_back({client, packet});
    };
    ashiato::sync::ReplicationServer server(server_registry, server_options);
    REQUIRE(server.add_client(1));
    REQUIRE(server.add_client(2));
    REQUIRE(start_sync(server_registry, server_entity, server_archetype));

    ashiato::Registry client_one_registry;
    const ashiato::sync::SyncArchetypeId client_one_archetype =
        ashiato_sync_tests::define_position_archetype(client_one_registry);
    REQUIRE(client_one_archetype == server_archetype);
    ashiato_sync_tests::configure_test_client_registry(client_one_registry, 1);

    ashiato::Registry client_two_registry;
    const ashiato::sync::SyncArchetypeId client_two_archetype =
        ashiato_sync_tests::define_position_archetype(client_two_registry);
    REQUIRE(client_two_archetype == server_archetype);
    ashiato_sync_tests::configure_test_client_registry(client_two_registry, 2);

    ashiato::sync::ReplicationClient client_one(client_one_registry, ashiato_sync_tests::make_test_client_options(client_one_registry, {}));
    ashiato::sync::ReplicationClient client_two(client_two_registry, ashiato_sync_tests::make_test_client_options(client_two_registry, {}));

    server.tick(server_registry, server.options().fixed_dt_seconds);
    REQUIRE(packets.size() == 2);
    REQUIRE(client_one.receive(client_one_registry, packet_for(packets, 1)));
    REQUIRE(client_two.receive(client_two_registry, packet_for(packets, 2)));
    const ashiato::Entity client_one_local = client_one.local_entity(first_allocated_client_entity_network_id(1));
    const ashiato::Entity client_two_local = client_two.local_entity(first_allocated_client_entity_network_id(2));
    REQUIRE(client_one_local);
    REQUIRE(client_two_local);
    for (const ashiato::BitBuffer& ack : client_one.drain_ack_packets()) {
        REQUIRE(server.process_packet(server_registry, 1, ack));
    }
    for (const ashiato::BitBuffer& ack : client_two.drain_ack_packets()) {
        REQUIRE(server.process_packet(server_registry, 2, ack));
    }

    REQUIRE(server_registry.destroy(server_entity));
    packets.clear();
    server.tick(server_registry, server.options().fixed_dt_seconds);
    REQUIRE(packets.size() == 2);
    REQUIRE(client_one.receive(client_one_registry, packet_for(packets, 1)));
    REQUIRE(client_two.receive(client_two_registry, packet_for(packets, 2)));
    REQUIRE_FALSE(client_one_registry.alive(client_one_local));
    REQUIRE_FALSE(client_two_registry.alive(client_two_local));

    std::vector<ashiato::BitBuffer> client_one_acks = client_one.drain_ack_packets();
    std::vector<ashiato::BitBuffer> client_two_acks = client_two.drain_ack_packets();
    REQUIRE(client_one_acks.size() == 1);
    REQUIRE(client_two_acks.size() == 1);
    REQUIRE(read_acks(client_one_acks[0]).size() == 1);
    REQUIRE(read_acks(client_two_acks[0]).size() == 1);
    REQUIRE(server.process_packet(server_registry, 1, client_one_acks[0]));

    packets.clear();
    server.tick(server_registry, server.options().fixed_dt_seconds);
    REQUIRE(packets.size() == 1);
    REQUIRE(packets[0].first == 2);
    REQUIRE(server.process_packet(server_registry, 2, client_two_acks[0]));

    packets.clear();
    server.tick(server_registry, server.options().fixed_dt_seconds);
    REQUIRE(packets.empty());
}

TEST_CASE("replication client reconciles components when owner visibility changes") {
    ashiato::Registry server_registry;
    const ashiato::Entity server_position =
        ashiato::sync::register_sync_component<NetworkedPosition>(server_registry, "NetworkedPosition");
    const ashiato::Entity server_health = ashiato::sync::register_sync_component<Health>(server_registry, "Health");
    const ashiato::Entity server_public_state =
        ashiato::sync::register_sync_component<BandwidthProbe>(server_registry, "PublicState");
    const ashiato::sync::SyncArchetypeId server_archetype = ashiato::sync::define_archetype(
        server_registry,
        "OwnedActor",
        {
            {server_position, ashiato::sync::ReplicationAudience::All},
            {server_health, ashiato::sync::ReplicationAudience::Owner},
            {server_public_state, ashiato::sync::ReplicationAudience::EveryoneExceptOwner},
        });
    const ashiato::Entity server_entity = server_registry.create();
    REQUIRE(server_registry.add<NetworkedPosition>(server_entity, NetworkedPosition{1.0f, 2.0f}) != nullptr);
    REQUIRE(server_registry.add<Health>(server_entity, Health{42}) != nullptr);
    REQUIRE(server_registry.add<BandwidthProbe>(server_entity, BandwidthProbe{7}) != nullptr);
    REQUIRE(ashiato::sync::set_owner(server_registry, server_entity, 1));

    std::vector<std::pair<ashiato::sync::ClientId, ashiato::BitBuffer>> packets;
    ashiato::sync::ReplicationServerOptions server_options;
    server_options.transport = [&](ashiato::sync::ClientId client, const ashiato::BitBuffer& packet) {
        packets.push_back({client, packet});
    };
    ashiato::sync::ReplicationServer server(server_registry, server_options);
    REQUIRE(server.add_client(1));
    REQUIRE(server.add_client(2));
    REQUIRE(start_sync(server_registry, server_entity, server_archetype));

    ashiato::Registry client_one_registry;
    const ashiato::Entity client_one_position =
        ashiato::sync::register_sync_component<NetworkedPosition>(client_one_registry, "NetworkedPosition");
    const ashiato::Entity client_one_health = ashiato::sync::register_sync_component<Health>(client_one_registry, "Health");
    const ashiato::Entity client_one_public_state =
        ashiato::sync::register_sync_component<BandwidthProbe>(client_one_registry, "PublicState");
    const ashiato::sync::SyncArchetypeId client_one_archetype = ashiato::sync::define_archetype(
        client_one_registry,
        "OwnedActor",
        {
            {client_one_position, ashiato::sync::ReplicationAudience::All},
            {client_one_health, ashiato::sync::ReplicationAudience::Owner},
            {client_one_public_state, ashiato::sync::ReplicationAudience::EveryoneExceptOwner},
        });
    REQUIRE(client_one_archetype == server_archetype);
    ashiato_sync_tests::configure_test_client_registry(client_one_registry, 1);

    ashiato::Registry client_two_registry;
    const ashiato::Entity client_two_position =
        ashiato::sync::register_sync_component<NetworkedPosition>(client_two_registry, "NetworkedPosition");
    const ashiato::Entity client_two_health = ashiato::sync::register_sync_component<Health>(client_two_registry, "Health");
    const ashiato::Entity client_two_public_state =
        ashiato::sync::register_sync_component<BandwidthProbe>(client_two_registry, "PublicState");
    const ashiato::sync::SyncArchetypeId client_two_archetype = ashiato::sync::define_archetype(
        client_two_registry,
        "OwnedActor",
        {
            {client_two_position, ashiato::sync::ReplicationAudience::All},
            {client_two_health, ashiato::sync::ReplicationAudience::Owner},
            {client_two_public_state, ashiato::sync::ReplicationAudience::EveryoneExceptOwner},
        });
    REQUIRE(client_two_archetype == server_archetype);
    ashiato_sync_tests::configure_test_client_registry(client_two_registry, 2);

    ashiato::sync::ReplicationClient client_one(client_one_registry, ashiato_sync_tests::make_test_client_options(client_one_registry, {}));
    ashiato::sync::ReplicationClient client_two(client_two_registry, ashiato_sync_tests::make_test_client_options(client_two_registry, {}));

    server.tick(server_registry, server.options().fixed_dt_seconds);
    REQUIRE(packets.size() == 2);
    for (const auto& sent : packets) {
        if (sent.first == 1) {
            REQUIRE(client_one.receive(client_one_registry, sent.second));
        } else {
            REQUIRE(client_two.receive(client_two_registry, sent.second));
        }
    }
    for (const ashiato::BitBuffer& ack : client_one.drain_ack_packets()) {
        REQUIRE(server.process_packet(server_registry, 1, ack));
    }
    for (const ashiato::BitBuffer& ack : client_two.drain_ack_packets()) {
        REQUIRE(server.process_packet(server_registry, 2, ack));
    }

    const ashiato::Entity client_one_local = client_one.local_entity(first_allocated_client_entity_network_id(1));
    const ashiato::Entity client_two_local = client_two.local_entity(first_allocated_client_entity_network_id(2));
    REQUIRE(client_one_registry.contains<Health>(client_one_local));
    REQUIRE_FALSE(client_two_registry.contains<Health>(client_two_local));
    REQUIRE_FALSE(client_one_registry.contains<BandwidthProbe>(client_one_local));
    REQUIRE(client_two_registry.contains<BandwidthProbe>(client_two_local));
    REQUIRE(client_two_registry.get<BandwidthProbe>(client_two_local).value == 7);

    REQUIRE(ashiato::sync::set_owner(server_registry, server_entity, 2));
    packets.clear();
    server.tick(server_registry, server.options().fixed_dt_seconds);
    REQUIRE(packets.size() == 2);
    for (const auto& sent : packets) {
        if (sent.first == 1) {
            REQUIRE(client_one.receive(client_one_registry, sent.second));
        } else {
            REQUIRE(client_two.receive(client_two_registry, sent.second));
        }
    }

    REQUIRE_FALSE(client_one_registry.contains<Health>(client_one_local));
    REQUIRE(client_two_registry.contains<Health>(client_two_local));
    REQUIRE(client_two_registry.get<Health>(client_two_local).value == 42);
    REQUIRE(client_one_registry.contains<BandwidthProbe>(client_one_local));
    REQUIRE(client_one_registry.get<BandwidthProbe>(client_one_local).value == 7);
    REQUIRE_FALSE(client_two_registry.contains<BandwidthProbe>(client_two_local));
}

TEST_CASE("replication client receives current component values when a replication decision mask reopens") {
    ashiato::Registry server_registry;
    const ashiato::Entity server_position =
        ashiato::sync::register_sync_component<NetworkedPosition>(server_registry, "NetworkedPosition");
    const ashiato::Entity server_health =
        ashiato::sync::register_sync_component<Health>(server_registry, "Health");
    const ashiato::sync::SyncArchetypeId server_archetype = ashiato::sync::define_archetype(
        server_registry,
        "MaskedActor",
        {
            {server_position, ashiato::sync::ReplicationAudience::All},
            {server_health, ashiato::sync::ReplicationAudience::All},
        });
    const ashiato::Entity server_entity = server_registry.create();
    REQUIRE(server_registry.add<NetworkedPosition>(server_entity, NetworkedPosition{1.0f, 2.0f}) != nullptr);
    REQUIRE(server_registry.add<Health>(server_entity, Health{25}) != nullptr);
    REQUIRE(start_sync(server_registry, server_entity, server_archetype));

    std::uint64_t component_mask = std::numeric_limits<std::uint64_t>::max();
    std::vector<ashiato::BitBuffer> packets;
    ashiato::sync::ReplicationServerOptions server_options;
    server_options.entity_replication_decision_interval_frames = 1;
    server_options.entity_replication_decider = [&](
        ashiato::sync::ClientId,
        ashiato::sync::EntityReplicationDecisionContext) {
        ashiato::sync::EntityReplicationDecision decision;
        decision.component_mask = component_mask;
        return decision;
    };
    server_options.transport = [&](ashiato::sync::ClientId client, const ashiato::BitBuffer& packet) {
        REQUIRE(client == 1);
        packets.push_back(packet);
    };
    ashiato::sync::ReplicationServer server(server_registry, server_options);
    REQUIRE(server.add_client(1));

    ashiato::Registry client_registry;
    const ashiato::Entity client_position =
        ashiato::sync::register_sync_component<NetworkedPosition>(client_registry, "NetworkedPosition");
    const ashiato::Entity client_health =
        ashiato::sync::register_sync_component<Health>(client_registry, "Health");
    const ashiato::sync::SyncArchetypeId client_archetype = ashiato::sync::define_archetype(
        client_registry,
        "MaskedActor",
        {
            {client_position, ashiato::sync::ReplicationAudience::All},
            {client_health, ashiato::sync::ReplicationAudience::All},
        });
    REQUIRE(client_archetype == server_archetype);
    configure_test_client_registry(client_registry, 1);
    ashiato::sync::ReplicationClient client(client_registry, make_test_client_options(client_registry, {}));
    const auto receive_and_ack_update = [&]() {
        REQUIRE(packets.size() == 1);
        REQUIRE(client.receive(client_registry, packets.back()));
        for (const ashiato::BitBuffer& ack : client.drain_ack_packets()) {
            REQUIRE(server.process_packet(server_registry, 1, ack));
        }
    };

    server.tick(server_registry, server.options().fixed_dt_seconds);
    receive_and_ack_update();

    const ashiato::Entity local = client.local_entity(first_allocated_client_entity_network_id(1));
    REQUIRE(client_registry.get<Health>(local).value == 25);

    component_mask = std::uint64_t{1} << 0U;
    server_registry.write<NetworkedPosition>(server_entity) = NetworkedPosition{4.0f, 6.0f};
    server_registry.write<Health>(server_entity) = Health{75};
    packets.clear();
    server.tick(server_registry, server.options().fixed_dt_seconds);
    receive_and_ack_update();
    REQUIRE(client_registry.get<Health>(local).value == 25);

    component_mask = std::numeric_limits<std::uint64_t>::max();
    server_registry.write<NetworkedPosition>(server_entity) = NetworkedPosition{7.0f, 8.0f};
    packets.clear();
    server.tick(server_registry, server.options().fixed_dt_seconds);
    receive_and_ack_update();
    REQUIRE(client_registry.get<Health>(local).value == 75);
}

TEST_CASE("replication client applies synced tags and owner-filtered tag visibility") {
    ashiato::Registry server_registry;
    const ashiato::Entity server_visible = server_registry.register_component<Visible>("Visible");
    const ashiato::Entity server_secret = server_registry.register_component<Secret>("Secret");
    const ashiato::Entity server_position =
        ashiato::sync::register_sync_component<NetworkedPosition>(server_registry, "NetworkedPosition");
    const ashiato::sync::SyncArchetypeId server_archetype = ashiato::sync::define_archetype(
        server_registry,
        ashiato::sync::SyncArchetypeDesc{
            "TaggedActor",
            {{server_visible, ashiato::sync::ReplicationAudience::All},
             {server_secret, ashiato::sync::ReplicationAudience::Owner}},
            {{server_position, ashiato::sync::ReplicationAudience::All}},
        });
    const ashiato::Entity server_entity = server_registry.create();
    REQUIRE(server_registry.add<NetworkedPosition>(server_entity, NetworkedPosition{1.0f, 2.0f}) != nullptr);
    REQUIRE(server_registry.add_tag(server_entity, server_visible));
    REQUIRE(server_registry.add_tag(server_entity, server_secret));
    REQUIRE(ashiato::sync::set_owner(server_registry, server_entity, 1));

    std::vector<std::pair<ashiato::sync::ClientId, ashiato::BitBuffer>> packets;
    ashiato::sync::ReplicationServerOptions server_options;
    server_options.transport = [&](ashiato::sync::ClientId client, const ashiato::BitBuffer& packet) {
        packets.push_back({client, packet});
    };
    ashiato::sync::ReplicationServer server(server_registry, server_options);
    REQUIRE(server.add_client(1));
    REQUIRE(server.add_client(2));
    REQUIRE(start_sync(server_registry, server_entity, server_archetype));

    ashiato::Registry owner_registry;
    const ashiato::Entity owner_visible = owner_registry.register_component<Visible>("Visible");
    const ashiato::Entity owner_secret = owner_registry.register_component<Secret>("Secret");
    const ashiato::Entity owner_position =
        ashiato::sync::register_sync_component<NetworkedPosition>(owner_registry, "NetworkedPosition");
    REQUIRE(ashiato::sync::define_archetype(
                owner_registry,
                ashiato::sync::SyncArchetypeDesc{
                    "TaggedActor",
                    {{owner_visible, ashiato::sync::ReplicationAudience::All},
                     {owner_secret, ashiato::sync::ReplicationAudience::Owner}},
                    {{owner_position, ashiato::sync::ReplicationAudience::All}},
                }) == server_archetype);
    ashiato_sync_tests::configure_test_client_registry(owner_registry, 1);

    ashiato::Registry non_owner_registry;
    const ashiato::Entity non_owner_visible = non_owner_registry.register_component<Visible>("Visible");
    const ashiato::Entity non_owner_secret = non_owner_registry.register_component<Secret>("Secret");
    const ashiato::Entity non_owner_position =
        ashiato::sync::register_sync_component<NetworkedPosition>(non_owner_registry, "NetworkedPosition");
    REQUIRE(ashiato::sync::define_archetype(
                non_owner_registry,
                ashiato::sync::SyncArchetypeDesc{
                    "TaggedActor",
                    {{non_owner_visible, ashiato::sync::ReplicationAudience::All},
                     {non_owner_secret, ashiato::sync::ReplicationAudience::Owner}},
                    {{non_owner_position, ashiato::sync::ReplicationAudience::All}},
                }) == server_archetype);
    ashiato_sync_tests::configure_test_client_registry(non_owner_registry, 2);

    ashiato::sync::ReplicationClient owner_client(owner_registry, ashiato_sync_tests::make_test_client_options(owner_registry, {}));
    ashiato::sync::ReplicationClient non_owner_client(non_owner_registry, ashiato_sync_tests::make_test_client_options(non_owner_registry, {}));
    server.tick(server_registry, server.options().fixed_dt_seconds);
    REQUIRE(owner_client.receive(owner_registry, packet_for(packets, 1)));
    REQUIRE(non_owner_client.receive(non_owner_registry, packet_for(packets, 2)));

    const ashiato::Entity owner_local = owner_client.local_entity(first_allocated_client_entity_network_id(1));
    const ashiato::Entity non_owner_local = non_owner_client.local_entity(first_allocated_client_entity_network_id(2));
    REQUIRE(owner_registry.has(owner_local, owner_visible));
    REQUIRE(owner_registry.has(owner_local, owner_secret));
    REQUIRE(non_owner_registry.has(non_owner_local, non_owner_visible));
    REQUIRE_FALSE(non_owner_registry.has(non_owner_local, non_owner_secret));

    for (const ashiato::BitBuffer& ack : owner_client.drain_ack_packets()) {
        REQUIRE(server.process_packet(server_registry, 1, ack));
    }
    for (const ashiato::BitBuffer& ack : non_owner_client.drain_ack_packets()) {
        REQUIRE(server.process_packet(server_registry, 2, ack));
    }

    REQUIRE(ashiato::sync::set_owner(server_registry, server_entity, 2));
    packets.clear();
    server.tick(server_registry, server.options().fixed_dt_seconds);
    REQUIRE(owner_client.receive(owner_registry, packet_for(packets, 1)));
    REQUIRE(non_owner_client.receive(non_owner_registry, packet_for(packets, 2)));
    REQUIRE(owner_registry.has(owner_local, owner_visible));
    REQUIRE_FALSE(owner_registry.has(owner_local, owner_secret));
    REQUIRE(non_owner_registry.has(non_owner_local, non_owner_visible));
    REQUIRE(non_owner_registry.has(non_owner_local, non_owner_secret));
}

TEST_CASE("buffered interpolation rejects stale duplicate packets without ACKing") {
    ashiato::Registry server_registry;
    const ashiato::sync::SyncArchetypeId server_archetype = ashiato_sync_tests::define_position_archetype(server_registry);
    const ashiato::Entity server_entity = server_registry.create();
    REQUIRE(server_registry.add<Position>(server_entity, Position{1.0f, 2.0f}) != nullptr);

    std::vector<ashiato::BitBuffer> packets;
    ashiato::sync::ReplicationServerOptions server_options;
    server_options.transport = [&](ashiato::sync::ClientId, const ashiato::BitBuffer& packet) {
        packets.push_back(packet);
    };
    ashiato::sync::ReplicationServer server(server_registry, server_options);
    REQUIRE(server.add_client(1));
    REQUIRE(start_sync(server_registry, server_entity, server_archetype));

    ashiato::Registry client_registry;
    const ashiato::sync::SyncArchetypeId client_archetype = ashiato_sync_tests::define_position_archetype(client_registry);
    REQUIRE(client_archetype == server_archetype);
    ashiato_sync_tests::configure_test_client_registry(client_registry, 1);

    ashiato::sync::ReplicationClient client(client_registry, ashiato_sync_tests::make_test_client_options(client_registry, ashiato::sync::ReplicationClientOptions{
        ashiato::sync::ReplicationClientNetworkOptions{1200},
        ashiato::sync::ReplicationClientEntityOptions{ashiato::sync::ReplicationClientMode::BufferedInterpolation},
        ashiato::sync::ReplicationClientBufferedOptions{1}}));

    server.tick(server_registry, server.options().fixed_dt_seconds);
    const ashiato::BitBuffer full_packet = packets.back();
    REQUIRE(client.receive(client_registry, full_packet));
    REQUIRE(client.drain_ack_packets().size() == 1);
    REQUIRE_FALSE(client.receive(client_registry, full_packet));
    REQUIRE(client.pending_ack_count() == 0);

    server_registry.write<Position>(server_entity) = Position{3.0f, 4.0f};
    packets.clear();
    server.tick(server_registry, server.options().fixed_dt_seconds);
    const ashiato::BitBuffer stale_full_packet = full_packet;
    REQUIRE(client.receive(client_registry, packets.back()));
    REQUIRE(client.drain_ack_packets().size() == 1);
    REQUIRE_FALSE(client.receive(client_registry, stale_full_packet));
    REQUIRE(client.pending_ack_count() == 0);

    REQUIRE(server_registry.destroy(server_entity));
    packets.clear();
    server.tick(server_registry, server.options().fixed_dt_seconds);
    const ashiato::BitBuffer destroy_packet = packets.back();
    REQUIRE(client.receive(client_registry, destroy_packet));
    REQUIRE(client.drain_ack_packets().size() == 1);
    REQUIRE_FALSE(client.receive(client_registry, destroy_packet));
    REQUIRE(client.pending_ack_count() == 0);
}

TEST_CASE("replication client rejects stale full after destroy and accepts reused network id") {
    ashiato::Registry server_registry;
    const ashiato::sync::SyncArchetypeId server_archetype = ashiato_sync_tests::define_position_archetype(server_registry);

    std::vector<ashiato::BitBuffer> packets;
    ashiato::sync::ReplicationServerOptions server_options;
    server_options.transport = [&](ashiato::sync::ClientId, const ashiato::BitBuffer& packet) {
        packets.push_back(packet);
    };
    ashiato::sync::ReplicationServer server(server_registry, server_options);
    REQUIRE(server.add_client(1));

    ashiato::Registry client_registry;
    const ashiato::sync::SyncArchetypeId client_archetype = ashiato_sync_tests::define_position_archetype(client_registry);
    REQUIRE(client_archetype == server_archetype);
    ashiato_sync_tests::configure_test_client_registry(client_registry, 1);
    ashiato::sync::ReplicationClient client(client_registry, ashiato_sync_tests::make_test_client_options(client_registry, {}));

    auto process_client_acks = [&]() {
        for (const ashiato::BitBuffer& ack : client.drain_ack_packets()) {
            REQUIRE(server.process_packet(server_registry, 1, ack));
        }
    };

    const ashiato::Entity first = server_registry.create();
    REQUIRE(server_registry.add<Position>(first, Position{1.0f, 2.0f}) != nullptr);
    REQUIRE(start_sync(server_registry, first, server_archetype));
    server.tick(server_registry, server.options().fixed_dt_seconds);
    const ashiato::BitBuffer first_full = packets.back();
    const UpdatePacket first_update = read_update(first_full);
    REQUIRE(first_update.records.size() == 1);
    const std::uint32_t reused_wire_network_id = first_update.records[0].network_id;
    REQUIRE(client.receive(client_registry, first_full));
    const ashiato::Entity first_local =
        client.local_entity(test_client_entity_network_id(1, reused_wire_network_id, 1U));
    REQUIRE(first_local);
    process_client_acks();

    packets.clear();
    REQUIRE(server_registry.destroy(first));
    server.tick(server_registry, server.options().fixed_dt_seconds);
    const ashiato::BitBuffer destroy = packets.back();
    REQUIRE(client.receive(client_registry, destroy));
    REQUIRE(client.client_entity_network_id(first_local) == ashiato::sync::invalid_client_entity_network_id);
    process_client_acks();
    REQUIRE_FALSE(client.receive(client_registry, first_full));
    REQUIRE(client.pending_ack_count() == 0);

    packets.clear();
    const ashiato::Entity second = server_registry.create();
    REQUIRE(server_registry.add<Position>(second, Position{3.0f, 4.0f}) != nullptr);
    REQUIRE(start_sync(server_registry, second, server_archetype));
    server.tick(server_registry, server.options().fixed_dt_seconds);
    const UpdatePacket second_update = read_update(packets.back());
    REQUIRE(second_update.records.size() == 1);
    REQUIRE(second_update.records[0].network_id == reused_wire_network_id);
    REQUIRE(client.receive(client_registry, packets.back()));
    const ashiato::sync::ClientEntityNetworkId second_id =
        test_client_entity_network_id(1, reused_wire_network_id, 2U);
    const ashiato::Entity second_local = client.local_entity(second_id);
    REQUIRE(second_local);
    REQUIRE(client.client_entity_network_id(second_local) == second_id);
}

TEST_CASE("replication client rejects stale client entity network ids after wire id reuse") {
    ashiato::Registry client_registry;
    const ashiato::sync::SyncArchetypeId client_archetype = ashiato_sync_tests::define_position_archetype(client_registry);
    REQUIRE(client_archetype.value == 0);
    ashiato_sync_tests::configure_test_client_registry(client_registry, 1);

    const ashiato::Entity server_entity{42};
    const std::uint32_t wire_id = test_network_id(server_entity);
    const ashiato::sync::ClientEntityNetworkId first_id = test_client_entity_network_id(1, wire_id, 1U);
    const ashiato::sync::ClientEntityNetworkId second_id = test_client_entity_network_id(1, wire_id, 2U);
    ashiato::sync::ReplicationClient client(client_registry, ashiato_sync_tests::make_test_client_options(client_registry, {}));

    REQUIRE(client.receive(client_registry, make_position_packet(1, {{server_entity, Position{5.0f, 6.0f}}})));
    const ashiato::Entity first_local = client.local_entity(first_id);
    REQUIRE(first_local);
    REQUIRE(client_registry.alive(first_local));
    REQUIRE(client.is_alive_client_entity_network_id(first_id));
    REQUIRE(client.drain_ack_packets().size() == 1);

    REQUIRE(client.receive(client_registry, make_destroy_packet(2, server_entity)));
    REQUIRE_FALSE(client_registry.alive(first_local));
    REQUIRE_FALSE(client.is_alive_client_entity_network_id(first_id));
    REQUIRE(client.drain_ack_packets().size() == 1);

    REQUIRE(client.receive(client_registry, make_position_packet(3, {{server_entity, Position{1.0f, 2.0f}}})));
    const ashiato::Entity second_local = client.local_entity(second_id);
    REQUIRE(second_local);
    REQUIRE(second_local != first_local);
    REQUIRE(client.is_alive_client_entity_network_id(second_id));
    REQUIRE_FALSE(client.local_entity(first_id));
}

TEST_CASE("replication client does not advance implicit versions twice for destroy resends") {
    ashiato::Registry client_registry;
    const ashiato::sync::SyncArchetypeId client_archetype = ashiato_sync_tests::define_position_archetype(client_registry);
    REQUIRE(client_archetype.value == 0);
    ashiato_sync_tests::configure_test_client_registry(client_registry, 1);

    const ashiato::Entity server_entity{42};
    const std::uint32_t wire_id = test_network_id(server_entity);
    const ashiato::sync::ClientEntityNetworkId second_id = test_client_entity_network_id(1, wire_id, 2U);
    const ashiato::sync::ClientEntityNetworkId third_id = test_client_entity_network_id(1, wire_id, 3U);
    ashiato::sync::ReplicationClient client(client_registry, ashiato_sync_tests::make_test_client_options(client_registry, {}));

    REQUIRE(client.receive(client_registry, make_position_packet(1, {{server_entity, Position{1.0f, 2.0f}}})));
    REQUIRE(client.drain_ack_packets().size() == 1);

    REQUIRE(client.receive(client_registry, make_destroy_packet(2, server_entity)));
    REQUIRE(client.drain_ack_packets().size() == 1);
    REQUIRE(client.receive(client_registry, make_destroy_packet(3, server_entity)));
    REQUIRE(client.drain_ack_packets().size() == 1);

    REQUIRE(client.receive(client_registry, make_position_packet(4, {{server_entity, Position{5.0f, 6.0f}}})));
    const ashiato::Entity local = client.local_entity(second_id);
    REQUIRE(local);
    REQUIRE(client_registry.alive(local));
    REQUIRE(client_registry.get<Position>(local).x == 5.0f);
    REQUIRE_FALSE(client.local_entity(third_id));

    const std::vector<ashiato::BitBuffer> acks = client.drain_ack_packets();
    REQUIRE(acks.size() == 1);
    const std::vector<AckRecord> records = read_acks(acks[0]);
    REQUIRE(records.size() == 1);
    REQUIRE(records[0].packet_id == 4);
}

TEST_CASE("replication client evicts destroy tombstones by deterministic age") {
    ashiato::Registry client_registry;
    const ashiato::sync::SyncArchetypeId client_archetype = ashiato_sync_tests::define_position_archetype(client_registry);
    REQUIRE(client_archetype.value == 0);
    ashiato_sync_tests::configure_test_client_registry(client_registry, 1);

    ashiato::sync::ReplicationClient client(client_registry, ashiato_sync_tests::make_test_client_options(client_registry, {}));
    constexpr std::uint32_t first_wire_id = 2U;
    constexpr std::uint16_t first_packet_destroy_count =
        static_cast<std::uint16_t>(client_destroy_tombstone_capacity - 1U);
    REQUIRE(client.receive(
        client_registry,
        make_destroy_packet_for_wire_ids(1, 1, first_wire_id, first_packet_destroy_count)));
    REQUIRE(client.receive(
        client_registry,
        make_destroy_packet_for_wire_ids(2, 2, first_wire_id + first_packet_destroy_count, 2U)));
    REQUIRE_FALSE(client.drain_ack_packets().empty());

    const ashiato::Entity evicted_server_entity{first_wire_id - 1U};
    const ashiato::sync::ClientEntityNetworkId evicted_id =
        test_client_entity_network_id(1, first_wire_id, 1U);
    REQUIRE(client.receive(
        client_registry,
        make_position_packet(1, {{evicted_server_entity, Position{1.0f, 2.0f}}}, 2U, 3U)));
    const ashiato::Entity evicted_local = client.local_entity(evicted_id);
    REQUIRE(evicted_local);
    REQUIRE(client_registry.alive(evicted_local));

    constexpr std::uint32_t retained_wire_id = first_wire_id + 1U;
    const ashiato::Entity retained_server_entity{retained_wire_id - 1U};
    REQUIRE_FALSE(client.receive(
        client_registry,
        make_position_packet(1, {{retained_server_entity, Position{3.0f, 4.0f}}}, 2U, 4U)));
    REQUIRE_FALSE(client.local_entity(test_client_entity_network_id(1, retained_wire_id, 1U)));
}

TEST_CASE("replication client keeps receiving an entity through processing and ACK stalls") {
    // A client that stops processing for a while -- its main thread blocked, say -- sends no ACKs, so the server
    // keeps delta-encoding a changing entity against the last baseline the client ACKed. When the client catches
    // up it applies the queued packets in one burst. Its per-entity baseline history is a ring indexed by frame
    // (client/state.hpp, max_baseline_history_per_entity), so once it applies a record 64 or more frames newer than
    // that baseline, the baseline's slot is overwritten and every later delta against it fails.
    const int stalled_ticks = GENERATE(40, 150, 512);
    const bool process_during_stall = GENERATE(false, true);
    INFO("stalled for " << stalled_ticks << " ticks; process packets: " << process_during_stall);
    {
        {
            ashiato::Registry server_registry;
            const ashiato::Entity position_component =
                ashiato::sync::register_sync_component<NetworkedPosition>(server_registry, "NetworkedPosition");
            const ashiato::sync::SyncArchetypeId server_archetype = ashiato::sync::define_archetype(
                server_registry,
                "NetworkedActor",
                {{position_component, ashiato::sync::ReplicationAudience::All}});
            const ashiato::Entity server_entity = server_registry.create();
            REQUIRE(server_registry.add<NetworkedPosition>(server_entity, NetworkedPosition{1.0f, 2.0f}) != nullptr);

            std::vector<ashiato::BitBuffer> packets;
            ashiato::sync::ReplicationServerOptions server_options;
            server_options.transport = [&](ashiato::sync::ClientId, const ashiato::BitBuffer& packet) {
                packets.push_back(packet);
            };
            ashiato::sync::ReplicationServer server(server_registry, server_options);
            REQUIRE(server.add_client(1));
            REQUIRE(start_sync(server_registry, server_entity, server_archetype));

            ashiato::Registry client_registry;
            const ashiato::Entity client_position =
                ashiato::sync::register_sync_component<NetworkedPosition>(client_registry, "NetworkedPosition");
            const ashiato::sync::SyncArchetypeId client_archetype = ashiato::sync::define_archetype(
                client_registry,
                "NetworkedActor",
                {{client_position, ashiato::sync::ReplicationAudience::All}});
            REQUIRE(client_archetype == server_archetype);
            ashiato_sync_tests::configure_test_client_registry(client_registry, 1);
            ashiato::sync::ReplicationClient client(
                client_registry,
                ashiato_sync_tests::make_test_client_options(client_registry, {}));

            auto deliver_acks = [&]() {
                // Not REQUIREd: after a long stall the server rightly ignores ACKs for sends it has stopped tracking.
                for (const ashiato::BitBuffer& ack : client.drain_ack_packets()) {
                    (void)server.process_packet(server_registry, 1, ack);
                }
            };

            // A shared baseline, ACKed.
            server.tick(server_registry, server.options().fixed_dt_seconds);
            REQUIRE(client.receive(client_registry, packets.back()));
            deliver_acks();
            packets.clear();

            // In a processing stall, packets queue without reaching the client. In an ACK stall, the client
            // applies every packet but its outgoing ACKs are lost. Keep the values within the test codec's
            // 8-bit fields even when the outage spans several baseline-history rotations.
            float x = 1.0f;
            for (int tick = 0; tick < stalled_ticks; ++tick) {
                x += 0.01f;
                server_registry.write<NetworkedPosition>(server_entity) = NetworkedPosition{x, 2.0f};
                server.tick(server_registry, server.options().fixed_dt_seconds);
                // A tick whose quantized value did not move (0.01 against the codec's 0.1) sends no record,
                // and so no packet: nothing the client lacks.
                if (process_during_stall && !packets.empty()) {
                    REQUIRE(packets.size() == 1);
                    REQUIRE(client.receive(client_registry, packets.back()));
                    (void)client.drain_ack_packets();
                    packets.clear();
                }
            }

            // A processing stall catches up in one burst. An ACK stall has already applied the same stream.
            for (const ashiato::BitBuffer& packet : packets) {
                REQUIRE(client.receive(client_registry, packet));
            }
            deliver_acks();
            packets.clear();

            // Normal service again, with no loss at all. A whole codec step a tick, so every tick is a real
            // change and carries a record.
            std::size_t applied = 0;
            for (int tick = 0; tick < 20; ++tick) {
                x += 0.1f;
                server_registry.write<NetworkedPosition>(server_entity) = NetworkedPosition{x, 2.0f};
                server.tick(server_registry, server.options().fixed_dt_seconds);
                if (client.receive(client_registry, packets.back())) {
                    ++applied;
                }
                deliver_acks();
                packets.clear();
            }

            const ashiato::Entity local = client.local_entity(first_allocated_client_entity_network_id(1));
            REQUIRE(local);
            CHECK(applied == 20U);
            REQUIRE(client_registry.get<NetworkedPosition>(local).x == Catch::Approx(x).margin(0.11));
        }
    }
}
