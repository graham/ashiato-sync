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

TEST_CASE("replication server sends pending destroys before bandwidth-limited updates") {
    ashiato::Registry registry;
    const ashiato::Entity position_component =
        ashiato::sync::register_sync_component<NetworkedPosition>(registry, "NetworkedPosition");
    const ashiato::sync::SyncArchetypeId archetype = ashiato::sync::define_archetype(
        registry,
        "NetworkedActor",
        {{position_component, ashiato::sync::ReplicationAudience::All}});
    const ashiato::Entity destroyed = registry.create();
    const ashiato::Entity live = registry.create();
    REQUIRE(registry.add<NetworkedPosition>(destroyed, NetworkedPosition{1.0f, 1.0f}) != nullptr);
    REQUIRE(registry.add<NetworkedPosition>(live, NetworkedPosition{2.0f, 2.0f}) != nullptr);

    std::vector<ashiato::BitBuffer> payloads;
    ashiato::sync::ReplicationServerOptions options;
    options.bandwidth_limit_bytes_per_tick = 21;
    options.mtu_bytes = 21;
    options.transport = [&](ashiato::sync::ClientId, const ashiato::BitBuffer& payload) {
        payloads.push_back(payload);
    };

    ashiato::sync::ReplicationServer server(registry, options);
    REQUIRE(server.add_client(1));
    REQUIRE(start_sync(registry, destroyed, archetype));
    REQUIRE(start_sync(registry, live, archetype));
    server.tick(registry, server.options().fixed_dt_seconds);
    REQUIRE(payloads.size() == 1);
    payloads.clear();

    REQUIRE(registry.destroy(destroyed));
    server.tick(registry, server.options().fixed_dt_seconds);
    REQUIRE(payloads.size() == 1);
    ServerUpdatePacket update = read_server_update(payloads.back());
    REQUIRE(update.entities.size() == 1);
    REQUIRE(update.entities[0].destroy);
    REQUIRE(update.entities[0].network_id != 0);
    REQUIRE(server.process_packet(1, write_ack_packet(update.packet_id)));
    payloads.clear();

    server.tick(registry, server.options().fixed_dt_seconds);
    REQUIRE(payloads.size() == 1);
    update = read_server_update(payloads.back());
    REQUIRE(update.entities.size() == 1);
    REQUIRE_FALSE(update.entities[0].destroy);
}

TEST_CASE("replication server resends pending destroys until ACKed") {
    ashiato::Registry registry;
    const ashiato::sync::SyncArchetypeId archetype = define_position_archetype(registry);
    const ashiato::Entity entity = registry.create();
    REQUIRE(registry.add<ashiato_sync_tests::Position>(entity, ashiato_sync_tests::Position{1.0f, 2.0f}) != nullptr);

    std::vector<ashiato::BitBuffer> payloads;
    ashiato::sync::ReplicationServerOptions options;
    options.bandwidth_limit_bytes_per_tick = 30;
    options.mtu_bytes = 30;
    options.transport = [&](ashiato::sync::ClientId, const ashiato::BitBuffer& payload) {
        payloads.push_back(payload);
    };

    ashiato::sync::ReplicationServer server(registry, options);
    REQUIRE(server.add_client(1));
    REQUIRE(start_sync(registry, entity, archetype));
    server.tick(registry, server.options().fixed_dt_seconds);
    payloads.clear();

    REQUIRE(registry.destroy(entity));
    server.tick(registry, server.options().fixed_dt_seconds);
    REQUIRE(payloads.size() == 1);
    ServerUpdatePacket first_destroy = read_server_update(payloads.back());
    REQUIRE(first_destroy.entities.size() == 1);
    REQUIRE(first_destroy.entities[0].destroy);
    payloads.clear();

    server.tick(registry, server.options().fixed_dt_seconds);
    REQUIRE(payloads.size() == 1);
    ServerUpdatePacket resent_destroy = read_server_update(payloads.back());
    REQUIRE(resent_destroy.entities.size() == 1);
    REQUIRE(resent_destroy.entities[0].destroy);
    REQUIRE(resent_destroy.entities[0].network_id == first_destroy.entities[0].network_id);
    REQUIRE(resent_destroy.frame > first_destroy.frame);

    REQUIRE(server.process_packet(registry, 1, write_ack_packet(resent_destroy.packet_id)));
    payloads.clear();
    server.tick(registry, server.options().fixed_dt_seconds);
    REQUIRE(payloads.empty());
}

TEST_CASE("replication server does not reuse client-local network ids before destroy ACK") {
    ashiato::Registry registry;
    const ashiato::sync::SyncArchetypeId archetype = define_position_archetype(registry);

    std::vector<ashiato::BitBuffer> payloads;
    ashiato::sync::ReplicationServerOptions options;
    options.transport = [&](ashiato::sync::ClientId, const ashiato::BitBuffer& payload) {
        payloads.push_back(payload);
    };

    ashiato::sync::ReplicationServer server(registry, options);
    REQUIRE(server.add_client(1));

    auto spawn = [&](float x) {
        const ashiato::Entity entity = registry.create();
        REQUIRE(registry.add<ashiato_sync_tests::Position>(entity, ashiato_sync_tests::Position{x, x}) != nullptr);
        REQUIRE(start_sync(registry, entity, archetype));
        return entity;
    };

    const ashiato::Entity first = spawn(1.0f);
    server.tick(registry, server.options().fixed_dt_seconds);
    REQUIRE(payloads.size() == 1);
    const ServerUpdatePacket first_update =
        read_server_update(payloads.back(), 2U, sizeof(ashiato_sync_tests::Position) * 8U);
    REQUIRE(first_update.entities.size() == 1);
    const std::uint32_t first_network_id = first_update.entities[0].network_id;
    REQUIRE(first_network_id != 0);

    payloads.clear();
    REQUIRE(registry.destroy(first));
    server.tick(registry, server.options().fixed_dt_seconds);
    REQUIRE(payloads.size() == 1);
    const ServerUpdatePacket destroy_update =
        read_server_update(payloads.back(), 2U, sizeof(ashiato_sync_tests::Position) * 8U);
    REQUIRE(destroy_update.entities.size() == 1);
    REQUIRE(destroy_update.entities[0].destroy);

    payloads.clear();
    spawn(2.0f);
    server.tick(registry, server.options().fixed_dt_seconds);
    REQUIRE(payloads.size() == 1);
    const ServerUpdatePacket second_update =
        read_server_update(payloads.back(), 2U, sizeof(ashiato_sync_tests::Position) * 8U);
    REQUIRE(second_update.entities.size() == 2);
    const auto upsert = std::find_if(
        second_update.entities.begin(),
        second_update.entities.end(),
        [](const EntityRecord& record) {
            return !record.destroy;
        });
    REQUIRE(upsert != second_update.entities.end());
    REQUIRE(upsert->network_id != first_network_id);
}

TEST_CASE("replication server reuses client-local network ids after each client's destroy ACK") {
    ashiato::Registry registry;
    const ashiato::sync::SyncArchetypeId archetype = define_position_archetype(registry);

    std::vector<std::pair<ashiato::sync::ClientId, ashiato::BitBuffer>> payloads;
    ashiato::sync::ReplicationServerOptions options;
    options.transport = [&](ashiato::sync::ClientId client, const ashiato::BitBuffer& payload) {
        payloads.push_back({client, payload});
    };

    ashiato::sync::ReplicationServer server(registry, options);
    REQUIRE(server.add_client(1));
    REQUIRE(server.add_client(2));

    auto spawn = [&](float x) {
        const ashiato::Entity entity = registry.create();
        REQUIRE(registry.add<ashiato_sync_tests::Position>(entity, ashiato_sync_tests::Position{x, x}) != nullptr);
        REQUIRE(start_sync(registry, entity, archetype));
        return entity;
    };
    auto update_for = [&](ashiato::sync::ClientId client) {
        for (const auto& sent : payloads) {
            if (sent.first != client) {
                continue;
            }
            ServerUpdatePacket update = read_server_update(sent.second, 2U, sizeof(ashiato_sync_tests::Position) * 8U);
            const auto found = std::find_if(update.entities.begin(), update.entities.end(), [](const EntityRecord& record) {
                return !record.destroy;
            });
            if (found != update.entities.end()) {
                return update;
            }
        }
        return ServerUpdatePacket{};
    };
    auto destroy_for = [&](ashiato::sync::ClientId client, std::uint32_t network_id) {
        for (const auto& sent : payloads) {
            if (sent.first != client) {
                continue;
            }
            ServerUpdatePacket update = read_server_update(sent.second, 2U, sizeof(ashiato_sync_tests::Position) * 8U);
            const auto found = std::find_if(update.entities.begin(), update.entities.end(), [&](const EntityRecord& record) {
                return record.destroy && record.network_id == network_id;
            });
            if (found != update.entities.end()) {
                return update;
            }
        }
        return ServerUpdatePacket{};
    };
    auto packet_id = [](ashiato::BitBuffer packet) {
        packet.read_bits(ashiato::sync::protocol::message_bits);
        packet.read_bits(32U);
        return static_cast<std::uint32_t>(packet.read_bits(ashiato::sync::protocol::server_packet_id_bits));
    };

    const ashiato::Entity first = spawn(1.0f);
    server.tick(registry, server.options().fixed_dt_seconds);
    ServerUpdatePacket first_update = update_for(1);
    REQUIRE(first_update.entities.size() == 1);
    const std::uint32_t reusable_network_id = first_update.entities[0].network_id;
    REQUIRE(reusable_network_id != 0);

    payloads.clear();
    REQUIRE(registry.destroy(first));
    server.tick(registry, server.options().fixed_dt_seconds);
    ServerUpdatePacket client_one_destroy = destroy_for(1, reusable_network_id);
    ServerUpdatePacket client_two_destroy = destroy_for(2, reusable_network_id);
    REQUIRE(client_one_destroy.entities.size() == 1);
    REQUIRE(client_two_destroy.entities.size() == 1);
    REQUIRE(server.process_packet(registry, 1, write_ack_packet(client_one_destroy.packet_id)));

    payloads.clear();
    spawn(2.0f);
    server.tick(registry, server.options().fixed_dt_seconds);
    ServerUpdatePacket second_update = update_for(1);
    REQUIRE(second_update.entities.size() == 1);
    REQUIRE(second_update.entities[0].network_id == reusable_network_id);
    ServerUpdatePacket client_two_second_update = update_for(2);
    REQUIRE(client_two_second_update.entities.size() == 2);
    const auto client_two_second = std::find_if(
        client_two_second_update.entities.begin(),
        client_two_second_update.entities.end(),
        [](const EntityRecord& record) {
            return !record.destroy;
        });
    REQUIRE(client_two_second != client_two_second_update.entities.end());
    REQUIRE(client_two_second->network_id != reusable_network_id);

    for (const auto& sent : payloads) {
        REQUIRE(server.process_packet(registry, sent.first, write_ack_packet(packet_id(sent.second))));
    }
    payloads.clear();
    spawn(3.0f);
    server.tick(registry, server.options().fixed_dt_seconds);
    ServerUpdatePacket third_update = update_for(2);
    const auto third_record = std::find_if(
        third_update.entities.begin(),
        third_update.entities.end(),
        [](const EntityRecord& record) {
            return !record.destroy;
        });
    REQUIRE(third_record != third_update.entities.end());
    const bool saw_reused_network_id = std::any_of(
        third_update.entities.begin(),
        third_update.entities.end(),
        [reusable_network_id](const EntityRecord& record) {
            return !record.destroy && record.network_id == reusable_network_id;
        });
    REQUIRE(saw_reused_network_id);
}

TEST_CASE("replication server reuses network ids immediately when no clients have pending destroys") {
    ashiato::Registry registry;
    const ashiato::sync::SyncArchetypeId archetype = define_position_archetype(registry);

    std::vector<ashiato::BitBuffer> payloads;
    ashiato::sync::ReplicationServerOptions options;
    options.transport = [&](ashiato::sync::ClientId, const ashiato::BitBuffer& payload) {
        payloads.push_back(payload);
    };

    ashiato::sync::ReplicationServer server(registry, options);
    const ashiato::Entity first = registry.create();
    REQUIRE(registry.add<ashiato_sync_tests::Position>(first, ashiato_sync_tests::Position{1.0f, 1.0f}) != nullptr);
    REQUIRE(start_sync(registry, first, archetype));
    server.tick(registry, server.options().fixed_dt_seconds);
    REQUIRE(registry.destroy(first));
    server.tick(registry, server.options().fixed_dt_seconds);

    const ashiato::Entity second = registry.create();
    REQUIRE(registry.add<ashiato_sync_tests::Position>(second, ashiato_sync_tests::Position{2.0f, 2.0f}) != nullptr);
    REQUIRE(start_sync(registry, second, archetype));
    REQUIRE(server.add_client(1));
    server.tick(registry, server.options().fixed_dt_seconds);

    REQUIRE(payloads.size() == 1);
    const ServerUpdatePacket update = read_server_update(payloads.back());
    REQUIRE(update.entities.size() == 1);
    REQUIRE(update.entities[0].network_id == 1);
}

TEST_CASE("replication server accepts delayed entity ACKs for retained quantized frames") {
    ashiato::Registry registry;
    const ashiato::Entity position_component =
        ashiato::sync::register_sync_component<NetworkedPosition>(registry, "NetworkedPosition");
    const ashiato::sync::SyncArchetypeId archetype = ashiato::sync::define_archetype(
        registry,
        "NetworkedActor",
        {{position_component, ashiato::sync::ReplicationAudience::All}});
    const ashiato::Entity entity = registry.create();
    REQUIRE(registry.add<NetworkedPosition>(entity, NetworkedPosition{1.0f, 2.0f}) != nullptr);

    std::vector<ashiato::BitBuffer> payloads;
    ashiato::sync::ReplicationServerOptions options;
    options.bandwidth_limit_bytes_per_tick = 1024;
    options.transport = [&](ashiato::sync::ClientId, const ashiato::BitBuffer& payload) {
        payloads.push_back(payload);
    };

    ashiato::sync::ReplicationServer server(registry, options);
    REQUIRE(server.add_client(1));
    REQUIRE(start_sync(registry, entity, archetype));

    server.tick(registry, server.options().fixed_dt_seconds);
    const ashiato::sync::SyncFrame first_frame = read_server_update(payloads.back()).frame;
    registry.write<NetworkedPosition>(entity) = NetworkedPosition{2.0f, 3.0f};
    server.tick(registry, server.options().fixed_dt_seconds);
    REQUIRE(read_first_networked_payload(payloads.back()).delta == false);
    REQUIRE(server.acknowledge_entity(1, entity, first_frame));
    REQUIRE_FALSE(server.acknowledge_entity(1, entity, first_frame));

    const ashiato::sync::SyncFrame second_frame = read_server_update(payloads.back()).frame;
    REQUIRE(server.acknowledge_entity(1, entity, second_frame));
    REQUIRE_FALSE(server.acknowledge_entity(1, entity, second_frame));

    registry.write<NetworkedPosition>(entity) = NetworkedPosition{3.0f, 4.0f};
    server.tick(registry, server.options().fixed_dt_seconds);
    REQUIRE(read_first_networked_payload(payloads.back()).delta);
}

TEST_CASE("replication server shares ACKed quantized frames across clients and frees them") {
    ashiato::Registry registry;
    const ashiato::Entity position_component =
        ashiato::sync::register_sync_component<NetworkedPosition>(registry, "NetworkedPosition");
    const ashiato::sync::SyncArchetypeId archetype = ashiato::sync::define_archetype(
        registry,
        "NetworkedActor",
        {{position_component, ashiato::sync::ReplicationAudience::All}});
    const ashiato::Entity entity = registry.create();
    REQUIRE(registry.add<NetworkedPosition>(entity, NetworkedPosition{1.0f, 2.0f}) != nullptr);

    std::vector<std::pair<ashiato::sync::ClientId, ashiato::BitBuffer>> payloads;
    ashiato::sync::ReplicationServerOptions options;
    options.bandwidth_limit_bytes_per_tick = 1024;
    options.transport = [&](ashiato::sync::ClientId client, const ashiato::BitBuffer& payload) {
        payloads.push_back({client, payload});
    };

    ashiato::sync::ReplicationServer server(registry, options);
    REQUIRE(server.add_client(1));
    REQUIRE(server.add_client(2));
    REQUIRE(start_sync(registry, entity, archetype));

    server.tick(registry, server.options().fixed_dt_seconds);
    REQUIRE(payloads.size() == 2);
    REQUIRE(server.retained_quantized_frame_count() == 1);
    REQUIRE(server.retained_quantized_frame_bytes() == sizeof(ashiato_sync_tests::QuantizedNetworkedPosition));

    REQUIRE(server.acknowledge_entity(1, entity, read_server_update(payloads[0].second).frame));
    REQUIRE(server.acknowledge_entity(2, entity, read_server_update(payloads[1].second).frame));
    REQUIRE(server.retained_quantized_frame_count() == 1);
    REQUIRE(server.retained_quantized_frame_bytes() == sizeof(ashiato_sync_tests::QuantizedNetworkedPosition));

    REQUIRE(server.remove_client(registry, 1));
    REQUIRE(server.retained_quantized_frame_count() == 1);
    REQUIRE(server.remove_client(registry, 2));
    REQUIRE(server.retained_quantized_frame_count() == 0);
    REQUIRE(server.retained_quantized_frame_bytes() == 0);
}

TEST_CASE("replication server trims unacked pending quantized frames per entity") {
    ashiato::Registry registry;
    const ashiato::Entity position_component =
        ashiato::sync::register_sync_component<NetworkedPosition>(registry, "NetworkedPosition");
    const ashiato::sync::SyncArchetypeId archetype = ashiato::sync::define_archetype(
        registry,
        "NetworkedActor",
        {{position_component, ashiato::sync::ReplicationAudience::All}});
    const ashiato::Entity entity = registry.create();
    REQUIRE(registry.add<NetworkedPosition>(entity, NetworkedPosition{0.0f, 0.0f}) != nullptr);

    std::vector<ashiato::BitBuffer> payloads;
    ashiato::sync::ReplicationServerOptions options;
    options.bandwidth_limit_bytes_per_tick = 1024;
    options.transport = [&](ashiato::sync::ClientId, const ashiato::BitBuffer& payload) {
        payloads.push_back(payload);
    };
    ashiato::sync::ReplicationServer server(registry, options);
    REQUIRE(server.add_client(1));
    REQUIRE(start_sync(registry, entity, archetype));

    for (int frame = 0; frame < 70; ++frame) {
        registry.write<NetworkedPosition>(entity) =
            NetworkedPosition{static_cast<float>(frame), static_cast<float>(frame)};
        server.tick(registry, server.options().fixed_dt_seconds);
    }

    REQUIRE(payloads.size() == 70U);
    REQUIRE(server.retained_quantized_frame_count() <= 64U);
    REQUIRE(server.retained_quantized_frame_bytes() <= 64U * sizeof(ashiato_sync_tests::QuantizedNetworkedPosition));
}

TEST_CASE("replication server keeps swapped clients addressable after removal") {
    ashiato::Registry registry;
    const ashiato::Entity position_component =
        ashiato::sync::register_sync_component<NetworkedPosition>(registry, "NetworkedPosition");
    const ashiato::sync::SyncArchetypeId archetype = ashiato::sync::define_archetype(
        registry,
        "NetworkedActor",
        {{position_component, ashiato::sync::ReplicationAudience::All}});
    const ashiato::Entity entity = registry.create();
    REQUIRE(registry.add<NetworkedPosition>(entity, NetworkedPosition{1.0f, 2.0f}) != nullptr);

    std::vector<std::pair<ashiato::sync::ClientId, ashiato::BitBuffer>> payloads;
    ashiato::sync::ReplicationServerOptions options;
    options.bandwidth_limit_bytes_per_tick = 1024;
    options.transport = [&](ashiato::sync::ClientId client, const ashiato::BitBuffer& payload) {
        payloads.push_back({client, payload});
    };

    ashiato::sync::ReplicationServer server(registry, options);
    REQUIRE(server.add_client(1));
    REQUIRE(server.add_client(2));
    REQUIRE(start_sync(registry, entity, archetype));

    server.tick(registry, server.options().fixed_dt_seconds);
    REQUIRE(payloads.size() == 2);

    std::uint32_t client_two_packet_id = 0;
    for (const auto& sent : payloads) {
        if (sent.first == 2) {
            client_two_packet_id = read_server_update(sent.second).packet_id;
        }
    }
    REQUIRE(client_two_packet_id != 0);

    REQUIRE(server.remove_client(registry, 1));
    REQUIRE(server.has_client(2));
    REQUIRE(server.process_packet(registry, 2, write_ack_packet(client_two_packet_id)));
}

TEST_CASE("replication server records bandwidth savings for ACKed delta updates") {
    ashiato::Registry registry;
    const ashiato::Entity probe_component =
        ashiato::sync::register_sync_component<BandwidthProbe>(registry, "BandwidthProbe");
    const ashiato::sync::SyncArchetypeId archetype = ashiato::sync::define_archetype(
        registry,
        "BandwidthActor",
        {{probe_component, ashiato::sync::ReplicationAudience::All}});
    const ashiato::Entity entity = registry.create();
    REQUIRE(registry.add<BandwidthProbe>(entity, BandwidthProbe{100}) != nullptr);

    std::vector<ashiato::BitBuffer> payloads;
    ashiato::sync::ReplicationServerOptions options;
    options.bandwidth_limit_bytes_per_tick = 1024;
    options.transport = [&](ashiato::sync::ClientId, const ashiato::BitBuffer& payload) {
        payloads.push_back(payload);
    };

    ashiato::sync::ReplicationServer server(registry, options);
    REQUIRE(server.add_client(1));
    REQUIRE(start_sync(registry, entity, archetype));

    server.tick(registry, server.options().fixed_dt_seconds);
    REQUIRE(payloads.back().byte_size() == 22);
    REQUIRE(server.acknowledge_entity(1, entity, read_server_update(payloads.back()).frame));

    registry.write<BandwidthProbe>(entity) = BandwidthProbe{105};
    server.tick(registry, server.options().fixed_dt_seconds);

    const std::size_t expected_delta_bits = ashiato::sync::protocol::server_update_header_bits +
        1U + ashiato::sync::protocol::network_entity_id_encoded_bits(1U) + 1U +
        (1U + ashiato::sync::protocol::baseline_frame_delta_bits) + 1U + 9U;
    REQUIRE(payloads.back().byte_size() == ashiato::sync::protocol::bytes_for_bits(expected_delta_bits));
}

TEST_CASE("replication server keeps a quantized frame a second client refuses for budget") {
    ashiato::Registry registry;
    const ashiato::Entity probe_component =
        ashiato::sync::register_sync_component<BandwidthProbe>(registry, "BandwidthProbe");
    const ashiato::sync::SyncArchetypeId archetype = ashiato::sync::define_archetype(
        registry,
        "Probed",
        {{probe_component, ashiato::sync::ReplicationAudience::All}});
    const ashiato::Entity shared = registry.create();
    const ashiato::Entity filler = registry.create();
    REQUIRE(registry.add<BandwidthProbe>(shared, BandwidthProbe{1}) != nullptr);
    REQUIRE(registry.add<BandwidthProbe>(filler, BandwidthProbe{2}) != nullptr);

    std::vector<std::pair<ashiato::sync::ClientId, ashiato::BitBuffer>> payloads;
    ashiato::sync::ReplicationServerOptions options;
    // A budget that carries exactly one entity record per client per tick, and an MTU large
    // enough that a refused record is refused for budget and never for size.
    options.bandwidth_limit_bytes_per_tick = 22;
    options.mtu_bytes = 4096;
    options.prioritizer_interval_frames = 0;
    // Client 1 serializes `shared` first and has budget for it. Client 2 spends its budget on
    // `filler`, so it serializes `shared` second and cannot fit it. Both clients quantize the
    // same entity on the same frame, so both see the same quantized frame.
    options.prioritizer = [&](ashiato::sync::ClientId client, ashiato::sync::ReplicationPriorityObject object) {
        ashiato::sync::ReplicationPriorityDecision decision;
        const ashiato::Entity wanted_first = client == 1 ? shared : filler;
        decision.priority = object.entity == wanted_first ? 100.0f : 1.0f;
        return decision;
    };
    options.transport = [&](ashiato::sync::ClientId client, const ashiato::BitBuffer& payload) {
        payloads.push_back({client, payload});
    };

    ashiato::sync::ReplicationServer server(registry, options);
    REQUIRE(server.add_client(1));
    REQUIRE(server.add_client(2));
    REQUIRE(start_sync(registry, shared, archetype));
    REQUIRE(start_sync(registry, filler, archetype));

    server.tick(registry, server.options().fixed_dt_seconds);

    REQUIRE(payloads.size() == 2);
    REQUIRE(payloads[0].first == 1);
    REQUIRE(payloads[1].first == 2);
    const ashiato::sync::SyncFrame sent_frame = read_server_update(payloads[0].second).frame;
    REQUIRE(read_server_update(payloads[0].second).entities.size() == 1);
    REQUIRE(read_server_update(payloads[1].second).entities.size() == 1);

    // Client 1 holds the quantized frame it was sent; client 2 holds the one it was sent.
    REQUIRE(server.retained_quantized_frame_count() == 2);

    // And the server can still honour client 1's ACK for the frame it really sent.
    REQUIRE(server.acknowledge_entity(1, shared, sent_frame));
}
