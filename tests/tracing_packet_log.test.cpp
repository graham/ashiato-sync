#include "test_tracing_helpers.hpp"

#include <catch2/catch_test_macros.hpp>

// Every test here enables packet logs, which exist only with ASHIATO_SYNC_TRACE_PACKET_LOGS.
#if defined(ASHIATO_SYNC_ENABLE_TRACING) && defined(ASHIATO_SYNC_TRACE_PACKET_LOGS)

#include <algorithm>
#include <array>
#include <cstdint>
#include <filesystem>
#include <fstream>
#include <string>
#include <utility>
#include <vector>

using namespace ashiato_sync_tests;

TEST_CASE("packet log tracing is opt-in and records client and server packet details") {
    std::vector<ashiato::sync::SyncTraceEvent> gated_events;
    ashiato::sync::SyncTracer gated_tracer;
    gated_tracer.set_callbacks(ashiato::sync::SyncTraceCallbacks{
        [&](const ashiato::sync::SyncTraceEvent& event) { gated_events.push_back(event); }});
    ashiato::sync::SyncTraceEvent gated_event;
    gated_event.type = ashiato::sync::SyncTraceEventType::PacketLog;
    gated_event.role = ashiato::sync::SyncTraceRole::Client;
    gated_event.data = "direction=out";
    gated_tracer.trace(gated_event);
    REQUIRE(gated_events.empty());
    gated_tracer.set_packet_logs_enabled(true);
    gated_tracer.trace(gated_event);
    REQUIRE(gated_events.size() == 1);

    ashiato::Registry server_registry;
    const ashiato::sync::SyncArchetypeId server_archetype = define_networked_archetype(server_registry);
    const ashiato::Entity server_entity = server_registry.create();
    REQUIRE(server_registry.add<ashiato_sync_tests::NetworkedPosition>(
        server_entity,
        ashiato_sync_tests::NetworkedPosition{1.0f, 2.0f}) != nullptr);

    std::vector<ashiato::BitBuffer> server_packets;
    std::vector<ashiato::sync::SyncTraceEvent> server_events;
    ashiato::sync::SyncTracer server_tracer;
    server_tracer.set_packet_logs_enabled(true);
    server_tracer.set_callbacks(ashiato::sync::SyncTraceCallbacks{
        [&](const ashiato::sync::SyncTraceEvent& event) { server_events.push_back(event); }});
    ashiato::sync::ReplicationServerOptions server_options;
    server_options.transport = [&](ashiato::sync::ClientId, const ashiato::BitBuffer& packet) {
        server_packets.push_back(packet);
    };
    ashiato::sync::ReplicationServer server(server_registry, server_options);
    server.set_tracer(&server_tracer);
    REQUIRE(server.add_client(1));
    REQUIRE(start_sync(server_registry, server_entity, server_archetype));
    server.tick(server_registry, server.options().fixed_dt_seconds);
    REQUIRE(server_packets.size() == 1);
    REQUIRE(std::any_of(server_events.begin(), server_events.end(), [&](const ashiato::sync::SyncTraceEvent& event) {
        return event.type == ashiato::sync::SyncTraceEventType::PacketLog &&
            event.role == ashiato::sync::SyncTraceRole::Server &&
            event.data.find("message=server_update") != std::string::npos &&
            event.data.find("sequence=1") != std::string::npos &&
            event.data.find("record_count=1") != std::string::npos &&
            event.data.find("replicated_count=1") != std::string::npos &&
            event.data.find("client_count=1") != std::string::npos &&
            event.data.find("updated_server_entities=[" + std::to_string(server_entity.value) + "]") != std::string::npos;
    }));

    ashiato::Registry client_registry;
    const ashiato::sync::SyncArchetypeId client_archetype = define_networked_archetype(client_registry);
    REQUIRE(client_archetype == server_archetype);
    ashiato_sync_tests::configure_test_client_registry(client_registry, 1);
    std::vector<ashiato::sync::SyncTraceEvent> client_events;
    ashiato::sync::SyncTracer client_tracer;
    client_tracer.set_packet_logs_enabled(true);
    client_tracer.set_callbacks(ashiato::sync::SyncTraceCallbacks{
        [&](const ashiato::sync::SyncTraceEvent& event) { client_events.push_back(event); }});
    ashiato::sync::ReplicationClient client(client_registry, ashiato_sync_tests::make_test_client_options(client_registry, {}));
    client.set_tracer(&client_tracer);
    REQUIRE(client.receive(client_registry, server_packets[0]));
    REQUIRE(std::any_of(client_events.begin(), client_events.end(), [](const ashiato::sync::SyncTraceEvent& event) {
        return event.type == ashiato::sync::SyncTraceEventType::PacketLog &&
            event.role == ashiato::sync::SyncTraceRole::Client &&
            event.data.find("message=server_update") != std::string::npos &&
            event.data.find("sequence=1") != std::string::npos &&
            event.data.find("record_count=1") != std::string::npos &&
            event.data.find("applied=true") != std::string::npos;
    }));
    std::vector<ashiato::BitBuffer> ack_packets = client.drain_ack_packets();
    REQUIRE(ack_packets.size() == 1);
    REQUIRE(std::any_of(client_events.begin(), client_events.end(), [](const ashiato::sync::SyncTraceEvent& event) {
        return event.type == ashiato::sync::SyncTraceEventType::PacketLog &&
            event.role == ashiato::sync::SyncTraceRole::Client &&
            event.data.find("message=client_ack") != std::string::npos &&
            event.data.find("acks=[1]") != std::string::npos;
    }));
    REQUIRE(server.process_packet(server_registry, 1, ack_packets[0]));
    REQUIRE(std::any_of(server_events.begin(), server_events.end(), [](const ashiato::sync::SyncTraceEvent& event) {
        return event.type == ashiato::sync::SyncTraceEventType::PacketLog &&
            event.role == ashiato::sync::SyncTraceRole::Server &&
            event.data.find("message=client_ack") != std::string::npos &&
            event.data.find("acks=[1]") != std::string::npos;
    }));
}

TEST_CASE("packet log tracing records ACK-only traffic as client ACK packets") {
    ashiato::Registry server_registry;
    const ashiato::sync::SyncArchetypeId server_archetype = define_networked_archetype(server_registry);
    const ashiato::Entity server_entity = server_registry.create();
    REQUIRE(server_registry.add<ashiato_sync_tests::NetworkedPosition>(
        server_entity,
        ashiato_sync_tests::NetworkedPosition{1.0f, 2.0f}) != nullptr);

    std::vector<ashiato::BitBuffer> server_packets;
    ashiato::sync::ReplicationServerOptions server_options;
    server_options.transport = [&](ashiato::sync::ClientId, const ashiato::BitBuffer& packet) {
        server_packets.push_back(packet);
    };
    ashiato::sync::ReplicationServer server(server_registry, server_options);
    REQUIRE(server.add_client(1));
    REQUIRE(start_sync(server_registry, server_entity, server_archetype));
    server.tick(server_registry, server.options().fixed_dt_seconds);
    REQUIRE(server_packets.size() == 1);

    ashiato::Registry client_registry;
    const ashiato::sync::SyncArchetypeId client_archetype = define_networked_archetype(client_registry);
    REQUIRE(client_archetype == server_archetype);
    ashiato_sync_tests::configure_test_client_registry(client_registry, 1);
    REQUIRE(ashiato::sync::set_client_input_component<ashiato_sync_tests::NetworkedPosition>(client_registry));

    std::vector<ashiato::sync::SyncTraceEvent> client_events;
    ashiato::sync::SyncTracer client_tracer;
    client_tracer.set_packet_logs_enabled(true);
    client_tracer.set_callbacks(ashiato::sync::SyncTraceCallbacks{
        [&](const ashiato::sync::SyncTraceEvent& event) { client_events.push_back(event); }});
    ashiato::sync::ReplicationClient client(client_registry, ashiato_sync_tests::make_test_client_options(client_registry, {}));
    client.set_tracer(&client_tracer);
    REQUIRE(client.receive(client_registry, server_packets[0]));

    std::vector<ashiato::BitBuffer> packets = client.drain_packets();
    REQUIRE(std::any_of(packets.begin(), packets.end(), [](ashiato::BitBuffer packet) {
        return static_cast<std::uint8_t>(packet.read_bits(ashiato::sync::protocol::message_bits)) == ashiato::sync::protocol::client_ack_message;
    }));
    REQUIRE(std::none_of(packets.begin(), packets.end(), [](ashiato::BitBuffer packet) {
        return static_cast<std::uint8_t>(packet.read_bits(ashiato::sync::protocol::message_bits)) == ashiato::sync::protocol::client_input_message;
    }));
    REQUIRE(std::any_of(client_events.begin(), client_events.end(), [](const ashiato::sync::SyncTraceEvent& event) {
        return event.type == ashiato::sync::SyncTraceEventType::PacketLog &&
            event.role == ashiato::sync::SyncTraceRole::Client &&
            event.data.find("message=client_ack") != std::string::npos &&
            event.data.find("acks=[1]") != std::string::npos;
    }));
    REQUIRE(std::none_of(client_events.begin(), client_events.end(), [](const ashiato::sync::SyncTraceEvent& event) {
        return event.type == ashiato::sync::SyncTraceEventType::PacketLog &&
            event.role == ashiato::sync::SyncTraceRole::Client &&
            event.data.find("message=client_input") != std::string::npos;
    }));
}

#ifdef ASHIATO_SYNC_TRACE_PACKET_LOGS
TEST_CASE("packet log tracing records why a server update was not applied") {
    ashiato::Registry registry;
    REQUIRE(ashiato_sync_tests::define_position_archetype(registry).value == 0);
    ashiato_sync_tests::configure_test_client_registry(registry, 1);

    std::vector<ashiato::sync::SyncTraceEvent> events;
    ashiato::sync::SyncTracer tracer;
    tracer.set_packet_logs_enabled(true);
    tracer.set_callbacks(ashiato::sync::SyncTraceCallbacks{
        [&](const ashiato::sync::SyncTraceEvent& event) { events.push_back(event); }});

    ashiato::sync::ReplicationClient client(registry, ashiato_sync_tests::make_test_client_options(registry, {}));
    client.set_tracer(&tracer);

    ashiato::BitBuffer packet;
    packet.write_bits(ashiato::sync::protocol::server_update_message, ashiato::sync::protocol::message_bits);
    packet.write_bits(1, 32U);
    packet.write_bits(1, ashiato::sync::protocol::server_packet_id_bits);
    packet.write_bits(0, 32U);
    packet.write_bits(1, 16U);

    REQUIRE_FALSE(client.receive(registry, packet));
    REQUIRE(std::any_of(events.begin(), events.end(), [](const ashiato::sync::SyncTraceEvent& event) {
        return event.type == ashiato::sync::SyncTraceEventType::PacketLog &&
            event.role == ashiato::sync::SyncTraceRole::Client &&
            event.data.find("message=server_update") != std::string::npos &&
            event.data.find("applied=false") != std::string::npos &&
            event.data.find("apply_failure=record_0_record_header_read_failed") != std::string::npos;
    }));
}

TEST_CASE("packet log tracing records ping and pong traffic") {
    ashiato::Registry client_registry;
    ashiato_sync_tests::configure_test_client_registry(client_registry, 1);
    std::vector<ashiato::sync::SyncTraceEvent> client_events;
    ashiato::sync::SyncTracer client_tracer;
    client_tracer.set_packet_logs_enabled(true);
    client_tracer.set_callbacks(ashiato::sync::SyncTraceCallbacks{
        [&](const ashiato::sync::SyncTraceEvent& event) { client_events.push_back(event); }});
    ashiato::sync::ReplicationClient client(client_registry, ashiato_sync_tests::make_test_client_options(client_registry, {}));
    client.set_tracer(&client_tracer);

    PingPacket ping;
    REQUIRE(drain_ping(client, client_registry, 0.0, ping));
    REQUIRE(std::any_of(client_events.begin(), client_events.end(), [&](const ashiato::sync::SyncTraceEvent& event) {
        return event.type == ashiato::sync::SyncTraceEventType::PacketLog &&
            event.role == ashiato::sync::SyncTraceRole::Client &&
            event.data.find("direction=out") != std::string::npos &&
            event.data.find("message=client_ping") != std::string::npos &&
            event.data.find("sequence=" + std::to_string(ping.sequence)) != std::string::npos;
    }));

    ashiato::Registry server_registry;
    ashiato_sync_tests::configure_test_server_registry(server_registry);
    std::vector<ashiato::BitBuffer> server_packets;
    std::vector<ashiato::sync::SyncTraceEvent> server_events;
    ashiato::sync::SyncTracer server_tracer;
    server_tracer.set_packet_logs_enabled(true);
    server_tracer.set_callbacks(ashiato::sync::SyncTraceCallbacks{
        [&](const ashiato::sync::SyncTraceEvent& event) { server_events.push_back(event); }});
    ashiato::sync::ReplicationServerOptions server_options;
    server_options.transport = [&](ashiato::sync::ClientId, const ashiato::BitBuffer& packet) {
        server_packets.push_back(packet);
    };
    ashiato::sync::ReplicationServer server(server_registry, server_options);
    server.set_tracer(&server_tracer);
    REQUIRE(server.add_client(1));

    ashiato::BitBuffer ping_packet;
    ping_packet.write_bits(ashiato::sync::protocol::client_ping_message, ashiato::sync::protocol::message_bits);
    ping_packet.write_bits(ping.sequence, 32U);
    REQUIRE(server.process_packet(server_registry, 1, ping_packet));
    REQUIRE(server_packets.size() == 1);
    REQUIRE(std::any_of(server_events.begin(), server_events.end(), [&](const ashiato::sync::SyncTraceEvent& event) {
        return event.type == ashiato::sync::SyncTraceEventType::PacketLog &&
            event.role == ashiato::sync::SyncTraceRole::Server &&
            event.data.find("direction=in") != std::string::npos &&
            event.data.find("message=client_ping") != std::string::npos &&
            event.data.find("sequence=" + std::to_string(ping.sequence)) != std::string::npos;
    }));
    REQUIRE(std::any_of(server_events.begin(), server_events.end(), [&](const ashiato::sync::SyncTraceEvent& event) {
        return event.type == ashiato::sync::SyncTraceEventType::PacketLog &&
            event.role == ashiato::sync::SyncTraceRole::Server &&
            event.client == 1 &&
            event.data.find("direction=out") != std::string::npos &&
            event.data.find("message=server_pong") != std::string::npos &&
            event.data.find("client=1") != std::string::npos &&
            event.data.find("peer=1") != std::string::npos &&
            event.data.find("sequence=" + std::to_string(ping.sequence)) != std::string::npos &&
            event.data.find("server_receive_frame=") != std::string::npos &&
            event.data.find("server_frame=") != std::string::npos;
    }));

    REQUIRE(client.receive(client_registry, server_packets[0]));
    REQUIRE(std::any_of(client_events.begin(), client_events.end(), [&](const ashiato::sync::SyncTraceEvent& event) {
        return event.type == ashiato::sync::SyncTraceEventType::PacketLog &&
            event.role == ashiato::sync::SyncTraceRole::Client &&
            event.data.find("direction=in") != std::string::npos &&
            event.data.find("message=server_pong") != std::string::npos &&
            event.data.find("sequence=" + std::to_string(ping.sequence)) != std::string::npos &&
            event.data.find("server_receive_frame=") != std::string::npos &&
            event.data.find("server_frame=") != std::string::npos;
    }));
}
#endif

TEST_CASE("packet log tracing records cue summaries and cue payload data") {
    ashiato::Registry server_registry;
    const ashiato::sync::SyncArchetypeId server_archetype =
        ashiato_sync_tests::define_position_archetype(server_registry);
    ashiato::sync::register_sync_cue<ashiato_sync_tests::TestCue>(server_registry);
    const ashiato::Entity server_entity = server_registry.create();
    REQUIRE(server_registry.add<ashiato_sync_tests::Position>(
        server_entity,
        ashiato_sync_tests::Position{1.0f, 2.0f}) != nullptr);

    std::vector<ashiato::BitBuffer> packets;
    std::vector<ashiato::sync::SyncTraceEvent> server_events;
    ashiato::sync::SyncTracer server_tracer;
    server_tracer.set_packet_logs_enabled(true);
    server_tracer.set_frame_data_enabled(true);
    server_tracer.set_callbacks(ashiato::sync::SyncTraceCallbacks{
        [&](const ashiato::sync::SyncTraceEvent& event) { server_events.push_back(event); }});
    ashiato::sync::ReplicationServerOptions server_options;
    server_options.transport = [&](ashiato::sync::ClientId, const ashiato::BitBuffer& packet) {
        packets.push_back(packet);
    };
    ashiato::sync::ReplicationServer server(server_registry, server_options);
    server.set_tracer(&server_tracer);
    REQUIRE(server.add_client(1));
    REQUIRE(start_sync(server_registry, server_entity, server_archetype));
    REQUIRE(ashiato_sync_tests::emit_test_cue(server_registry, server_entity, 1, ashiato_sync_tests::TestCue{7}, 1.0f));
    server.tick(server_registry, server.options().fixed_dt_seconds);
    REQUIRE(packets.size() == 1);
    REQUIRE(std::any_of(server_events.begin(), server_events.end(), [](const ashiato::sync::SyncTraceEvent& event) {
        return event.type == ashiato::sync::SyncTraceEventType::PacketLog &&
            event.role == ashiato::sync::SyncTraceRole::Server &&
            event.data.find("message=server_update") != std::string::npos &&
            event.data.find("cues=[{") != std::string::npos &&
            event.data.find("type=0") != std::string::npos &&
            event.data.find("data=id=7") != std::string::npos;
    }));

    ashiato::Registry client_registry;
    REQUIRE(ashiato_sync_tests::define_position_archetype(client_registry) == server_archetype);
    client_registry.register_component<ashiato_sync_tests::CuePlayback>("CuePlayback");
    ashiato::sync::register_sync_cue<ashiato_sync_tests::TestCue>(client_registry);
    ashiato_sync_tests::configure_test_client_registry(client_registry, 1);
    std::vector<ashiato::sync::SyncTraceEvent> client_events;
    ashiato::sync::SyncTracer client_tracer;
    client_tracer.set_packet_logs_enabled(true);
    client_tracer.set_frame_data_enabled(true);
    client_tracer.set_callbacks(ashiato::sync::SyncTraceCallbacks{
        [&](const ashiato::sync::SyncTraceEvent& event) { client_events.push_back(event); }});
    ashiato::sync::ReplicationClient client(client_registry, ashiato_sync_tests::make_test_client_options(client_registry, {}));
    client.set_tracer(&client_tracer);
    REQUIRE(client.receive(client_registry, packets[0]));
    REQUIRE(std::any_of(client_events.begin(), client_events.end(), [](const ashiato::sync::SyncTraceEvent& event) {
        return event.type == ashiato::sync::SyncTraceEventType::PacketLog &&
            event.role == ashiato::sync::SyncTraceRole::Client &&
            event.data.find("message=server_update") != std::string::npos &&
            event.data.find("cues=[{") != std::string::npos &&
            event.data.find("type=0") != std::string::npos &&
            event.data.find("data=id=7") != std::string::npos;
    }));
}

#endif
