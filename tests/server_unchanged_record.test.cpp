// An entity whose components are written every tick with the values they already had.
//
// A game system that takes a component by mutable reference marks the entity dirty whether or
// not it changes the value; that is how ashiato hands a non-const query component out. The
// server then finds the entity dirty, compares its quantized components against the client's
// acked baseline, finds nothing changed, and before this fix still wrote a delta record for it:
// the network id, the baseline frame and one all-false "changed" bit per component, every tick,
// for as long as the entity lived. Found by the cockpit game (lane/netbudget, 2026-09-24): a
// parked aircraft cost each client 4 bytes a tick, 240 B/s at 60 Hz, with no component sent.

#include "test_protocol.hpp"

#include <catch2/catch_test_macros.hpp>

#include <algorithm>
#include <cstddef>
#include <cstdint>
#include <memory>
#include <utility>
#include <vector>

using namespace ashiato_sync_tests;

namespace {

struct UnchangedRecordServer {
    ashiato::Registry registry;
    ashiato::Entity still{};
    ashiato::Entity moving{};
    std::vector<ashiato::BitBuffer> payloads;
    std::unique_ptr<ashiato::sync::ReplicationServer> server;
    std::uint32_t still_id = 0;
    std::uint32_t moving_id = 0;

    UnchangedRecordServer() {
        const ashiato::Entity position_component =
            ashiato::sync::register_sync_component<NetworkedPosition>(registry, "NetworkedPosition");
        const ashiato::sync::SyncArchetypeId archetype = ashiato::sync::define_archetype(
            registry, "UnchangedProbe", {{position_component, ashiato::sync::ReplicationAudience::All}});
        still = registry.create();
        moving = registry.create();
        REQUIRE(registry.add<NetworkedPosition>(still, NetworkedPosition{1.0f, 1.0f}) != nullptr);
        REQUIRE(registry.add<NetworkedPosition>(moving, NetworkedPosition{2.0f, 2.0f}) != nullptr);

        ashiato::sync::ReplicationServerOptions options;
        options.transport = [this](ashiato::sync::ClientId, const ashiato::BitBuffer& payload) {
            payloads.push_back(payload);
        };
        server = std::make_unique<ashiato::sync::ReplicationServer>(registry, options);
        REQUIRE(server->add_client(1));
        REQUIRE(start_sync(registry, still, archetype));
        REQUIRE(start_sync(registry, moving, archetype));
    }

    // One tick of a game that rewrites both components, the still one with the value it already
    // had and the moving one half a unit further on, then acknowledges everything it was sent.
    // Returns the records the tick carried for each entity.
    std::pair<std::size_t, std::size_t> tick() {
        registry.write<NetworkedPosition>(still) = NetworkedPosition{1.0f, 1.0f};
        NetworkedPosition& position = registry.write<NetworkedPosition>(moving);
        position.x += 0.5f;
        payloads.clear();
        server->tick(registry, server->options().fixed_dt_seconds);
        std::size_t still_records = 0;
        std::size_t moving_records = 0;
        for (const ashiato::BitBuffer& payload : payloads) {
            const ServerUpdatePacket update = read_server_update(payload);
            if (update.message != ashiato::sync::protocol::server_update_message) {
                continue;
            }
            for (const EntityRecord& record : update.entities) {
                // A client's network ids are its own, so each entity is named by its first full
                // record: the still one starts at x = 1.0, which the codec carries as 10.
                if (record.full && record.components.size() == 1) {
                    const NetworkedPayload value = read_networked_payload(record.components[0].payload);
                    (value.x == 10 ? still_id : moving_id) = record.network_id;
                }
                if (record.network_id == still_id) {
                    ++still_records;
                } else if (record.network_id == moving_id) {
                    ++moving_records;
                }
            }
            REQUIRE(server->process_packet(registry, 1, write_ack_packet(update.packet_id)));
        }
        return {still_records, moving_records};
    }
};

constexpr std::size_t settle_ticks = 4;
constexpr std::size_t measured_ticks = 20;

}  // namespace

TEST_CASE("an entity rewritten with the value it had is not sent once its client holds that value") {
    UnchangedRecordServer world;
    std::size_t first_still = 0;
    for (std::size_t tick = 0; tick < settle_ticks; ++tick) {
        first_still += world.tick().first;
    }
    // THE CONTROL on the first half: the still entity was sent at least once, in full, so the
    // count below is about records it did not need, not about an entity that never replicated.
    REQUIRE(first_still >= 1);

    std::size_t still_records = 0;
    std::size_t moving_records = 0;
    for (std::size_t tick = 0; tick < measured_ticks; ++tick) {
        const auto [still, moving] = world.tick();
        still_records += still;
        moving_records += moving;
    }
    // THE CONTROL on the second half: a component that really changes is still sent every tick.
    REQUIRE(moving_records == measured_ticks);
    // THE CLAIM: nothing the client does not already have. Before the fix this is 20, one empty
    // delta record a tick.
    REQUIRE(still_records == 0);
}
