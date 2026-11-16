#include "test_protocol.hpp"

#include "client/state.hpp"

#include <catch2/catch_approx.hpp>
#include <catch2/catch_test_macros.hpp>

#include <array>
#include <cstddef>
#include <cstdint>
#include <memory>
#include <set>
#include <utility>
#include <vector>

using namespace ashiato_sync_tests;

namespace {

constexpr ashiato::sync::ClientId test_client_id = 1;
constexpr std::size_t baseline_history_size = ashiato::sync::client_detail::max_baseline_history_per_entity;

struct TrackedEntity {
    ashiato::Entity server;
    ashiato::sync::ClientEntityNetworkId network_id = ashiato::sync::invalid_client_entity_network_id;
};

class BaselineRecoveryHarness {
public:
    explicit BaselineRecoveryHarness(bool include_health = false) : include_health_(include_health) {
        server_archetype_ = define_archetype(server_registry_);
        client_archetype_ = define_archetype(client_registry_);
        REQUIRE(client_archetype_ == server_archetype_);
        REQUIRE(configure_test_client_registry(client_registry_, test_client_id));

        ashiato::sync::ReplicationServerOptions server_options;
        server_options.bandwidth_limit_bytes_per_tick = 64U * 1024U;
        server_options.transport = [this](ashiato::sync::ClientId client, const ashiato::BitBuffer& packet) {
            REQUIRE(client == test_client_id);
            downstream_.push_back(packet);
        };
        server_ = std::make_unique<ashiato::sync::ReplicationServer>(server_registry_, server_options);
        REQUIRE(server_->add_client(test_client_id));

        ashiato::sync::ReplicationClientOptions client_options;
        client_options.entities.default_mode = ashiato::sync::ReplicationClientMode::Snap;
        client_ = std::make_unique<ashiato::sync::ReplicationClient>(
            client_registry_,
            make_test_client_options(client_registry_, client_options));
    }

    TrackedEntity add_entity(NetworkedPosition position = {}, Health health = {}) {
        const ashiato::Entity entity = server_registry_.create();
        REQUIRE(server_registry_.add<NetworkedPosition>(entity, position) != nullptr);
        if (include_health_) {
            REQUIRE(server_registry_.add<Health>(entity, health) != nullptr);
        }
        REQUIRE(start_sync(server_registry_, entity, server_archetype_));
        return TrackedEntity{entity};
    }

    ServerUpdatePacket establish_initial_baseline(std::vector<TrackedEntity>& entities) {
        tick_server();
        ashiato::BitBuffer packet = take_single_packet();
        const ServerUpdatePacket update = read_update(packet);
        REQUIRE(update.entities.size() == entities.size());
        for (const EntityRecord& record : update.entities) {
            REQUIRE(record.full);
        }
        REQUIRE(client_->receive(client_registry_, packet));
        forward_all_client_acks();
        for (TrackedEntity& entity : entities) {
            entity.network_id = server_->client_entity_network_id(test_client_id, entity.server);
            REQUIRE(entity.network_id != ashiato::sync::invalid_client_entity_network_id);
            REQUIRE(client_->local_entity(entity.network_id));
        }
        return update;
    }

    ServerUpdatePacket establish_initial_baseline(TrackedEntity& entity) {
        std::vector<TrackedEntity> entities{entity};
        const ServerUpdatePacket update = establish_initial_baseline(entities);
        entity = entities.front();
        return update;
    }

    void tick_server() {
        REQUIRE(server_->tick(server_registry_, server_->options().fixed_dt_seconds));
    }

    ashiato::BitBuffer take_single_packet() {
        REQUIRE(downstream_.size() == 1);
        ashiato::BitBuffer packet = downstream_.front();
        downstream_.clear();
        return packet;
    }

    void require_no_packet() {
        REQUIRE(downstream_.empty());
    }

    ServerUpdatePacket read_update(const ashiato::BitBuffer& packet) const {
        return read_server_update(packet, include_health_ ? 3U : 2U);
    }

    bool receive(const ashiato::BitBuffer& packet) {
        return client_->receive(client_registry_, packet);
    }

    void collect_client_acks(std::vector<ashiato::BitBuffer>& destination) {
        std::vector<ashiato::BitBuffer> packets = client_->drain_ack_packets();
        destination.insert(
            destination.end(),
            std::make_move_iterator(packets.begin()),
            std::make_move_iterator(packets.end()));
    }

    void forward_all_client_acks() {
        for (const ashiato::BitBuffer& packet : client_->drain_ack_packets()) {
            REQUIRE(server_->process_packet(server_registry_, test_client_id, packet));
        }
    }

    bool forward_ack(const ashiato::BitBuffer& packet) {
        return server_->process_packet(server_registry_, test_client_id, packet);
    }

    // Forwards every queued ACK without requiring the server to match each one (an ACK it cannot credit is dropped).
    void forward_all_client_acks_unchecked() {
        for (const ashiato::BitBuffer& packet : client_->drain_ack_packets()) {
            (void)server_->process_packet(server_registry_, test_client_id, packet);
        }
    }

    void set_position(const TrackedEntity& entity, float x) {
        server_registry_.write<NetworkedPosition>(entity.server) = NetworkedPosition{x, x};
    }

    NetworkedPosition client_position(const TrackedEntity& entity) const {
        const ashiato::Entity local = client_->local_entity(entity.network_id);
        REQUIRE(local);
        return client_registry_.get<NetworkedPosition>(local);
    }

    ashiato::Registry& server_registry() {
        return server_registry_;
    }

    ashiato::sync::ReplicationServer& server() {
        return *server_;
    }

private:
    ashiato::sync::SyncArchetypeId define_archetype(ashiato::Registry& registry) const {
        const ashiato::Entity position =
            ashiato::sync::register_sync_component<NetworkedPosition>(registry, "NetworkedPosition");
        if (!include_health_) {
            return ashiato::sync::define_archetype(
                registry,
                "BaselineRecoveryActor",
                {{position, ashiato::sync::ReplicationAudience::All}});
        }
        const ashiato::Entity health = ashiato::sync::register_sync_component<Health>(registry, "Health");
        return ashiato::sync::define_archetype(
            registry,
            "BaselineRecoveryActorWithHealth",
            {
                {position, ashiato::sync::ReplicationAudience::All},
                {health, ashiato::sync::ReplicationAudience::All},
            });
    }

    bool include_health_ = false;
    ashiato::Registry server_registry_;
    ashiato::Registry client_registry_;
    ashiato::sync::SyncArchetypeId server_archetype_;
    ashiato::sync::SyncArchetypeId client_archetype_;
    std::vector<ashiato::BitBuffer> downstream_;
    std::unique_ptr<ashiato::sync::ReplicationServer> server_;
    std::unique_ptr<ashiato::sync::ReplicationClient> client_;
};

float alternating_position(std::size_t update_index, std::size_t entity_index = 0) {
    return ((update_index + entity_index) & 1U) == 0U ? 1.0f : 2.0f;
}

void require_position(const NetworkedPosition& actual, float expected_x) {
    REQUIRE(actual.x == Catch::Approx(expected_x));
    REQUIRE(actual.y == Catch::Approx(expected_x));
}

}  // namespace

TEST_CASE("queued update replay remains decodable at baseline history boundaries", "[baseline-recovery]") {
    constexpr std::array<std::size_t, 6> update_counts{63U, 64U, 65U, 127U, 128U, 129U};

    for (const std::size_t update_count : update_counts) {
        DYNAMIC_SECTION(update_count << " queued updates") {
            BaselineRecoveryHarness harness;
            TrackedEntity entity = harness.add_entity();
            const ashiato::sync::SyncFrame baseline_frame = harness.establish_initial_baseline(entity).frame;
            std::vector<ashiato::BitBuffer> queued;
            queued.reserve(update_count);

            for (std::size_t update = 1; update <= update_count; ++update) {
                harness.set_position(entity, alternating_position(update));
                harness.tick_server();
                ashiato::BitBuffer packet = harness.take_single_packet();
                const ServerUpdatePacket decoded = harness.read_update(packet);
                REQUIRE(decoded.entities.size() == 1);
                REQUIRE_FALSE(decoded.entities.front().full);
                REQUIRE(decoded.entities.front().baseline_frame == baseline_frame);
                queued.push_back(std::move(packet));
            }

            std::size_t rejected_updates = 0;
            std::vector<ashiato::BitBuffer> delayed_acks;
            for (const ashiato::BitBuffer& packet : queued) {
                if (!harness.receive(packet)) {
                    ++rejected_updates;
                }
                harness.collect_client_acks(delayed_acks);
            }

            CAPTURE(update_count, rejected_updates);
            CHECK(rejected_updates == 0U);
            require_position(harness.client_position(entity), alternating_position(update_count));
        }
    }
}

TEST_CASE(
    "ACK blackout retains forward progress and converges after the newest ACK is restored",
    "[baseline-recovery]") {
    BaselineRecoveryHarness harness;
    TrackedEntity entity = harness.add_entity();
    const ashiato::sync::SyncFrame baseline_frame = harness.establish_initial_baseline(entity).frame;
    std::vector<ashiato::BitBuffer> delayed_acks;
    std::size_t rejected_updates = 0;

    constexpr std::size_t blackout_updates = 129;
    for (std::size_t update = 1; update <= blackout_updates; ++update) {
        harness.set_position(entity, alternating_position(update));
        harness.tick_server();
        ashiato::BitBuffer packet = harness.take_single_packet();
        const ServerUpdatePacket decoded = harness.read_update(packet);
        REQUIRE(decoded.entities.size() == 1);
        REQUIRE_FALSE(decoded.entities.front().full);
        REQUIRE(decoded.entities.front().baseline_frame == baseline_frame);
        if (!harness.receive(packet)) {
            ++rejected_updates;
        }
        harness.collect_client_acks(delayed_acks);
    }

    REQUIRE_FALSE(delayed_acks.empty());
    CHECK(rejected_updates == 0U);

    // Model all earlier ACKs being lost and only the newest successfully applied update surviving.
    REQUIRE(harness.forward_ack(delayed_acks.back()));
    for (std::size_t recovery = 1; recovery <= 3U; ++recovery) {
        const float expected = 3.0f + static_cast<float>(recovery);
        harness.set_position(entity, expected);
        harness.tick_server();
        const ashiato::BitBuffer packet = harness.take_single_packet();
        REQUIRE(harness.receive(packet));
        harness.forward_all_client_acks();
        require_position(harness.client_position(entity), expected);
    }
}

TEST_CASE("out-of-order ACKs do not regress the selected server baseline", "[baseline-recovery]") {
    BaselineRecoveryHarness harness;
    TrackedEntity entity = harness.add_entity();
    harness.establish_initial_baseline(entity);
    std::vector<ashiato::BitBuffer> delayed_acks;
    std::array<ashiato::sync::SyncFrame, 2> sent_frames{};

    for (std::size_t update = 0; update < sent_frames.size(); ++update) {
        harness.set_position(entity, static_cast<float>(update + 1U));
        harness.tick_server();
        const ashiato::BitBuffer packet = harness.take_single_packet();
        sent_frames[update] = harness.read_update(packet).frame;
        REQUIRE(harness.receive(packet));
        harness.collect_client_acks(delayed_acks);
    }
    REQUIRE(delayed_acks.size() == 2U);

    REQUIRE(harness.forward_ack(delayed_acks[1]));
    CHECK_FALSE(harness.forward_ack(delayed_acks[0]));

    harness.set_position(entity, 3.0f);
    harness.tick_server();
    const ashiato::BitBuffer packet = harness.take_single_packet();
    const ServerUpdatePacket decoded = harness.read_update(packet);
    REQUIRE(decoded.entities.size() == 1U);
    REQUIRE_FALSE(decoded.entities.front().full);
    REQUIRE(decoded.entities.front().baseline_frame == sent_frames[1]);
    REQUIRE(harness.receive(packet));
    harness.forward_all_client_acks();
    require_position(harness.client_position(entity), 3.0f);
}

TEST_CASE("an ACK candidate is retired before its client baseline can be evicted", "[baseline-recovery]") {
    BaselineRecoveryHarness harness;
    TrackedEntity entity = harness.add_entity();
    const ashiato::sync::SyncFrame baseline_frame = harness.establish_initial_baseline(entity).frame;
    std::vector<ashiato::BitBuffer> delayed_acks;

    for (std::size_t update = 1; update <= baseline_history_size + 1U; ++update) {
        harness.set_position(entity, alternating_position(update));
        harness.tick_server();
        REQUIRE(harness.receive(harness.take_single_packet()));
        harness.collect_client_acks(delayed_acks);
    }
    REQUIRE(delayed_acks.size() == baseline_history_size + 1U);

    REQUIRE_FALSE(harness.forward_ack(delayed_acks.front()));
    harness.set_position(entity, 3.0f);
    harness.tick_server();
    const ashiato::BitBuffer recovery_packet = harness.take_single_packet();
    const ServerUpdatePacket recovery = harness.read_update(recovery_packet);
    REQUIRE(recovery.entities.size() == 1U);
    REQUIRE_FALSE(recovery.entities.front().full);
    REQUIRE(recovery.entities.front().baseline_frame == baseline_frame);
    REQUIRE(harness.receive(recovery_packet));
    harness.forward_all_client_acks();
    require_position(harness.client_position(entity), 3.0f);
}

TEST_CASE(
    "correlated multi-entity ACK blackout recovers entities with different update cadences",
    "[baseline-recovery]") {
    BaselineRecoveryHarness harness;
    std::vector<TrackedEntity> entities;
    constexpr std::array<std::size_t, 6> cadences{1U, 2U, 3U, 7U, 16U, 31U};
    entities.reserve(cadences.size());
    for (std::size_t index = 0; index < cadences.size(); ++index) {
        entities.push_back(harness.add_entity());
    }
    harness.establish_initial_baseline(entities);

    std::vector<ashiato::BitBuffer> delayed_acks;
    std::size_t rejected_packets = 0;
    constexpr std::size_t blackout_ticks = 129;
    for (std::size_t tick = 1; tick <= blackout_ticks; ++tick) {
        for (std::size_t index = 0; index < entities.size(); ++index) {
            if (tick % cadences[index] == 0U) {
                harness.set_position(entities[index], alternating_position(tick, index));
            }
        }
        harness.tick_server();
        const ashiato::BitBuffer packet = harness.take_single_packet();
        if (!harness.receive(packet)) {
            ++rejected_packets;
        }
        harness.collect_client_acks(delayed_acks);
    }

    CAPTURE(rejected_packets);
    CHECK(rejected_packets == 0U);

    // Reverse delivery makes the first surviving ACK for each entity its newest one.
    for (auto ack = delayed_acks.rbegin(); ack != delayed_acks.rend(); ++ack) {
        (void)harness.forward_ack(*ack);
    }

    for (std::size_t index = 0; index < entities.size(); ++index) {
        const float expected = 10.0f + static_cast<float>(index);
        harness.set_position(entities[index], expected);
    }
    harness.tick_server();
    REQUIRE(harness.receive(harness.take_single_packet()));
    harness.forward_all_client_acks();

    for (std::size_t index = 0; index < entities.size(); ++index) {
        require_position(harness.client_position(entities[index]), 10.0f + static_cast<float>(index));
    }
}

TEST_CASE("a sent full update is a barrier against deltas from an older baseline", "[baseline-recovery]") {
    BaselineRecoveryHarness harness(true);
    TrackedEntity entity = harness.add_entity(NetworkedPosition{}, Health{100});
    const ServerUpdatePacket initial = harness.establish_initial_baseline(entity);

    while (harness.server().frame() < initial.frame + baseline_history_size - 1U) {
        harness.tick_server();
        harness.require_no_packet();
    }

    REQUIRE(harness.server_registry().remove<Health>(entity.server));
    harness.tick_server();
    const ashiato::BitBuffer colliding_full_packet = harness.take_single_packet();
    const ServerUpdatePacket colliding_full = harness.read_update(colliding_full_packet);
    REQUIRE(colliding_full.frame == initial.frame + baseline_history_size);
    REQUIRE(colliding_full.entities.size() == 1U);
    REQUIRE(colliding_full.entities.front().full);
    REQUIRE(harness.receive(colliding_full_packet));
    std::vector<ashiato::BitBuffer> delayed_full_ack;
    harness.collect_client_acks(delayed_full_ack);
    REQUIRE(delayed_full_ack.size() == 1U);

    REQUIRE(harness.server_registry().add<Health>(entity.server, Health{75}) != nullptr);
    harness.set_position(entity, 1.0f);
    harness.tick_server();
    const ashiato::BitBuffer restored_shape_packet = harness.take_single_packet();
    const ServerUpdatePacket restored_shape = harness.read_update(restored_shape_packet);
    REQUIRE(restored_shape.entities.size() == 1U);
    REQUIRE(restored_shape.entities.front().full);
    REQUIRE(harness.receive(restored_shape_packet));
    std::vector<ashiato::BitBuffer> restored_shape_ack;
    harness.collect_client_acks(restored_shape_ack);
    REQUIRE(restored_shape_ack.size() == 1U);

    (void)harness.forward_ack(delayed_full_ack.front());
    REQUIRE(harness.forward_ack(restored_shape_ack.front()));
    harness.set_position(entity, 2.0f);
    harness.tick_server();
    const ashiato::BitBuffer resumed_delta_packet = harness.take_single_packet();
    const ServerUpdatePacket resumed_delta = harness.read_update(resumed_delta_packet);
    REQUIRE_FALSE(resumed_delta.entities.front().full);
    REQUIRE(resumed_delta.entities.front().baseline_frame == restored_shape.frame);
    REQUIRE(harness.receive(resumed_delta_packet));
    harness.forward_all_client_acks();
    require_position(harness.client_position(entity), 2.0f);
}

// A starved client (a joiner's process held off the CPU, measured in Open Range's net_trail pairs: 234 to 289 frames behind)
// applies an update the server sent more than one packet-id cycle ago and acknowledges it. The 8-bit id in that ACK names a newer
// packet too; crediting the newer one makes the server delta against a frame the client never applied, and every later update is
// rejected with delta_missing_baseline. Without the fix: the held-back ids repeat and every update after the late ACK is rejected.
TEST_CASE("a late ACK for a wrapped packet id is never credited to the newer packet", "[baseline-recovery][packet-id-wrap]") {
    BaselineRecoveryHarness harness;
    TrackedEntity entity = harness.add_entity();
    harness.establish_initial_baseline(entity);

    constexpr std::size_t held_back_updates = 300;  // more than one cycle of the default 8-bit packet ids (255)
    std::vector<ashiato::BitBuffer> held_back;
    std::vector<std::uint32_t> held_back_ids;
    held_back.reserve(held_back_updates);
    for (std::size_t update = 1; update <= held_back_updates; ++update) {
        harness.set_position(entity, alternating_position(update));
        harness.tick_server();
        ashiato::BitBuffer packet = harness.take_single_packet();
        held_back_ids.push_back(harness.read_update(packet).packet_id);
        held_back.push_back(std::move(packet));
    }

    // No two packets the client might still acknowledge share an id (0 is never acknowledged, so it may repeat).
    std::set<std::uint32_t> outstanding_ids;
    std::size_t repeated_ids = 0;
    for (const std::uint32_t id : held_back_ids) {
        if (id != 0U && !outstanding_ids.insert(id).second) {
            ++repeated_ids;
        }
    }
    CAPTURE(repeated_ids);
    CHECK(repeated_ids == 0U);

    // The starved client gets the oldest held-back update only (the rest were dropped) and acknowledges it late.
    REQUIRE(harness.receive(held_back.front()));
    std::vector<ashiato::BitBuffer> late_ack;
    harness.collect_client_acks(late_ack);
    REQUIRE(late_ack.size() == 1U);
    (void)harness.forward_ack(late_ack.front());

    std::size_t rejected_updates = 0;
    constexpr std::size_t resumed_updates = 8;
    for (std::size_t update = 1; update <= resumed_updates; ++update) {
        harness.set_position(entity, 10.0f + static_cast<float>(update));
        harness.tick_server();
        if (!harness.receive(harness.take_single_packet())) {
            ++rejected_updates;
        }
        harness.forward_all_client_acks_unchecked();
    }
    CAPTURE(rejected_updates);
    CHECK(rejected_updates == 0U);
    require_position(harness.client_position(entity), 10.0f + static_cast<float>(resumed_updates));
}

// Whatever credits a frame the client never applied (a misattributed ACK, a baseline the client has since evicted), the client can
// no longer decode that entity's deltas and never acknowledges it again. The server must notice the missing ACK progress, resend the
// entity in full and resume, at a bounded cost. Without the fix the stream stays frozen for good.
TEST_CASE("an entity whose deltas the client cannot decode is resent in full and resumes", "[baseline-recovery][packet-id-wrap]") {
    BaselineRecoveryHarness harness;
    TrackedEntity entity = harness.add_entity();
    harness.establish_initial_baseline(entity);

    // The update at this frame is lost, yet the server is told the client applied it.
    harness.set_position(entity, 1.0f);
    harness.tick_server();
    const ashiato::sync::SyncFrame lost_frame = harness.read_update(harness.take_single_packet()).frame;
    REQUIRE(harness.server().acknowledge_entity(test_client_id, entity.server, lost_frame));

    constexpr std::size_t observed_ticks = 240;  // past the recovery bound (180 frames, 3 s at 60 Hz)
    constexpr std::size_t settled_ticks = 16;
    std::size_t rejected_updates = 0;
    std::size_t full_records = 0;
    std::size_t rejected_after_settling = 0;
    // A rising ramp, one 0.1 quantum a tick: the test position's delta is an unsigned 8-bit difference, so it only encodes rises.
    for (std::size_t tick = 1; tick <= observed_ticks; ++tick) {
        harness.set_position(entity, (static_cast<float>(tick) + 0.5f) / 10.0f);
        harness.tick_server();
        const ashiato::BitBuffer packet = harness.take_single_packet();
        const ServerUpdatePacket decoded = harness.read_update(packet);
        REQUIRE(decoded.entities.size() == 1U);
        if (decoded.entities.front().full) {
            ++full_records;
        }
        if (!harness.receive(packet)) {
            ++rejected_updates;
            if (tick > observed_ticks - settled_ticks) {
                ++rejected_after_settling;
            }
        }
        harness.forward_all_client_acks_unchecked();
    }

    CAPTURE(rejected_updates, full_records, rejected_after_settling);
    CHECK(rejected_updates > 0U);           // the stall really happened
    CHECK(rejected_after_settling == 0U);   // and the stream resumed
    CHECK(full_records >= 1U);
    CHECK(full_records <= 2U);              // the cost: one full resend (and at most one more while its ACK is in flight)
    require_position(harness.client_position(entity), static_cast<float>(observed_ticks) / 10.0f);
}
