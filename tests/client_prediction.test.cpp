#include "test_protocol.hpp"

#include <catch2/catch_approx.hpp>
#include <catch2/catch_test_macros.hpp>

#include <algorithm>
#include <cstdint>
#include <stdexcept>
#include <utility>
#include <vector>

using namespace ashiato_sync_tests;

namespace {

ashiato::BitBuffer make_predicted_position_packet_for_archetype(
    ashiato::sync::SyncFrame frame,
    ashiato::Entity server_entity,
    ashiato::sync::SyncArchetypeId archetype,
    PredictedPosition position,
    std::uint32_t packet_id = 0U) {
    ashiato::BitBuffer packet;
    packet.write_bits(ashiato::sync::protocol::server_update_message, ashiato::sync::protocol::message_bits);
    packet.write_bits(frame, 32U);
    packet.write_bits(packet_id == 0U ? frame : packet_id, ashiato::sync::protocol::server_packet_id_bits);
    packet.write_bits(0, 32U);
    packet.write_bits(1, 16U);
    packet.write_bool(false);
    ashiato::sync::protocol::write_network_entity_id(packet, test_network_id(server_entity));
    packet.write_bool(true);
    packet.write_bits(archetype.value, 32U);
    packet.write_bool(false);
    packet.write_bits(1, 16U);
    packet.write_bits(1, ashiato::sync::protocol::bits_for_range(2U));
    packet.write_bits(static_cast<std::int32_t>(position.x * 10.0f), 16U);
    packet.write_bits(static_cast<std::int32_t>(position.y * 10.0f), 16U);
    packet.write_bool(false);
    return packet;
}

}  // namespace

TEST_CASE("predicted client mode requires ShouldRollBack traits") {
    ashiato::Registry server_registry;
    const ashiato::sync::SyncArchetypeId server_archetype = ashiato_sync_tests::define_position_archetype(server_registry);
    const ashiato::Entity server_entity = server_registry.create();

    ashiato::Registry client_registry;
    const ashiato::sync::SyncArchetypeId client_archetype = ashiato_sync_tests::define_position_archetype(client_registry);
    REQUIRE(client_archetype == server_archetype);
    ashiato_sync_tests::configure_test_client_registry(client_registry, 1);

    ashiato::sync::ReplicationClientOptions options;
    options.entities.default_mode = ashiato::sync::ReplicationClientMode::Predict;
    ashiato::sync::ReplicationClient client(client_registry, ashiato_sync_tests::make_test_client_options(client_registry, options));

    try {
        (void)client.receive(client_registry, make_position_packet(1, {{server_entity, Position{1.0f, 2.0f}}}));
        FAIL("expected missing prediction rollback trait error");
    } catch (const ashiato::sync::ClientError& error) {
        REQUIRE(error.status() == ashiato::sync::ClientStatus::MissingPredictionRollbackTrait);
    }
}

TEST_CASE("predicted client snaps first frame predicts locally and skips matching rollback") {
    ashiato::Registry registry;
    const ashiato::Entity position_component =
        ashiato::sync::register_sync_component<PredictedPosition>(registry, "PredictedPosition");
    const ashiato::sync::SyncArchetypeId archetype = ashiato::sync::define_archetype(
        registry,
        "PredictedActor",
        {{position_component, ashiato::sync::ReplicationAudience::All}});
    REQUIRE(archetype.value == 0);
    ashiato_sync_tests::configure_test_client_registry(registry, 1);
    ashiato::sync::ReplicationClientOptions options;
    options.entities.default_mode = ashiato::sync::ReplicationClientMode::Predict;
    ashiato::sync::ReplicationClient client(registry, ashiato_sync_tests::make_test_client_options(registry, options));
    client.simulation_job<PredictedPosition>(registry, 0).each([](ashiato::Entity, PredictedPosition& position) {
        position.x += 1.0f;
    });

    const ashiato::Entity server_entity = registry.create();
    REQUIRE(client.receive(registry, make_predicted_position_packet(1, server_entity, PredictedPosition{0.0f, 0.0f})));
    const ashiato::Entity local = client.local_entity(test_client_entity_network_id(1, server_entity));
    REQUIRE(local);
    REQUIRE(registry.get<PredictedPosition>(local).x == 0.0f);

    REQUIRE(client.tick(registry, client.fixed_dt_seconds()));
    REQUIRE(registry.get<PredictedPosition>(local).x == 1.0f);
    REQUIRE(client.receive(registry, make_predicted_position_packet(2, server_entity, PredictedPosition{1.0f, 0.0f})));
    REQUIRE(client.tick(registry, client.fixed_dt_seconds()));
    REQUIRE(registry.get<PredictedPosition>(local).x == 2.0f);
}

TEST_CASE("predicted client jobs use quantized input samples") {
    ashiato::Registry registry;
    const ashiato::Entity position_component =
        ashiato::sync::register_sync_component<PredictedPosition>(registry, "PredictedPosition");
    ashiato::sync::register_sync_component<NetworkedPosition>(registry, "NetworkedPosition");
    const ashiato::sync::SyncArchetypeId archetype = ashiato::sync::define_archetype(
        registry,
        "PredictedActor",
        {{position_component, ashiato::sync::ReplicationAudience::All}});
    REQUIRE(archetype.value == 0);
    ashiato_sync_tests::configure_test_client_registry(registry, 1);
    REQUIRE(ashiato::sync::set_client_input_component<NetworkedPosition>(registry));

    ashiato::sync::ReplicationClientOptions options;
    options.entities.default_mode = ashiato::sync::ReplicationClientMode::Predict;
    ashiato::sync::ReplicationClient client(registry, ashiato_sync_tests::make_test_client_options(registry, options));
    client.simulation_job<PredictedPosition, const NetworkedPosition>(registry, 0).each(
        [](ashiato::Entity, PredictedPosition& position, const NetworkedPosition& input) {
            position.x += input.x;
            position.y += input.y;
        });

    const ashiato::Entity server_entity = registry.create();
    REQUIRE(client.receive(registry, make_predicted_position_packet(1, server_entity, PredictedPosition{0.0f, 0.0f})));
    const ashiato::Entity local = client.local_entity(test_client_entity_network_id(1, server_entity));
    REQUIRE(local);
    REQUIRE(ashiato::sync::set_owner(registry, local, 1));

    REQUIRE(client.set_input(registry, NetworkedPosition{1.29f, 2.39f}));
    REQUIRE(client.tick(registry, client.fixed_dt_seconds()));

    REQUIRE(registry.get<PredictedPosition>(local).x == Catch::Approx(1.2f));
    REQUIRE(registry.get<PredictedPosition>(local).y == Catch::Approx(2.3f));
}

TEST_CASE("predicted client rolls back and resimulates mismatched frames") {
    ashiato::Registry registry;
    const ashiato::Entity position_component =
        ashiato::sync::register_sync_component<PredictedPosition>(registry, "PredictedPosition");
    const ashiato::sync::SyncArchetypeId archetype = ashiato::sync::define_archetype(
        registry,
        "PredictedActor",
        {{position_component, ashiato::sync::ReplicationAudience::All}});
    REQUIRE(archetype.value == 0);
    ashiato_sync_tests::configure_test_client_registry(registry, 1);
    ashiato::sync::ReplicationClientOptions options;
    options.entities.default_mode = ashiato::sync::ReplicationClientMode::Predict;
    options.prediction.rollback_policy = ashiato::sync::ReplicationRollbackPolicy::All;
    ashiato::sync::ReplicationClient client(registry, ashiato_sync_tests::make_test_client_options(registry, options));
    client.simulation_job<PredictedPosition>(registry, 0).each([](ashiato::Entity, PredictedPosition& position) {
        position.x += 1.0f;
    });

    const ashiato::Entity server_entity = registry.create();
    REQUIRE(client.receive(registry, make_predicted_position_packet(1, server_entity, PredictedPosition{0.0f, 0.0f})));
    const ashiato::Entity local = client.local_entity(test_client_entity_network_id(1, server_entity));
    REQUIRE(client.tick(registry, client.fixed_dt_seconds()));
    REQUIRE(registry.get<PredictedPosition>(local).x == 1.0f);

    REQUIRE(client.receive(registry, make_predicted_position_packet(2, server_entity, PredictedPosition{2.0f, 0.0f})));
    REQUIRE(registry.get<PredictedPosition>(local).x == 1.0f);
    REQUIRE(client.tick(registry, client.fixed_dt_seconds()));
    REQUIRE(registry.get<PredictedPosition>(local).x == 3.0f);
}

TEST_CASE("predicted client notifies after rollback state is restored before resimulation") {
    ashiato::Registry registry;
    const ashiato::Entity position_component =
        ashiato::sync::register_sync_component<PredictedPosition>(registry, "PredictedPosition");
    const ashiato::sync::SyncArchetypeId archetype = ashiato::sync::define_archetype(
        registry,
        "PredictedActor",
        {{position_component, ashiato::sync::ReplicationAudience::All}});
    REQUIRE(archetype.value == 0);
    ashiato_sync_tests::configure_test_client_registry(registry, 1);

    int rollback_prepared_count = 0;
    ashiato::Entity rollback_entity;
    ashiato::sync::ReplicationClientOptions options;
    options.entities.default_mode = ashiato::sync::ReplicationClientMode::Predict;
    options.prediction.rollback_policy = ashiato::sync::ReplicationRollbackPolicy::All;
    options.rollback_prepared_handler =
        [&](ashiato::Registry& callback_registry,
            const ashiato::sync::ReplicationClientRollbackPreparedEvent& event) {
            ++rollback_prepared_count;
            REQUIRE(event.rollback_frame == 2U);
            REQUIRE(event.resim_end_frame == 2U);
            REQUIRE(event.rollback_policy == ashiato::sync::ReplicationRollbackPolicy::All);
            REQUIRE(event.resimulated_entities.size() == 1U);

            rollback_entity = event.resimulated_entities[0];
            REQUIRE(callback_registry.alive(rollback_entity));
            REQUIRE(callback_registry.get<PredictedPosition>(rollback_entity).x == 2.0f);
        };

    ashiato::sync::ReplicationClient client(registry, ashiato_sync_tests::make_test_client_options(registry, options));
    client.simulation_job<PredictedPosition>(registry, 0).each([](ashiato::Entity, PredictedPosition& position) {
        position.x += 1.0f;
    });

    const ashiato::Entity server_entity = registry.create();
    REQUIRE(client.receive(registry, make_predicted_position_packet(1, server_entity, PredictedPosition{0.0f, 0.0f})));
    const ashiato::Entity local = client.local_entity(test_client_entity_network_id(1, server_entity));
    REQUIRE(local);
    REQUIRE(client.tick(registry, client.fixed_dt_seconds()));
    REQUIRE(registry.get<PredictedPosition>(local).x == 1.0f);

    REQUIRE(client.receive(registry, make_predicted_position_packet(2, server_entity, PredictedPosition{2.0f, 0.0f})));
    REQUIRE(rollback_prepared_count == 0);
    REQUIRE(client.tick(registry, client.fixed_dt_seconds()));

    REQUIRE(rollback_prepared_count == 1);
    REQUIRE(rollback_entity == local);
    REQUIRE(registry.get<PredictedPosition>(local).x == 3.0f);
}

TEST_CASE("predicted client resimulation is not blocked by missing buffered entity frames") {
    ashiato::Registry registry;
    const ashiato::Entity position_component =
        ashiato::sync::register_sync_component<PredictedPosition>(registry, "PredictedPosition");
    const ashiato::sync::SyncArchetypeId archetype = ashiato::sync::define_archetype(
        registry,
        "PredictedActor",
        {{position_component, ashiato::sync::ReplicationAudience::All}});
    REQUIRE(archetype.value == 0);
    ashiato_sync_tests::configure_test_client_registry(registry, 1);

    const ashiato::Entity predicted_server_entity = registry.create();
    const ashiato::Entity buffered_server_entity = registry.create();
    const ashiato::sync::ClientEntityNetworkId predicted_network_id =
        test_client_entity_network_id(1, predicted_server_entity);
    const ashiato::sync::ClientEntityNetworkId buffered_network_id =
        test_client_entity_network_id(1, buffered_server_entity);

    ashiato::sync::ReplicationClientOptions options;
    options.entities.default_mode = ashiato::sync::ReplicationClientMode::Snap;
    options.entities.mode_selector = [&](const ashiato::sync::ReplicatedEntityUpdateView& update) {
        if (update.client_entity_network_id == predicted_network_id) {
            return ashiato::sync::ReplicationClientMode::Predict;
        }
        if (update.client_entity_network_id == buffered_network_id) {
            return ashiato::sync::ReplicationClientMode::BufferedInterpolation;
        }
        return ashiato::sync::ReplicationClientMode::Snap;
    };
    options.prediction.rollback_policy = ashiato::sync::ReplicationRollbackPolicy::All;
    options.buffered.buffered_frame_lag = 1;
    ashiato::sync::ReplicationClient client(registry, ashiato_sync_tests::make_test_client_options(registry, options));
    client.simulation_job<PredictedPosition>(registry, 0).each([](ashiato::Entity, PredictedPosition& position) {
        position.x += 1.0f;
    });

    REQUIRE(client.receive(
        registry,
        make_predicted_position_packet(1, predicted_server_entity, PredictedPosition{0.0f, 0.0f}, 1)));
    REQUIRE(client.receive(
        registry,
        make_predicted_position_packet(1, buffered_server_entity, PredictedPosition{100.0f, 0.0f}, 2)));
    REQUIRE(client.entity_mode(predicted_network_id) == ashiato::sync::ReplicationClientMode::Predict);
    REQUIRE(client.entity_mode(buffered_network_id) == ashiato::sync::ReplicationClientMode::BufferedInterpolation);

    const ashiato::Entity predicted_local = client.local_entity(predicted_network_id);
    REQUIRE(predicted_local);
    REQUIRE(client.apply_frame(registry, 1));
    const ashiato::Entity buffered_local = client.local_entity(buffered_network_id);
    REQUIRE(buffered_local);
    REQUIRE(registry.get<PredictedPosition>(buffered_local).x == 100.0f);
    REQUIRE(registry.add<ashiato::sync::NoSimulate>(buffered_local));

    REQUIRE(client.tick(registry, client.fixed_dt_seconds()));
    REQUIRE(registry.get<PredictedPosition>(predicted_local).x == 1.0f);
    REQUIRE(client.tick(registry, client.fixed_dt_seconds()));
    REQUIRE(registry.get<PredictedPosition>(predicted_local).x == 2.0f);

    REQUIRE(client.receive(
        registry,
        make_predicted_position_packet(2, predicted_server_entity, PredictedPosition{20.0f, 0.0f}, 3)));
    REQUIRE(client.tick(registry, client.fixed_dt_seconds()));
    REQUIRE(registry.get<PredictedPosition>(predicted_local).x == 22.0f);
    REQUIRE(registry.get<PredictedPosition>(buffered_local).x == 100.0f);
}

TEST_CASE("predicted client resimulation applies available buffered frames when another buffered entity is missing") {
    ashiato::Registry registry;
    const ashiato::Entity position_component =
        ashiato::sync::register_sync_component<PredictedPosition>(registry, "PredictedPosition");
    const ashiato::sync::SyncArchetypeId archetype = ashiato::sync::define_archetype(
        registry,
        "PredictedActor",
        {{position_component, ashiato::sync::ReplicationAudience::All}});
    REQUIRE(archetype.value == 0);
    ashiato_sync_tests::configure_test_client_registry(registry, 1);

    const ashiato::Entity predicted_server_entity = registry.create();
    const ashiato::Entity missing_buffered_server_entity = registry.create();
    const ashiato::Entity available_buffered_server_entity = registry.create();
    const ashiato::sync::ClientEntityNetworkId predicted_network_id =
        test_client_entity_network_id(1, predicted_server_entity);
    const ashiato::sync::ClientEntityNetworkId missing_buffered_network_id =
        test_client_entity_network_id(1, missing_buffered_server_entity);
    const ashiato::sync::ClientEntityNetworkId available_buffered_network_id =
        test_client_entity_network_id(1, available_buffered_server_entity);

    ashiato::sync::ReplicationClientOptions options;
    options.entities.default_mode = ashiato::sync::ReplicationClientMode::Snap;
    options.entities.mode_selector = [&](const ashiato::sync::ReplicatedEntityUpdateView& update) {
        if (update.client_entity_network_id == predicted_network_id) {
            return ashiato::sync::ReplicationClientMode::Predict;
        }
        if (update.client_entity_network_id == missing_buffered_network_id ||
            update.client_entity_network_id == available_buffered_network_id) {
            return ashiato::sync::ReplicationClientMode::BufferedInterpolation;
        }
        return ashiato::sync::ReplicationClientMode::Snap;
    };
    options.prediction.rollback_policy = ashiato::sync::ReplicationRollbackPolicy::All;
    options.buffered.buffered_frame_lag = 1;
    ashiato::sync::ReplicationClient client(registry, ashiato_sync_tests::make_test_client_options(registry, options));
    client.simulation_job<PredictedPosition>(registry, 0).each([](ashiato::Entity, PredictedPosition& position) {
        position.x += 1.0f;
    });

    REQUIRE(client.receive(
        registry,
        make_predicted_position_packet(1, predicted_server_entity, PredictedPosition{0.0f, 0.0f}, 1)));
    REQUIRE(client.receive(
        registry,
        make_predicted_position_packet(1, missing_buffered_server_entity, PredictedPosition{100.0f, 0.0f}, 2)));
    REQUIRE(client.receive(
        registry,
        make_predicted_position_packet(1, available_buffered_server_entity, PredictedPosition{200.0f, 0.0f}, 3)));

    const ashiato::Entity predicted_local = client.local_entity(predicted_network_id);
    REQUIRE(predicted_local);
    REQUIRE(client.apply_frame(registry, 1));
    const ashiato::Entity missing_buffered_local = client.local_entity(missing_buffered_network_id);
    const ashiato::Entity available_buffered_local = client.local_entity(available_buffered_network_id);
    REQUIRE(missing_buffered_local);
    REQUIRE(available_buffered_local);
    REQUIRE(registry.add<ashiato::sync::NoSimulate>(missing_buffered_local));
    REQUIRE(registry.add<ashiato::sync::NoSimulate>(available_buffered_local));

    REQUIRE(client.receive(
        registry,
        make_predicted_position_packet(2, available_buffered_server_entity, PredictedPosition{300.0f, 0.0f}, 4)));
    REQUIRE(client.receive(
        registry,
        make_predicted_position_packet(3, available_buffered_server_entity, PredictedPosition{300.0f, 0.0f}, 5)));
    registry.write<PredictedPosition>(available_buffered_local).x = 999.0f;

    REQUIRE(client.tick(registry, client.fixed_dt_seconds()));
    REQUIRE(client.tick(registry, client.fixed_dt_seconds()));
    REQUIRE(registry.get<PredictedPosition>(predicted_local).x == 2.0f);

    REQUIRE(client.receive(
        registry,
        make_predicted_position_packet(2, predicted_server_entity, PredictedPosition{20.0f, 0.0f}, 6)));
    REQUIRE(client.tick(registry, client.fixed_dt_seconds()));
    REQUIRE(registry.get<PredictedPosition>(predicted_local).x == 22.0f);
    REQUIRE(registry.get<PredictedPosition>(missing_buffered_local).x == 100.0f);
    REQUIRE(registry.get<PredictedPosition>(available_buffered_local).x == 200.0f);
}

TEST_CASE("predicted client resimulation applies snap frames and delayed buffered frames by entity type") {
    constexpr ashiato::sync::SyncFrame rollback_frame = 2;
    constexpr ashiato::sync::SyncFrame observed_frame = 5;

    ashiato::Registry registry;
    const ashiato::Entity position_component =
        ashiato::sync::register_sync_component<PredictedPosition>(registry, "PredictedPosition");
    const ashiato::sync::SyncArchetypeId step_archetype = ashiato::sync::define_archetype(
        registry,
        "StepActor",
        {{position_component, ashiato::sync::ReplicationAudience::All, ashiato::sync::ComponentInterpolation::Step}});
    const ashiato::sync::SyncArchetypeId interpolated_archetype = ashiato::sync::define_archetype(
        registry,
        "InterpolatedActor",
        {{position_component, ashiato::sync::ReplicationAudience::All, ashiato::sync::ComponentInterpolation::Interpolate}});
    REQUIRE(step_archetype.value == 0);
    REQUIRE(interpolated_archetype.value == 1);
    ashiato_sync_tests::configure_test_client_registry(registry, 1);

    const ashiato::Entity predicted_server_entity = registry.create();
    const ashiato::Entity snap_server_entity = registry.create();
    const ashiato::Entity step_buffered_server_entity = registry.create();
    const ashiato::Entity interpolated_buffered_server_entity = registry.create();
    const ashiato::Entity other_predicted_server_entity = registry.create();
    const ashiato::sync::ClientEntityNetworkId predicted_network_id =
        test_client_entity_network_id(1, predicted_server_entity);
    const ashiato::sync::ClientEntityNetworkId snap_network_id =
        test_client_entity_network_id(1, snap_server_entity);
    const ashiato::sync::ClientEntityNetworkId step_buffered_network_id =
        test_client_entity_network_id(1, step_buffered_server_entity);
    const ashiato::sync::ClientEntityNetworkId interpolated_buffered_network_id =
        test_client_entity_network_id(1, interpolated_buffered_server_entity);
    const ashiato::sync::ClientEntityNetworkId other_predicted_network_id =
        test_client_entity_network_id(1, other_predicted_server_entity);

    struct ResimObservedValues {
        bool original_seen = false;
        bool resim_seen = false;
        float snap = 0.0f;
        float step_buffered = 0.0f;
        float interpolated_buffered = 0.0f;
        float other_predicted = 0.0f;
    };
    ResimObservedValues observed;
    ashiato::Entity predicted_local;
    ashiato::Entity snap_local;
    ashiato::Entity step_buffered_local;
    ashiato::Entity interpolated_buffered_local;
    ashiato::Entity other_predicted_local;

    ashiato::sync::ReplicationClientOptions options;
    options.entities.default_mode = ashiato::sync::ReplicationClientMode::Snap;
    options.entities.mode_selector = [&](const ashiato::sync::ReplicatedEntityUpdateView& update) {
        if (update.client_entity_network_id == predicted_network_id ||
            update.client_entity_network_id == other_predicted_network_id) {
            return ashiato::sync::ReplicationClientMode::Predict;
        }
        if (update.client_entity_network_id == step_buffered_network_id ||
            update.client_entity_network_id == interpolated_buffered_network_id) {
            return ashiato::sync::ReplicationClientMode::BufferedInterpolation;
        }
        return ashiato::sync::ReplicationClientMode::Snap;
    };
    options.prediction.rollback_policy = ashiato::sync::ReplicationRollbackPolicy::All;
    options.prediction.auto_lead_frames = false;
    options.prediction.lead_frames = 2;
    options.buffered.auto_buffered_frame_lag = false;
    options.buffered.buffered_frame_lag = 2;
    ashiato::sync::ReplicationClient client(registry, ashiato_sync_tests::make_test_client_options(registry, options));
    client.simulation_job<PredictedPosition, const ashiato::sync::FrameInfo>(registry, 0).each(
        [](ashiato::Entity, PredictedPosition& position, const ashiato::sync::FrameInfo&) {
            position.x += 1.0f;
        });
    client.simulation_job<const PredictedPosition, const ashiato::sync::FrameInfo>(registry, 1)
        .access_other_entities<PredictedPosition>()
        .each([&](
            auto& view,
            ashiato::Entity entity,
            const PredictedPosition&,
            const ashiato::sync::FrameInfo& frame) {
            if (frame.frame != observed_frame || entity != predicted_local) {
                return;
            }

            const float snap = view.template get<const PredictedPosition>(snap_local).x;
            const float step_buffered = view.template get<const PredictedPosition>(step_buffered_local).x;
            const float interpolated_buffered =
                view.template get<const PredictedPosition>(interpolated_buffered_local).x;
            const float other_predicted = view.template get<const PredictedPosition>(other_predicted_local).x;
            if (!observed.original_seen) {
                observed = ResimObservedValues{
                    true,
                    false,
                    snap,
                    step_buffered,
                    interpolated_buffered,
                    other_predicted};
                return;
            }

            observed.resim_seen = true;
            REQUIRE(snap == observed.snap);
            REQUIRE(step_buffered == observed.step_buffered);
            REQUIRE(interpolated_buffered == observed.interpolated_buffered);
            REQUIRE(other_predicted == observed.other_predicted);
        });

    REQUIRE(client.receive(
        registry,
        make_predicted_position_packet_for_archetype(
            1,
            predicted_server_entity,
            step_archetype,
            PredictedPosition{0.0f, 0.0f},
            1)));
    REQUIRE(client.receive(
        registry,
        make_predicted_position_packet_for_archetype(
            1,
            other_predicted_server_entity,
            step_archetype,
            PredictedPosition{50.0f, 0.0f},
            2)));
    REQUIRE(client.receive(
        registry,
        make_predicted_position_packet_for_archetype(
            1,
            snap_server_entity,
            step_archetype,
            PredictedPosition{100.0f, 0.0f},
            3)));
    REQUIRE(client.receive(
        registry,
        make_predicted_position_packet_for_archetype(
            1,
            step_buffered_server_entity,
            step_archetype,
            PredictedPosition{200.0f, 0.0f},
            4)));
    REQUIRE(client.receive(
        registry,
        make_predicted_position_packet_for_archetype(
            1,
            interpolated_buffered_server_entity,
            interpolated_archetype,
            PredictedPosition{1000.0f, 0.0f},
            5)));

    predicted_local = client.local_entity(predicted_network_id);
    other_predicted_local = client.local_entity(other_predicted_network_id);
    snap_local = client.local_entity(snap_network_id);
    REQUIRE(predicted_local);
    REQUIRE(other_predicted_local);
    REQUIRE(snap_local);
    REQUIRE(client.apply_frame(registry, 1));
    step_buffered_local = client.local_entity(step_buffered_network_id);
    interpolated_buffered_local = client.local_entity(interpolated_buffered_network_id);
    REQUIRE(step_buffered_local);
    REQUIRE(interpolated_buffered_local);
    REQUIRE(registry.add<ashiato::sync::NoSimulate>(snap_local));
    REQUIRE(registry.add<ashiato::sync::NoSimulate>(step_buffered_local));
    REQUIRE(registry.add<ashiato::sync::NoSimulate>(interpolated_buffered_local));

    REQUIRE(client.receive(
        registry,
        make_predicted_position_packet_for_archetype(
            observed_frame,
            snap_server_entity,
            step_archetype,
            PredictedPosition{500.0f, 0.0f},
            6)));
    REQUIRE(client.receive(
        registry,
        make_predicted_position_packet_for_archetype(
            2,
            step_buffered_server_entity,
            step_archetype,
            PredictedPosition{220.0f, 0.0f},
            7)));
    REQUIRE(client.receive(
        registry,
        make_predicted_position_packet_for_archetype(
            3,
            interpolated_buffered_server_entity,
            interpolated_archetype,
            PredictedPosition{1200.0f, 0.0f},
            8)));

    while (client.predicted_frame() < observed_frame) {
        REQUIRE(client.tick(registry, client.fixed_dt_seconds()));
    }
    REQUIRE(client.predicted_frame() == observed_frame);
    REQUIRE(observed.original_seen);
    REQUIRE_FALSE(observed.resim_seen);

    registry.write<PredictedPosition>(snap_local).x = 999.0f;
    registry.write<PredictedPosition>(step_buffered_local).x = 999.0f;
    registry.write<PredictedPosition>(interpolated_buffered_local).x = 999.0f;
    registry.write<PredictedPosition>(other_predicted_local).x = 999.0f;

    REQUIRE(client.receive(
        registry,
        make_predicted_position_packet_for_archetype(
            rollback_frame,
            predicted_server_entity,
            step_archetype,
            PredictedPosition{20.0f, 0.0f},
            9)));
    REQUIRE(client.tick(registry, client.fixed_dt_seconds()));

    REQUIRE(observed.resim_seen);
}

TEST_CASE("predicted client resimulation does not reuse stale snap frames") {
    ashiato::Registry registry;
    const ashiato::Entity position_component =
        ashiato::sync::register_sync_component<PredictedPosition>(registry, "PredictedPosition");
    const ashiato::sync::SyncArchetypeId archetype = ashiato::sync::define_archetype(
        registry,
        "PredictedActor",
        {{position_component, ashiato::sync::ReplicationAudience::All}});
    REQUIRE(archetype.value == 0);
    ashiato_sync_tests::configure_test_client_registry(registry, 1);

    const ashiato::Entity predicted_server_entity = registry.create();
    const ashiato::Entity snap_server_entity = registry.create();
    const ashiato::Entity buffered_server_entity = registry.create();
    const ashiato::sync::ClientEntityNetworkId predicted_network_id =
        test_client_entity_network_id(1, predicted_server_entity);
    const ashiato::sync::ClientEntityNetworkId snap_network_id =
        test_client_entity_network_id(1, snap_server_entity);
    const ashiato::sync::ClientEntityNetworkId buffered_network_id =
        test_client_entity_network_id(1, buffered_server_entity);

    ashiato::sync::ReplicationClientOptions options;
    options.entities.default_mode = ashiato::sync::ReplicationClientMode::Snap;
    options.entities.mode_selector = [&](const ashiato::sync::ReplicatedEntityUpdateView& update) {
        if (update.client_entity_network_id == predicted_network_id) {
            return ashiato::sync::ReplicationClientMode::Predict;
        }
        if (update.client_entity_network_id == buffered_network_id) {
            return ashiato::sync::ReplicationClientMode::BufferedInterpolation;
        }
        return ashiato::sync::ReplicationClientMode::Snap;
    };
    options.prediction.rollback_policy = ashiato::sync::ReplicationRollbackPolicy::All;
    options.prediction.auto_lead_frames = false;
    options.prediction.lead_frames = 1;
    options.buffered.auto_buffered_frame_lag = false;
    options.buffered.buffered_frame_lag = 1;
    ashiato::sync::ReplicationClient client(registry, ashiato_sync_tests::make_test_client_options(registry, options));
    client.simulation_job<PredictedPosition>(registry, 0).each([](ashiato::Entity, PredictedPosition& position) {
        position.x += 1.0f;
    });

    REQUIRE(client.receive(
        registry,
        make_predicted_position_packet(1, predicted_server_entity, PredictedPosition{0.0f, 0.0f}, 1)));
    REQUIRE(client.receive(
        registry,
        make_predicted_position_packet(1, snap_server_entity, PredictedPosition{100.0f, 0.0f}, 2)));
    REQUIRE(client.receive(
        registry,
        make_predicted_position_packet(1, buffered_server_entity, PredictedPosition{200.0f, 0.0f}, 3)));

    const ashiato::Entity predicted_local = client.local_entity(predicted_network_id);
    const ashiato::Entity snap_local = client.local_entity(snap_network_id);
    REQUIRE(predicted_local);
    REQUIRE(snap_local);
    REQUIRE(client.apply_frame(registry, 1));
    const ashiato::Entity buffered_local = client.local_entity(buffered_network_id);
    REQUIRE(buffered_local);
    REQUIRE(registry.add<ashiato::sync::NoSimulate>(snap_local));
    REQUIRE(registry.add<ashiato::sync::NoSimulate>(buffered_local));

    while (client.predicted_frame() < 4U) {
        REQUIRE(client.tick(registry, client.fixed_dt_seconds()));
    }
    REQUIRE(client.predicted_frame() == 4U);
    REQUIRE(client.buffered_frame() == 2U);

    registry.write<PredictedPosition>(snap_local).x = 999.0f;
    registry.write<PredictedPosition>(buffered_local).x = 888.0f;

    REQUIRE(client.receive(
        registry,
        make_predicted_position_packet(2, predicted_server_entity, PredictedPosition{10.0f, 0.0f}, 4)));
    REQUIRE(client.tick(registry, client.fixed_dt_seconds()));

    REQUIRE(registry.get<PredictedPosition>(snap_local).x == 999.0f);
    REQUIRE(registry.get<PredictedPosition>(buffered_local).x == 200.0f);
}

#ifdef ASHIATO_SYNC_ENABLE_TRACING

TEST_CASE("predicted client starts bundled authoritative rollback from latest received baseline") {
    ashiato::Registry registry;
    const ashiato::Entity position_component =
        ashiato::sync::register_sync_component<PredictedPosition>(registry, "PredictedPosition");
    const ashiato::sync::SyncArchetypeId archetype = ashiato::sync::define_archetype(
        registry,
        "PredictedActor",
        {{position_component, ashiato::sync::ReplicationAudience::All}});
    REQUIRE(archetype.value == 0);
    ashiato_sync_tests::configure_test_client_registry(registry, 1);

    ashiato::sync::ReplicationClientOptions options;
    options.entities.default_mode = ashiato::sync::ReplicationClientMode::Predict;
    options.prediction.rollback_policy = ashiato::sync::ReplicationRollbackPolicy::All;
    options.prediction.auto_lead_frames = false;
    options.prediction.lead_frames = 0;
    ashiato::sync::ReplicationClient client(registry, ashiato_sync_tests::make_test_client_options(registry, options));
    client.simulation_job<PredictedPosition>(registry, 0).each([](ashiato::Entity, PredictedPosition& position) {
        position.x += 1.0f;
    });

    std::vector<ashiato::sync::SyncTraceEvent> events;
    ashiato::sync::SyncTracer tracer;
    tracer.set_frame_data_enabled(true);
    tracer.set_callbacks(ashiato::sync::SyncTraceCallbacks{
        [&](const ashiato::sync::SyncTraceEvent& event) { events.push_back(event); }});
    client.set_tracer(&tracer);

    const ashiato::Entity server_entity = registry.create();
    REQUIRE(client.receive(registry, make_predicted_position_packet(1, server_entity, PredictedPosition{0.0f, 0.0f})));
    const ashiato::Entity local = client.local_entity(test_client_entity_network_id(1, server_entity));
    REQUIRE(local);

    tick_client_fixed_frames(client, registry, 3);
    REQUIRE(registry.get<PredictedPosition>(local).x == 3.0f);

    REQUIRE(client.receive(registry, make_predicted_position_packet(2, server_entity, PredictedPosition{2.0f, 0.0f}, 2)));
    REQUIRE(client.receive(registry, make_predicted_position_packet(3, server_entity, PredictedPosition{3.0f, 0.0f}, 3)));
    tick_client_fixed_frames(client, registry, 1);

    std::vector<ashiato::sync::SyncFrame> resim_frames;
    for (const ashiato::sync::SyncTraceEvent& event : events) {
        if (event.type == ashiato::sync::SyncTraceEventType::ResimulatedFrameComponent &&
            event.component == position_component) {
            resim_frames.push_back(event.frame);
        }
    }

    REQUIRE(std::find(resim_frames.begin(), resim_frames.end(), 3) == resim_frames.end());
    REQUIRE(std::count(resim_frames.begin(), resim_frames.end(), 4) == 1);
    REQUIRE(registry.get<PredictedPosition>(local).x == 5.0f);
}

#endif

TEST_CASE("predicted client rolls back locally predicted cues missing from authoritative frame") {
    ashiato::Registry registry;
    const ashiato::Entity position_component =
        ashiato::sync::register_sync_component<PredictedPosition>(registry, "PredictedPosition");
    const ashiato::sync::SyncArchetypeId archetype = ashiato::sync::define_archetype(
        registry,
        "PredictedActor",
        {{position_component, ashiato::sync::ReplicationAudience::All}});
    REQUIRE(archetype.value == 0);
    registry.register_component<CuePlayback>("CuePlayback");
    ashiato::sync::register_sync_cue<TestCue>(registry);
    ashiato_sync_tests::configure_test_client_registry(registry, 1);

    bool emit_prediction_cue = true;
    ashiato::sync::ReplicationClientOptions options;
    options.entities.default_mode = ashiato::sync::ReplicationClientMode::Predict;
    options.prediction.rollback_policy = ashiato::sync::ReplicationRollbackPolicy::All;
    ashiato::sync::ReplicationClient client(registry, ashiato_sync_tests::make_test_client_options(registry, options));
    client.simulation_job<
        PredictedPosition,
        ashiato::sync::SyncSettings,
        ashiato::sync::FrameInfo,
        ashiato::sync::CueDispatcher>(registry, 0).each(
        [&](ashiato::Entity entity,
            PredictedPosition& position,
            ashiato::sync::SyncSettings& settings,
            ashiato::sync::FrameInfo& frame,
            ashiato::sync::CueDispatcher& cues) {
            position.x += 1.0f;
            if (emit_prediction_cue) {
                REQUIRE(cues.emit(settings, frame, entity, TestCue{7}, 1.0f));
                emit_prediction_cue = false;
            }
        });

    const ashiato::Entity server_entity = registry.create();
    REQUIRE(client.receive(registry, make_predicted_position_packet(1, server_entity, PredictedPosition{0.0f, 0.0f})));
    const ashiato::Entity local = client.local_entity(test_client_entity_network_id(1, server_entity));
    REQUIRE(client.tick(registry, client.fixed_dt_seconds()));
    REQUIRE(registry.get<CuePlayback>(local).plays == 1);
    REQUIRE(registry.get<CuePlayback>(local).rollbacks == 0);

    REQUIRE(client.receive(registry, make_predicted_position_packet(2, server_entity, PredictedPosition{2.0f, 0.0f})));
    REQUIRE(registry.get<CuePlayback>(local).rollbacks == 1);
}

TEST_CASE("predicted client keeps locally predicted cues replayed during resimulation") {
    ashiato::Registry registry;
    const ashiato::Entity position_component =
        ashiato::sync::register_sync_component<PredictedPosition>(registry, "PredictedPosition");
    const ashiato::sync::SyncArchetypeId archetype = ashiato::sync::define_archetype(
        registry,
        "PredictedActor",
        {{position_component, ashiato::sync::ReplicationAudience::All}});
    REQUIRE(archetype.value == 0);
    registry.register_component<CuePlayback>("CuePlayback");
    ashiato::sync::register_sync_cue<TestCue>(registry);
    ashiato_sync_tests::configure_test_client_registry(registry, 1);

    ashiato::sync::ReplicationClientOptions options;
    options.entities.default_mode = ashiato::sync::ReplicationClientMode::Predict;
    options.prediction.rollback_policy = ashiato::sync::ReplicationRollbackPolicy::All;
    ashiato::sync::ReplicationClient client(registry, ashiato_sync_tests::make_test_client_options(registry, options));
    int prediction_jobs = 0;
    client.simulation_job<
        PredictedPosition,
        ashiato::sync::SyncSettings,
        ashiato::sync::FrameInfo,
        ashiato::sync::CueDispatcher>(registry, 0).each(
        [&](ashiato::Entity entity,
            PredictedPosition& position,
            ashiato::sync::SyncSettings& settings,
            ashiato::sync::FrameInfo& frame,
            ashiato::sync::CueDispatcher& cues) {
            position.x += 1.0f;
            ++prediction_jobs;
            if (prediction_jobs == 2 || prediction_jobs == 3) {
                REQUIRE(cues.emit(settings, frame, entity, TestCue{5}, 1.0f));
            }
        });

    const ashiato::Entity server_entity = registry.create();
    REQUIRE(client.receive(registry, make_predicted_position_packet(1, server_entity, PredictedPosition{0.0f, 0.0f})));
    const ashiato::Entity local = client.local_entity(test_client_entity_network_id(1, server_entity));
    REQUIRE(client.tick(registry, client.fixed_dt_seconds()));
    REQUIRE_FALSE(registry.contains<CuePlayback>(local));
    REQUIRE(client.tick(registry, client.fixed_dt_seconds()));
    REQUIRE(registry.get<CuePlayback>(local).plays == 1);
    REQUIRE(registry.get<CuePlayback>(local).rollbacks == 0);

    REQUIRE(client.receive(registry, make_predicted_position_packet(2, server_entity, PredictedPosition{2.0f, 0.0f})));
    REQUIRE(registry.get<CuePlayback>(local).rollbacks == 0);
    REQUIRE(client.tick(registry, client.fixed_dt_seconds()));
    REQUIRE(registry.get<CuePlayback>(local).rollbacks == 0);
}

TEST_CASE("predicted client keeps a predicted cue older than the resimulation that follows it") {
    // A cue predicted at frame 2, then a rollback that begins at frame 3: the replay runs frame 4 and never frame 2, so
    // it cannot emit the cue again, and not emitting it is no evidence that it did not happen. Whether it did is the
    // server's frame 2 to say. Found by reading, in the cockpit game (lane/weapons, 2026-09-16), while chasing a shell
    // withdrawn for a different reason (the server acting on its trigger late); see the patch header.
    ashiato::Registry registry;
    const ashiato::Entity position_component =
        ashiato::sync::register_sync_component<PredictedPosition>(registry, "PredictedPosition");
    const ashiato::sync::SyncArchetypeId archetype = ashiato::sync::define_archetype(
        registry,
        "PredictedActor",
        {{position_component, ashiato::sync::ReplicationAudience::All}});
    REQUIRE(archetype.value == 0);
    registry.register_component<CuePlayback>("CuePlayback");
    ashiato::sync::register_sync_cue<TestCue>(registry);
    ashiato_sync_tests::configure_test_client_registry(registry, 1);

    ashiato::sync::ReplicationClientOptions options;
    options.entities.default_mode = ashiato::sync::ReplicationClientMode::Predict;
    options.prediction.rollback_policy = ashiato::sync::ReplicationRollbackPolicy::All;
    ashiato::sync::ReplicationClient client(registry, ashiato_sync_tests::make_test_client_options(registry, options));
    bool emitted = false;
    client.simulation_job<
        PredictedPosition,
        ashiato::sync::SyncSettings,
        ashiato::sync::FrameInfo,
        ashiato::sync::CueDispatcher>(registry, 0).each(
        [&](ashiato::Entity entity,
            PredictedPosition& position,
            ashiato::sync::SyncSettings& settings,
            ashiato::sync::FrameInfo& frame,
            ashiato::sync::CueDispatcher& cues) {
            position.x += 1.0f;
            if (frame.frame == 2U && !emitted) {
                REQUIRE(cues.emit(settings, frame, entity, TestCue{9}, 1.0f));
                emitted = true;
            }
        });

    const ashiato::Entity server_entity = registry.create();
    REQUIRE(client.receive(registry, make_predicted_position_packet(1, server_entity, PredictedPosition{0.0f, 0.0f})));
    const ashiato::Entity local = client.local_entity(test_client_entity_network_id(1, server_entity));
    REQUIRE(client.tick(registry, client.fixed_dt_seconds()));
    REQUIRE(client.tick(registry, client.fixed_dt_seconds()));
    REQUIRE(client.tick(registry, client.fixed_dt_seconds()));
    REQUIRE(registry.get<CuePlayback>(local).plays == 1);
    REQUIRE(registry.get<CuePlayback>(local).last_frame == 2U);

    REQUIRE(client.receive(registry, make_predicted_position_packet(3, server_entity, PredictedPosition{10.0f, 0.0f})));
    REQUIRE(client.tick(registry, client.fixed_dt_seconds()));
    REQUIRE(registry.get<PredictedPosition>(local).x == 12.0f);
    REQUIRE(registry.get<CuePlayback>(local).rollbacks == 0);
    REQUIRE(registry.get<CuePlayback>(local).plays == 1);
}

TEST_CASE("predicted client keeps a predicted cue on an entity the resimulation did not touch") {
    // ONLY_AFFECTED: the second entity is rolled back and replayed, the first is not, so the first's predicted cue is
    // never asked for again -- and must not be withdrawn for not being emitted by a replay that did not run it.
    ashiato::Registry registry;
    const ashiato::Entity position_component =
        ashiato::sync::register_sync_component<PredictedPosition>(registry, "PredictedPosition");
    const ashiato::sync::SyncArchetypeId archetype = ashiato::sync::define_archetype(
        registry,
        "PredictedActor",
        {{position_component, ashiato::sync::ReplicationAudience::All}});
    REQUIRE(archetype.value == 0);
    registry.register_component<CuePlayback>("CuePlayback");
    ashiato::sync::register_sync_cue<TestCue>(registry);
    ashiato_sync_tests::configure_test_client_registry(registry, 1);

    ashiato::sync::ReplicationClientOptions options;
    options.entities.default_mode = ashiato::sync::ReplicationClientMode::Predict;
    options.prediction.rollback_policy = ashiato::sync::ReplicationRollbackPolicy::OnlyAffected;
    ashiato::sync::ReplicationClient client(registry, ashiato_sync_tests::make_test_client_options(registry, options));
    ashiato::Entity cue_owner{};
    bool emitted = false;
    client.simulation_job<
        PredictedPosition,
        ashiato::sync::SyncSettings,
        ashiato::sync::FrameInfo,
        ashiato::sync::CueDispatcher>(registry, 0).each(
        [&](ashiato::Entity entity,
            PredictedPosition& position,
            ashiato::sync::SyncSettings& settings,
            ashiato::sync::FrameInfo& frame,
            ashiato::sync::CueDispatcher& cues) {
            position.x += 1.0f;
            if (entity == cue_owner && frame.frame == 3U && !emitted) {
                REQUIRE(cues.emit(settings, frame, entity, TestCue{4}, 1.0f));
                emitted = true;
            }
        });

    const ashiato::Entity first_server = registry.create();
    const ashiato::Entity second_server = registry.create();
    REQUIRE(client.receive(registry, make_predicted_position_packet(1, first_server, PredictedPosition{0.0f, 0.0f}, 1)));
    REQUIRE(client.receive(registry, make_predicted_position_packet(1, second_server, PredictedPosition{0.0f, 0.0f}, 2)));
    cue_owner = client.local_entity(test_client_entity_network_id(1, first_server));
    REQUIRE(client.tick(registry, client.fixed_dt_seconds()));
    REQUIRE(client.tick(registry, client.fixed_dt_seconds()));
    REQUIRE(client.tick(registry, client.fixed_dt_seconds()));
    REQUIRE(registry.get<CuePlayback>(cue_owner).plays == 1);

    // Frame 2: the first entity as predicted, the second not.
    REQUIRE(client.receive(registry, make_predicted_position_packet(2, first_server, PredictedPosition{1.0f, 0.0f}, 3)));
    REQUIRE(client.receive(registry, make_predicted_position_packet(2, second_server, PredictedPosition{5.0f, 0.0f}, 4)));
    REQUIRE(client.tick(registry, client.fixed_dt_seconds()));
    REQUIRE(registry.get<PredictedPosition>(client.local_entity(test_client_entity_network_id(1, second_server))).x == 8.0f);
    REQUIRE(registry.get<CuePlayback>(cue_owner).rollbacks == 0);
    REQUIRE(registry.get<CuePlayback>(cue_owner).plays == 1);
}

#ifdef ASHIATO_SYNC_ENABLE_TRACING

TEST_CASE("predicted client traces rollback reason separately from rollback conflict") {
    ashiato::Registry registry;
    const ashiato::Entity position_component =
        ashiato::sync::register_sync_component<PredictedPosition>(registry, "PredictedPosition");
    const ashiato::sync::SyncArchetypeId archetype = ashiato::sync::define_archetype(
        registry,
        "PredictedActor",
        {{position_component, ashiato::sync::ReplicationAudience::All}});
    REQUIRE(archetype.value == 0);
    ashiato_sync_tests::configure_test_client_registry(registry, 1);
    ashiato::sync::ReplicationClientOptions options;
    options.entities.default_mode = ashiato::sync::ReplicationClientMode::Predict;
    ashiato::sync::ReplicationClient client(registry, ashiato_sync_tests::make_test_client_options(registry, options));
    client.simulation_job<PredictedPosition>(registry, 0).each([](ashiato::Entity, PredictedPosition& position) {
        position.x += 1.0f;
    });

    std::vector<ashiato::sync::SyncTraceEvent> events;
    ashiato::sync::SyncTracer tracer;
    tracer.set_callbacks(ashiato::sync::SyncTraceCallbacks{
        [&](const ashiato::sync::SyncTraceEvent& event) { events.push_back(event); }});
    client.set_tracer(&tracer);

    const ashiato::Entity server_entity = registry.create();
    REQUIRE(client.receive(registry, make_predicted_position_packet(1, server_entity, PredictedPosition{0.0f, 0.0f})));
    REQUIRE(client.tick(registry, client.fixed_dt_seconds()));
    REQUIRE(client.receive(registry, make_predicted_position_packet(2, server_entity, PredictedPosition{2.0f, 0.0f})));

    const auto conflict = std::find_if(events.begin(), events.end(), [](const ashiato::sync::SyncTraceEvent& event) {
        return event.type == ashiato::sync::SyncTraceEventType::PredictionRollbackConflict;
    });
    REQUIRE(conflict != events.end());
    REQUIRE(conflict->component == position_component);
    REQUIRE(conflict->component_name == "PredictedPosition");
    REQUIRE(conflict->data.empty());

    const auto reason = std::find_if(events.begin(), events.end(), [](const ashiato::sync::SyncTraceEvent& event) {
        return event.type == ashiato::sync::SyncTraceEventType::RollbackReason;
    });
    REQUIRE(reason != events.end());
    REQUIRE(std::count_if(events.begin(), events.end(), [](const ashiato::sync::SyncTraceEvent& event) {
        return event.type == ashiato::sync::SyncTraceEventType::RollbackReason;
    }) == 1);
    REQUIRE(reason->component == position_component);
    REQUIRE(reason->component_name == "PredictedPosition");
    REQUIRE(reason->data == "PredictedPosition.x mismatch");
}

TEST_CASE("predicted cue rollback tracing records server mismatch reason") {
    ashiato::Registry registry;
    const ashiato::Entity position_component =
        ashiato::sync::register_sync_component<PredictedPosition>(registry, "PredictedPosition");
    const ashiato::sync::SyncArchetypeId archetype = ashiato::sync::define_archetype(
        registry,
        "PredictedActor",
        {{position_component, ashiato::sync::ReplicationAudience::All}});
    REQUIRE(archetype.value == 0);
    registry.register_component<CuePlayback>("CuePlayback");
    ashiato::sync::register_sync_cue<TestCue>(registry);
    ashiato_sync_tests::configure_test_client_registry(registry, 1);

    bool emit_prediction_cue = true;
    ashiato::sync::ReplicationClientOptions options;
    options.entities.default_mode = ashiato::sync::ReplicationClientMode::Predict;
    ashiato::sync::ReplicationClient client(registry, ashiato_sync_tests::make_test_client_options(registry, options));
    client.simulation_job<
        PredictedPosition,
        ashiato::sync::SyncSettings,
        ashiato::sync::FrameInfo,
        ashiato::sync::CueDispatcher>(registry, 0).each(
        [&](ashiato::Entity entity,
            PredictedPosition& position,
            ashiato::sync::SyncSettings& settings,
            ashiato::sync::FrameInfo& frame,
            ashiato::sync::CueDispatcher& cues) {
            position.x += 1.0f;
            if (emit_prediction_cue) {
                REQUIRE(cues.emit(settings, frame, entity, TestCue{7}, 1.0f));
                emit_prediction_cue = false;
            }
        });

    std::vector<ashiato::sync::SyncTraceEvent> events;
    ashiato::sync::SyncTracer tracer;
    tracer.set_callbacks(ashiato::sync::SyncTraceCallbacks{
        [&](const ashiato::sync::SyncTraceEvent& event) { events.push_back(event); }});
    client.set_tracer(&tracer);

    const ashiato::Entity server_entity = registry.create();
    REQUIRE(client.receive(registry, make_predicted_position_packet(1, server_entity, PredictedPosition{0.0f, 0.0f})));
    REQUIRE(client.tick(registry, client.fixed_dt_seconds()));
    REQUIRE(client.receive(registry, make_predicted_position_packet(2, server_entity, PredictedPosition{2.0f, 0.0f})));

    REQUIRE(std::any_of(events.begin(), events.end(), [](const ashiato::sync::SyncTraceEvent& event) {
        return event.type == ashiato::sync::SyncTraceEventType::CueRolledBack &&
            event.data.find("rollback_reason=server_mismatch") != std::string::npos;
    }));
}

TEST_CASE("predicted cue rollback tracing records resim omission reason") {
    ashiato::Registry registry;
    const ashiato::Entity position_component =
        ashiato::sync::register_sync_component<PredictedPosition>(registry, "PredictedPosition");
    const ashiato::sync::SyncArchetypeId archetype = ashiato::sync::define_archetype(
        registry,
        "PredictedActor",
        {{position_component, ashiato::sync::ReplicationAudience::All}});
    REQUIRE(archetype.value == 0);
    registry.register_component<CuePlayback>("CuePlayback");
    ashiato::sync::register_sync_cue<TestCue>(registry);
    ashiato_sync_tests::configure_test_client_registry(registry, 1);

    int prediction_jobs = 0;
    bool emit_during_resim = false;
    ashiato::sync::ReplicationClientOptions options;
    options.entities.default_mode = ashiato::sync::ReplicationClientMode::Predict;
    options.prediction.rollback_policy = ashiato::sync::ReplicationRollbackPolicy::All;
    ashiato::sync::ReplicationClient client(registry, ashiato_sync_tests::make_test_client_options(registry, options));
    client.simulation_job<
        PredictedPosition,
        ashiato::sync::SyncSettings,
        ashiato::sync::FrameInfo,
        ashiato::sync::CueDispatcher>(registry, 0).each(
        [&](ashiato::Entity entity,
            PredictedPosition& position,
            ashiato::sync::SyncSettings& settings,
            ashiato::sync::FrameInfo& frame,
            ashiato::sync::CueDispatcher& cues) {
            position.x += 1.0f;
            ++prediction_jobs;
            if (prediction_jobs == 2 || emit_during_resim) {
                REQUIRE(cues.emit(settings, frame, entity, TestCue{9}, 1.0f));
            }
        });

    std::vector<ashiato::sync::SyncTraceEvent> events;
    ashiato::sync::SyncTracer tracer;
    tracer.set_callbacks(ashiato::sync::SyncTraceCallbacks{
        [&](const ashiato::sync::SyncTraceEvent& event) { events.push_back(event); }});
    client.set_tracer(&tracer);

    const ashiato::Entity server_entity = registry.create();
    REQUIRE(client.receive(registry, make_predicted_position_packet(1, server_entity, PredictedPosition{0.0f, 0.0f})));
    REQUIRE(client.tick(registry, client.fixed_dt_seconds()));
    REQUIRE(client.tick(registry, client.fixed_dt_seconds()));
    REQUIRE(client.receive(registry, make_predicted_position_packet(2, server_entity, PredictedPosition{2.0f, 0.0f})));
    REQUIRE(client.tick(registry, client.fixed_dt_seconds()));

    REQUIRE(std::any_of(events.begin(), events.end(), [](const ashiato::sync::SyncTraceEvent& event) {
        return event.type == ashiato::sync::SyncTraceEventType::CueRolledBack &&
            event.data.find("rollback_reason=resim_not_replayed") != std::string::npos;
    }));
}
#endif

TEST_CASE("predicted client keeps local prediction phase when authoritative frames arrive early") {
    ashiato::Registry registry;
    const ashiato::Entity position_component =
        ashiato::sync::register_sync_component<PredictedPosition>(registry, "PredictedPosition");
    const ashiato::sync::SyncArchetypeId archetype = ashiato::sync::define_archetype(
        registry,
        "PredictedActor",
        {{position_component, ashiato::sync::ReplicationAudience::All}});
    REQUIRE(archetype.value == 0);
    ashiato_sync_tests::configure_test_client_registry(registry, 1);

    ashiato::sync::ReplicationClientOptions options;
    options.entities.default_mode = ashiato::sync::ReplicationClientMode::Predict;
    ashiato::sync::ReplicationClient client(registry, ashiato_sync_tests::make_test_client_options(registry, options));
    client.simulation_job<PredictedPosition>(registry, 0).each([](ashiato::Entity, PredictedPosition& position) {
        position.x += 1.0f;
    });

    const ashiato::Entity server_entity = registry.create();
    REQUIRE(client.receive(registry, make_predicted_position_packet(1, server_entity, PredictedPosition{0.0f, 0.0f}, 1)));
    const ashiato::Entity local = client.local_entity(test_client_entity_network_id(1, server_entity));
    REQUIRE(local);

    REQUIRE(client.tick(registry, client.fixed_dt_seconds() * 0.5));
    REQUIRE(registry.get<PredictedPosition>(local).x == 0.0f);

    REQUIRE(client.receive(registry, make_predicted_position_packet(2, server_entity, PredictedPosition{1.0f, 0.0f}, 2)));
    REQUIRE(client.tick(registry, client.fixed_dt_seconds() * 0.5));
    REQUIRE(registry.get<PredictedPosition>(local).x == 1.0f);
}

TEST_CASE("predicted client ONLY_AFFECTED resim runs jobs for affected entities") {
    ashiato::Registry registry;
    const ashiato::Entity position_component =
        ashiato::sync::register_sync_component<PredictedPosition>(registry, "PredictedPosition");
    const ashiato::sync::SyncArchetypeId archetype = ashiato::sync::define_archetype(
        registry,
        "PredictedActor",
        {{position_component, ashiato::sync::ReplicationAudience::All}});
    REQUIRE(archetype.value == 0);
    ashiato_sync_tests::configure_test_client_registry(registry, 1);
    int calls = 0;

    ashiato::sync::ReplicationClientOptions options;
    options.entities.default_mode = ashiato::sync::ReplicationClientMode::Predict;
    options.prediction.rollback_policy = ashiato::sync::ReplicationRollbackPolicy::OnlyAffected;
    ashiato::sync::ReplicationClient client(registry, ashiato_sync_tests::make_test_client_options(registry, options));
    client.simulation_job<PredictedPosition>(registry, 0).each([&](ashiato::Entity, PredictedPosition& position) {
        ++calls;
        position.x += 1.0f;
    });

    const ashiato::Entity first_server = registry.create();
    const ashiato::Entity second_server = registry.create();
    REQUIRE(client.receive(registry, make_predicted_position_packet(1, first_server, PredictedPosition{0.0f, 0.0f}, 1)));
    REQUIRE(client.receive(registry, make_predicted_position_packet(1, second_server, PredictedPosition{0.0f, 0.0f}, 2)));
    REQUIRE(client.tick(registry, client.fixed_dt_seconds()));
    REQUIRE(client.tick(registry, client.fixed_dt_seconds()));
    REQUIRE(calls == 4);

    REQUIRE(client.receive(registry, make_predicted_position_packet(2, first_server, PredictedPosition{2.0f, 0.0f}, 3)));
    REQUIRE(client.receive(registry, make_predicted_position_packet(2, second_server, PredictedPosition{1.0f, 0.0f}, 4)));
    REQUIRE(client.tick(registry, client.fixed_dt_seconds()));

    REQUIRE(calls == 7);
    REQUIRE(registry.get<PredictedPosition>(client.local_entity(test_client_entity_network_id(1, first_server))).x == 4.0f);
    REQUIRE(registry.get<PredictedPosition>(client.local_entity(test_client_entity_network_id(1, second_server))).x == 3.0f);
}

namespace {

struct PilotAndCraft {
    float pilot = 0.0f;
    float craft = 0.0f;
    int replay_visits = 0;
};

// A pilot and the craft it drives, both predicted. The pilot's lever climbs by
// one each frame; the craft moves by whatever the pilot's lever reads on the
// same frame, the way a game's craft reads its pilot's controls. The server
// then corrects the craft alone: the pilot's record agrees.
PilotAndCraft fly_pilot_and_craft(ashiato::sync::ReplicationRollbackPolicy policy) {
    ashiato::Registry registry;
    const ashiato::Entity position_component =
        ashiato::sync::register_sync_component<PredictedPosition>(registry, "PredictedPosition");
    const ashiato::sync::SyncArchetypeId archetype = ashiato::sync::define_archetype(
        registry,
        "PredictedActor",
        {{position_component, ashiato::sync::ReplicationAudience::All}});
    REQUIRE(archetype.value == 0);
    ashiato_sync_tests::configure_test_client_registry(registry, 1);

    ashiato::sync::ReplicationClientOptions options;
    options.entities.default_mode = ashiato::sync::ReplicationClientMode::Predict;
    options.prediction.rollback_policy = policy;
    ashiato::sync::ReplicationClient client(registry, ashiato_sync_tests::make_test_client_options(registry, options));

    ashiato::Entity pilot;
    ashiato::Entity craft;
    float lever = 0.0f;
    bool replaying = false;
    PilotAndCraft result;
    // Two jobs in order, so the craft reads the lever its pilot set this frame.
    client.simulation_job<PredictedPosition>(registry, 0).each([&](ashiato::Entity entity, PredictedPosition& position) {
        if (replaying) {
            ++result.replay_visits;
        }
        if (entity == pilot) {
            position.x += 1.0f;
            lever = position.x;
        }
    });
    client.simulation_job<PredictedPosition>(registry, 1).each([&](ashiato::Entity entity, PredictedPosition& position) {
        if (entity == craft) {
            position.x += lever;
        }
    });

    const ashiato::Entity pilot_server = registry.create();
    const ashiato::Entity craft_server = registry.create();
    REQUIRE(client.receive(registry, make_predicted_position_packet(1, pilot_server, PredictedPosition{0.0f, 0.0f}, 1)));
    REQUIRE(client.receive(registry, make_predicted_position_packet(1, craft_server, PredictedPosition{0.0f, 0.0f}, 2)));
    pilot = client.local_entity(test_client_entity_network_id(1, pilot_server));
    craft = client.local_entity(test_client_entity_network_id(1, craft_server));
    REQUIRE(pilot);
    REQUIRE(craft);

    // Frames 2, 3 and 4: pilot 1, 2, 3; craft 1, 3, 6.
    for (int tick = 0; tick < 3; ++tick) {
        REQUIRE(client.tick(registry, client.fixed_dt_seconds()));
    }
    REQUIRE(registry.get<PredictedPosition>(pilot).x == 3.0f);
    REQUIRE(registry.get<PredictedPosition>(craft).x == 6.0f);

    // The server agrees with the pilot at frame 2 and moves the craft to 5.
    REQUIRE(client.receive(registry, make_predicted_position_packet(2, pilot_server, PredictedPosition{1.0f, 0.0f}, 3)));
    REQUIRE(client.receive(registry, make_predicted_position_packet(2, craft_server, PredictedPosition{5.0f, 0.0f}, 4)));
    replaying = true;
    REQUIRE(client.tick(registry, client.fixed_dt_seconds()));
    replaying = false;

    result.pilot = registry.get<PredictedPosition>(pilot).x;
    result.craft = registry.get<PredictedPosition>(craft).x;
    return result;
}

}  // namespace

TEST_CASE("predicted client PREDICTED_ONLY replays a pilot and its craft together") {
    // All replays frames 3 and 4 for both, then predicts frame 5: pilot 2, 3,
    // 4; craft 5 + 2 + 3 + 4 = 14.
    const PilotAndCraft all = fly_pilot_and_craft(ashiato::sync::ReplicationRollbackPolicy::All);
    REQUIRE(all.pilot == 4.0f);
    REQUIRE(all.craft == 14.0f);

    const PilotAndCraft predicted = fly_pilot_and_craft(ashiato::sync::ReplicationRollbackPolicy::PredictedOnly);
    REQUIRE(predicted.pilot == all.pilot);
    REQUIRE(predicted.craft == all.craft);
    REQUIRE(predicted.replay_visits == all.replay_visits);

    // OnlyAffected resets and replays the craft alone, so the craft reads the
    // pilot's lever as the present frame left it on every replayed frame: the
    // pilot and its craft part. This pins that defect; if it ever passes as
    // All does, OnlyAffected has been fixed and this check should flip.
    const PilotAndCraft affected = fly_pilot_and_craft(ashiato::sync::ReplicationRollbackPolicy::OnlyAffected);
    REQUIRE(affected.craft != all.craft);
}

namespace {

enum class CarrierReplay { All, PredictedOnlyHeld, PredictedOnlyCompanion };

// A predicted craft riding a buffered carrier: the craft moves by whatever the carrier reads on the same frame, as a
// helicopter on a moving deck does. The carrier's buffered frames climb by ten a frame; the server then corrects the
// craft. Answers the craft's position after the rollback.
float ride_a_carrier(CarrierReplay replay) {
    ashiato::Registry registry;
    const ashiato::Entity position_component =
        ashiato::sync::register_sync_component<PredictedPosition>(registry, "PredictedPosition");
    const ashiato::sync::SyncArchetypeId archetype = ashiato::sync::define_archetype(
        registry,
        "PredictedActor",
        {{position_component, ashiato::sync::ReplicationAudience::All}});
    REQUIRE(archetype.value == 0);
    ashiato_sync_tests::configure_test_client_registry(registry, 1);

    const ashiato::Entity craft_server = registry.create();
    const ashiato::Entity carrier_server = registry.create();
    const ashiato::sync::ClientEntityNetworkId craft_id = test_client_entity_network_id(1, craft_server);
    const ashiato::sync::ClientEntityNetworkId carrier_id = test_client_entity_network_id(1, carrier_server);

    ashiato::sync::ReplicationClientOptions options;
    options.entities.default_mode = ashiato::sync::ReplicationClientMode::Snap;
    options.entities.mode_selector = [&](const ashiato::sync::ReplicatedEntityUpdateView& update) {
        if (update.client_entity_network_id == craft_id) {
            return ashiato::sync::ReplicationClientMode::Predict;
        }
        if (update.client_entity_network_id == carrier_id) {
            return ashiato::sync::ReplicationClientMode::BufferedInterpolation;
        }
        return ashiato::sync::ReplicationClientMode::Snap;
    };
    options.prediction.rollback_policy = replay == CarrierReplay::All
        ? ashiato::sync::ReplicationRollbackPolicy::All
        : ashiato::sync::ReplicationRollbackPolicy::PredictedOnly;
    options.buffered.buffered_frame_lag = 1;
    options.buffered.auto_buffered_frame_lag = false;
    ashiato::Entity craft;
    ashiato::Entity carrier;
    int companions_asked = 0;
    if (replay == CarrierReplay::PredictedOnlyCompanion) {
        options.predicted_only_companions_handler = [&](ashiato::Registry&,
                                                        const std::vector<ashiato::Entity>& predicted,
                                                        std::vector<ashiato::Entity>& companions) {
            ++companions_asked;
            REQUIRE(predicted.size() == 1U);
            REQUIRE(predicted.front() == craft);
            companions.push_back(carrier);
        };
    }
    ashiato::sync::ReplicationClient client(registry, ashiato_sync_tests::make_test_client_options(registry, options));

    float deck = 0.0f;
    client.simulation_job<PredictedPosition>(registry, 0).each([&](ashiato::Entity entity, PredictedPosition& position) {
        if (entity == carrier) {
            deck = position.x;
        }
    });
    client.simulation_job<PredictedPosition>(registry, 1).each([&](ashiato::Entity entity, PredictedPosition& position) {
        if (entity == craft) {
            position.x += deck;
        }
    });

    std::uint32_t packet = 1;
    REQUIRE(client.receive(registry, make_predicted_position_packet(1, craft_server, PredictedPosition{0.0f, 0.0f}, packet++)));
    for (ashiato::sync::SyncFrame frame = 1; frame <= 6; ++frame) {
        REQUIRE(client.receive(registry, make_predicted_position_packet(
            frame, carrier_server, PredictedPosition{10.0f * static_cast<float>(frame), 0.0f}, packet++)));
    }
    craft = client.local_entity(craft_id);
    REQUIRE(craft);
    REQUIRE(client.apply_frame(registry, 1));
    carrier = client.local_entity(carrier_id);
    REQUIRE(carrier);

    for (int tick = 0; tick < 4; ++tick) {
        REQUIRE(client.tick(registry, client.fixed_dt_seconds()));
    }
    // The server moves the craft at frame 2 to somewhere the client did not predict.
    REQUIRE(client.receive(registry, make_predicted_position_packet(2, craft_server, PredictedPosition{500.0f, 0.0f}, packet++)));
    REQUIRE(client.tick(registry, client.fixed_dt_seconds()));
    if (replay == CarrierReplay::PredictedOnlyCompanion) {
        REQUIRE(companions_asked == 1);
    }
    return registry.get<PredictedPosition>(craft).x;
}

}  // namespace

TEST_CASE("predicted client PREDICTED_ONLY replays a buffered companion as All does") {
    const float all = ride_a_carrier(CarrierReplay::All);
    const float companion = ride_a_carrier(CarrierReplay::PredictedOnlyCompanion);
    const float held = ride_a_carrier(CarrierReplay::PredictedOnlyHeld);
    REQUIRE(all > 500.0f);
    REQUIRE(companion == all);
    // Held where the last tick left it, the carrier reads the same on every replayed frame: the bounded error the
    // companion handler exists to remove.
    REQUIRE(held != all);
}

TEST_CASE("predicted client cosmetic jobs do not run during resimulation") {
    ashiato::Registry registry;
    const ashiato::Entity position_component =
        ashiato::sync::register_sync_component<PredictedPosition>(registry, "PredictedPosition");
    const ashiato::sync::SyncArchetypeId archetype = ashiato::sync::define_archetype(
        registry,
        "PredictedActor",
        {{position_component, ashiato::sync::ReplicationAudience::All}});
    REQUIRE(archetype.value == 0);
    ashiato_sync_tests::configure_test_client_registry(registry, 1);

    int simulation_calls = 0;
    int cosmetic_calls = 0;

    ashiato::sync::ReplicationClientOptions options;
    options.entities.default_mode = ashiato::sync::ReplicationClientMode::Predict;
    options.prediction.rollback_policy = ashiato::sync::ReplicationRollbackPolicy::All;
    ashiato::sync::ReplicationClient client(registry, ashiato_sync_tests::make_test_client_options(registry, options));

    client.simulation_job<PredictedPosition>(registry, 0).each([&](ashiato::Entity, PredictedPosition& position) {
        ++simulation_calls;
        position.x += 1.0f;
    });
    const ashiato::Entity cosmetic_job =
        client.cosmetic_job<PredictedPosition>(registry, 1).each([&](ashiato::Entity, PredictedPosition&) {
            ++cosmetic_calls;
        });
    REQUIRE(registry.has<ashiato::sync::NoResim>(cosmetic_job));

    const ashiato::Entity server_entity = registry.create();
    REQUIRE(client.receive(registry, make_predicted_position_packet(1, server_entity, PredictedPosition{0.0f, 0.0f})));
    REQUIRE(client.tick(registry, client.fixed_dt_seconds()));
    REQUIRE(client.tick(registry, client.fixed_dt_seconds()));
    REQUIRE(simulation_calls == 2);
    REQUIRE(cosmetic_calls == 2);

    REQUIRE(client.receive(registry, make_predicted_position_packet(2, server_entity, PredictedPosition{2.0f, 0.0f})));
    REQUIRE(client.tick(registry, client.fixed_dt_seconds()));

    REQUIRE(simulation_calls == 4);
    REQUIRE(cosmetic_calls == 3);
}

TEST_CASE("client simulation jobs skip NoSimulate entities") {
    ashiato::Registry registry;
    registry.register_component<PredictedPosition>("PredictedPosition");
    ashiato_sync_tests::configure_test_client_registry(registry, 1);

    const ashiato::Entity simulated = registry.create();
    const ashiato::Entity skipped = registry.create();
    REQUIRE(registry.add<PredictedPosition>(simulated, PredictedPosition{0.0f, 0.0f}) != nullptr);
    REQUIRE(registry.add<PredictedPosition>(skipped, PredictedPosition{0.0f, 0.0f}) != nullptr);
    REQUIRE(registry.add<ashiato::sync::NoSimulate>(skipped));

    ashiato::sync::ReplicationClient client(registry, ashiato_sync_tests::make_test_client_options(registry, {}));
    client.simulation_job<PredictedPosition>(registry, 0).each([](ashiato::Entity, PredictedPosition& position) {
        position.x += 1.0f;
    });

    registry.run_jobs();

    REQUIRE(registry.get<PredictedPosition>(simulated).x == 1.0f);
    REQUIRE(registry.get<PredictedPosition>(skipped).x == 0.0f);
}

TEST_CASE("role-tagged simulation jobs register endpoint-specific job roles") {
    ashiato::Registry client_registry;
    client_registry.register_component<PredictedPosition>("PredictedPosition");
    ashiato_sync_tests::configure_test_client_registry(client_registry, 1);

    const ashiato::Entity simulated = client_registry.create();
    const ashiato::Entity skipped = client_registry.create();
    REQUIRE(client_registry.add<PredictedPosition>(simulated, PredictedPosition{0.0f, 0.0f}) != nullptr);
    REQUIRE(client_registry.add<PredictedPosition>(skipped, PredictedPosition{0.0f, 0.0f}) != nullptr);
    REQUIRE(client_registry.add<ashiato::sync::NoSimulate>(skipped));

    ashiato::sync::SimulationJobs client_jobs(
        client_registry,
        ashiato::sync::SimulationJobEndpointRole::DedicatedClient);
    const ashiato::Entity shared_client_job =
        client_jobs.shared<PredictedPosition>(0).each([](ashiato::Entity, PredictedPosition& position) {
            position.x += 1.0f;
        });
    const ashiato::Entity cosmetic_client_job =
        client_jobs.client_only<PredictedPosition>(1).each([](ashiato::Entity, PredictedPosition& position) {
            position.y += 1.0f;
        });
    const ashiato::Entity server_only_client_job =
        client_jobs.server_only<PredictedPosition>(2).each([](ashiato::Entity, PredictedPosition& position) {
            position.x += 100.0f;
        });

    REQUIRE(client_registry.has<ashiato::sync::PredictiveSimulationJob>(shared_client_job));
    REQUIRE(client_registry.has<ashiato::sync::NoResim>(cosmetic_client_job));
    REQUIRE_FALSE(server_only_client_job);

    client_registry.run_jobs();
    REQUIRE(client_registry.get<PredictedPosition>(simulated).x == 1.0f);
    REQUIRE(client_registry.get<PredictedPosition>(skipped).x == 0.0f);
    REQUIRE(client_registry.get<PredictedPosition>(simulated).y == 1.0f);
    REQUIRE(client_registry.get<PredictedPosition>(skipped).y == 1.0f);

    ashiato::Registry server_registry;
    server_registry.register_component<PredictedPosition>("PredictedPosition");
    ashiato::sync::SimulationJobs server_jobs(
        server_registry,
        ashiato::sync::SimulationJobEndpointRole::DedicatedServer);
    const ashiato::Entity shared_server_job =
        server_jobs.shared<PredictedPosition>(0).each([](ashiato::Entity, PredictedPosition&) {});
    const ashiato::Entity client_only_server_job =
        server_jobs.client_only<PredictedPosition>(1).each([](ashiato::Entity, PredictedPosition&) {});
    const ashiato::Entity server_only_server_job =
        server_jobs.server_only<PredictedPosition>(2).each([](ashiato::Entity, PredictedPosition&) {});

    REQUIRE(shared_server_job);
    server_registry.register_component<ashiato::sync::PredictiveSimulationJob>(
        "ashiato.sync.PredictiveSimulationJob");
    REQUIRE_FALSE(server_registry.has<ashiato::sync::PredictiveSimulationJob>(shared_server_job));
    REQUIRE_FALSE(client_only_server_job);
    REQUIRE(server_only_server_job);

    ashiato::Registry listen_registry;
    listen_registry.register_component<PredictedPosition>("PredictedPosition");
    ashiato::sync::SimulationJobs listen_jobs(
        listen_registry,
        ashiato::sync::SimulationJobEndpointRole::ListenServer);
    const ashiato::Entity shared_listen_job =
        listen_jobs.shared<PredictedPosition>(0).each([](ashiato::Entity, PredictedPosition&) {});
    const ashiato::Entity client_only_listen_job =
        listen_jobs.client_only<PredictedPosition>(1).each([](ashiato::Entity, PredictedPosition&) {});
    const ashiato::Entity server_only_listen_job =
        listen_jobs.server_only<PredictedPosition>(2).each([](ashiato::Entity, PredictedPosition&) {});

    REQUIRE(shared_listen_job);
    listen_registry.register_component<ashiato::sync::PredictiveSimulationJob>(
        "ashiato.sync.PredictiveSimulationJob");
    REQUIRE_FALSE(listen_registry.has<ashiato::sync::PredictiveSimulationJob>(shared_listen_job));
    REQUIRE(client_only_listen_job);
    REQUIRE(listen_registry.has<ashiato::sync::NoResim>(client_only_listen_job));
    REQUIRE(server_only_listen_job);
}

TEST_CASE("role-tagged simulation jobs support advanced builder modifiers") {
    ashiato::Registry client_registry;
    client_registry.register_component<PredictedPosition>("PredictedPosition");
    client_registry.register_component<NetworkedPosition>("NetworkedPosition");
    ashiato::sync::SimulationJobs client_jobs(
        client_registry,
        ashiato::sync::SimulationJobEndpointRole::DedicatedClient);

    const ashiato::Entity optional_source = client_registry.create();
    const ashiato::Entity no_optional = client_registry.create();
    const ashiato::Entity no_simulate = client_registry.create();
    const ashiato::Entity untagged = client_registry.create();
    REQUIRE(client_registry.add<PredictedPosition>(optional_source, PredictedPosition{0.0f, 0.0f}) != nullptr);
    REQUIRE(client_registry.add<PredictedPosition>(no_optional, PredictedPosition{0.0f, 0.0f}) != nullptr);
    REQUIRE(client_registry.add<PredictedPosition>(no_simulate, PredictedPosition{0.0f, 0.0f}) != nullptr);
    REQUIRE(client_registry.add<PredictedPosition>(untagged, PredictedPosition{0.0f, 0.0f}) != nullptr);
    REQUIRE(client_registry.add<NetworkedPosition>(optional_source, NetworkedPosition{3.0f, 4.0f}) != nullptr);
    REQUIRE(client_registry.add<ashiato::sync::NoResim>(optional_source));
    REQUIRE(client_registry.add<ashiato::sync::NoResim>(no_optional));
    REQUIRE(client_registry.add<ashiato::sync::NoResim>(no_simulate));
    REQUIRE(client_registry.add<ashiato::sync::NoSimulate>(no_simulate));

    int optional_calls = 0;
    const ashiato::Entity optional_job = client_jobs.shared<PredictedPosition>(0)
        .name("advanced shared optional")
        .single_thread()
        .min_entities_per_thread(1)
        .optional<NetworkedPosition>()
        .with_tags<const ashiato::sync::NoResim>()
        .without_tags<const ashiato::sync::NoSimulate>()
        .each([&](
            auto& view,
            const ashiato::sync::JobRunContext& context,
            ashiato::Entity,
            PredictedPosition& position) {
            REQUIRE(context.is_predicting_client());
            ++optional_calls;
            if (const NetworkedPosition* networked = view.template try_get<NetworkedPosition>()) {
                position.x += networked->x;
                position.y += networked->y;
            } else {
                position.x += 1.0f;
            }
        });
    REQUIRE(client_registry.has<ashiato::sync::PredictiveSimulationJob>(optional_job));

    client_registry.run_jobs();
    REQUIRE(optional_calls == 2);
    REQUIRE(client_registry.get<PredictedPosition>(optional_source).x == 3.0f);
    REQUIRE(client_registry.get<PredictedPosition>(optional_source).y == 4.0f);
    REQUIRE(client_registry.get<PredictedPosition>(no_optional).x == 1.0f);
    REQUIRE(client_registry.get<PredictedPosition>(no_simulate).x == 0.0f);
    REQUIRE(client_registry.get<PredictedPosition>(untagged).x == 0.0f);

    ashiato::Registry server_registry;
    server_registry.register_component<PredictedPosition>("PredictedPosition");
    server_registry.register_component<NetworkedPosition>("NetworkedPosition");
    ashiato::sync::SimulationJobs server_jobs(
        server_registry,
        ashiato::sync::SimulationJobEndpointRole::DedicatedServer);

    const ashiato::Entity accessed = server_registry.create();
    const ashiato::Entity target = server_registry.create();
    REQUIRE(server_registry.add<PredictedPosition>(accessed, PredictedPosition{0.0f, 0.0f}) != nullptr);
    REQUIRE(server_registry.add<NetworkedPosition>(target, NetworkedPosition{5.0f, 0.0f}) != nullptr);

    server_jobs.shared<PredictedPosition>(0)
        .access_other_entities<const NetworkedPosition>()
        .each([&](
            auto& view,
            const ashiato::sync::JobRunContext& context,
            ashiato::Entity,
            PredictedPosition& position) {
            REQUIRE(context.is_authority());
            position.x += view.template get<const NetworkedPosition>(target).x;
        });

    server_jobs.server_only<PredictedPosition>(1)
        .max_threads(2)
        .structural<ashiato::sync::NoSimulate>()
        .each([](
            auto& structural,
            const ashiato::sync::JobRunContext& context,
            ashiato::Entity,
            PredictedPosition& position) {
            REQUIRE(context.is_server_only());
            position.y += 2.0f;
            REQUIRE(structural.template add<ashiato::sync::NoSimulate>());
        });

    server_registry.run_jobs();
    REQUIRE(server_registry.get<PredictedPosition>(accessed).x == 5.0f);
    REQUIRE(server_registry.get<PredictedPosition>(accessed).y == 2.0f);
    REQUIRE(server_registry.has<ashiato::sync::NoSimulate>(accessed));
}

TEST_CASE("predicted client applies authoritative destroys immediately") {
    ashiato::Registry registry;
    const ashiato::Entity position_component =
        ashiato::sync::register_sync_component<PredictedPosition>(registry, "PredictedPosition");
    const ashiato::sync::SyncArchetypeId archetype = ashiato::sync::define_archetype(
        registry,
        "PredictedActor",
        {{position_component, ashiato::sync::ReplicationAudience::All}});
    REQUIRE(archetype.value == 0);
    ashiato_sync_tests::configure_test_client_registry(registry, 1);

    const ashiato::Entity server_entity = registry.create();
    ashiato::sync::ReplicationClientOptions options;
    options.entities.default_mode = ashiato::sync::ReplicationClientMode::Predict;
    ashiato::sync::ReplicationClient client(registry, ashiato_sync_tests::make_test_client_options(registry, options));

    REQUIRE(client.receive(registry, make_predicted_position_packet(1, server_entity, PredictedPosition{0.0f, 0.0f})));
    const ashiato::Entity local = client.local_entity(test_client_entity_network_id(1, server_entity));
    REQUIRE(local);
    REQUIRE(registry.alive(local));

    REQUIRE(client.receive(registry, make_destroy_packet(2, server_entity)));
    REQUIRE_FALSE(registry.alive(local));
    REQUIRE_FALSE(client.local_entity(test_client_entity_network_id(1, server_entity)));
}
