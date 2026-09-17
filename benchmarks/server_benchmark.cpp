#include "benchmark_helpers.hpp"

#include <benchmark/benchmark.h>

#include <algorithm>
#include <cstdint>
#include <utility>
#include <vector>

namespace {

using namespace ashiato::sync::benchmarks;

void BM_ServerTickFullBudget(benchmark::State& state) {
    const int entity_count = static_cast<int>(state.range(0));
    const int client_count = static_cast<int>(state.range(1));

    ashiato::Registry registry;
    const ashiato::sync::SyncArchetypeId archetype = define_archetype(registry);
    const std::vector<ashiato::Entity> entities = create_entities(registry, entity_count);

    std::uint64_t sent = 0;
    ashiato::sync::ReplicationServerOptions options;
    options.bandwidth_limit_bytes_per_tick = static_cast<std::size_t>(entity_count) * 128U;
    options.transport = [&](ashiato::sync::ClientId client, const ashiato::BitBuffer& packet) {
        sent += client ^ packet.byte_size();
        benchmark::DoNotOptimize(sent);
    };
    ashiato::sync::ReplicationServer server(registry, options);
    add_clients(server, client_count);
    add_replication_configs(registry, entities, archetype);
    server.rediscover_all_replicated_entities(registry);

    for (auto _ : state) {
        server.tick(registry, server.options().fixed_dt_seconds);
    }

    state.SetItemsProcessed(
        state.iterations() * static_cast<std::int64_t>(entity_count) * static_cast<std::int64_t>(client_count));
}

void BM_ServerTickBudgetLimited(benchmark::State& state) {
    const int entity_count = static_cast<int>(state.range(0));
    const int client_count = static_cast<int>(state.range(1));
    const int sends_per_client = static_cast<int>(state.range(2));

    ashiato::Registry registry;
    const ashiato::sync::SyncArchetypeId archetype = define_archetype(registry);
    const std::vector<ashiato::Entity> entities = create_entities(registry, entity_count);

    std::uint64_t sent = 0;
    ashiato::sync::ReplicationServerOptions options;
    options.bandwidth_limit_bytes_per_tick = static_cast<std::size_t>(sends_per_client) * 128U;
    options.transport = [&](ashiato::sync::ClientId client, const ashiato::BitBuffer& packet) {
        sent += client + packet.byte_size();
        benchmark::DoNotOptimize(sent);
    };
    ashiato::sync::ReplicationServer server(registry, options);
    add_clients(server, client_count);
    add_replication_configs(registry, entities, archetype);
    server.rediscover_all_replicated_entities(registry);

    for (auto _ : state) {
        server.tick(registry, server.options().fixed_dt_seconds);
    }

    state.SetItemsProcessed(
        state.iterations() * static_cast<std::int64_t>(sends_per_client) * static_cast<std::int64_t>(client_count));
}

void BM_ServerRefreshReplicatedChanges(benchmark::State& state) {
    const int entity_count = static_cast<int>(state.range(0));

    ashiato::Registry registry;
    const ashiato::sync::SyncArchetypeId archetype = define_archetype(registry);
    const std::vector<ashiato::Entity> entities = create_entities(registry, entity_count);
    ashiato::sync::ReplicationServerOptions options;
    options.transport = [](ashiato::sync::ClientId, const ashiato::BitBuffer&) {};
    ashiato::sync::ReplicationServer server(registry, options);
    add_clients(server, 4);

    for (auto _ : state) {
        for (const ashiato::Entity entity : entities) {
            benchmark::DoNotOptimize(registry.add<ashiato::sync::Replicated>(
                entity,
                ashiato::sync::Replicated{archetype}));
        }
        server.rediscover_all_replicated_entities(registry);
        for (const ashiato::Entity entity : entities) {
            benchmark::DoNotOptimize(registry.remove<ashiato::sync::Replicated>(entity));
        }
        server.rediscover_all_replicated_entities(registry);
    }

    state.SetItemsProcessed(state.iterations() * static_cast<std::int64_t>(entity_count) * 2);
}

void BM_ServerAddClientsAfterReplicated(benchmark::State& state) {
    const int entity_count = static_cast<int>(state.range(0));
    const int client_count = static_cast<int>(state.range(1));

    ashiato::Registry registry;
    const ashiato::sync::SyncArchetypeId archetype = define_archetype(registry);
    const std::vector<ashiato::Entity> entities = create_entities(registry, entity_count);

    for (auto _ : state) {
        ashiato::sync::ReplicationServer server(registry);
        add_replication_configs(registry, entities, archetype);
        server.rediscover_all_replicated_entities(registry);
        for (int i = 0; i < client_count; ++i) {
            benchmark::DoNotOptimize(server.add_client(static_cast<ashiato::sync::ClientId>(i + 1)));
        }
        for (const ashiato::Entity entity : entities) {
            registry.remove<ashiato::sync::Replicated>(entity);
        }
        server.rediscover_all_replicated_entities(registry);
    }

    state.SetItemsProcessed(state.iterations() * static_cast<std::int64_t>(entity_count) * client_count);
}

void BM_ServerTickSerializedFullBudget(benchmark::State& state) {
    const int entity_count = static_cast<int>(state.range(0));
    const int client_count = static_cast<int>(state.range(1));

    ashiato::Registry registry;
    const ashiato::sync::SyncArchetypeId archetype = define_archetype(registry);
    const std::vector<ashiato::Entity> entities = create_position_entities(registry, entity_count);

    std::uint64_t sent = 0;
    ashiato::sync::ReplicationServerOptions options;
    options.bandwidth_limit_bytes_per_tick = static_cast<std::size_t>(entity_count) * sizeof(Position);
    options.transport = [&](ashiato::sync::ClientId client, const ashiato::BitBuffer& payload) {
        sent += client + payload.byte_size();
        benchmark::DoNotOptimize(sent);
    };

    ashiato::sync::ReplicationServer server(registry, options);
    add_clients(server, client_count);
    add_replication_configs(registry, entities, archetype);
    server.rediscover_all_replicated_entities(registry);

    for (auto _ : state) {
        server.tick(registry, server.options().fixed_dt_seconds);
    }

    state.SetItemsProcessed(
        state.iterations() * static_cast<std::int64_t>(entity_count) * static_cast<std::int64_t>(client_count));
}

#ifdef ASHIATO_SYNC_ENABLE_TRACING
void BM_ServerTickTracingRuntimeDisabled(benchmark::State& state) {
    const int entity_count = static_cast<int>(state.range(0));
    const int client_count = static_cast<int>(state.range(1));

    ashiato::Registry registry;
    const ashiato::sync::SyncArchetypeId archetype = define_archetype(registry);
    const std::vector<ashiato::Entity> entities = create_position_entities(registry, entity_count);

    std::uint64_t sent = 0;
    ashiato::sync::ReplicationServerOptions options;
    options.bandwidth_limit_bytes_per_tick = static_cast<std::size_t>(entity_count) * sizeof(Position);
    options.transport = [&](ashiato::sync::ClientId client, const ashiato::BitBuffer& payload) {
        sent += client + payload.byte_size();
        benchmark::DoNotOptimize(sent);
    };

    ashiato::sync::SyncTracer tracer;
    tracer.set_enabled(false);

    ashiato::sync::ReplicationServer server(registry, options);
    server.set_tracer(&tracer);
    add_clients(server, client_count);
    add_replication_configs(registry, entities, archetype);
    server.rediscover_all_replicated_entities(registry);

    for (auto _ : state) {
        server.tick(registry, server.options().fixed_dt_seconds);
    }

    state.SetItemsProcessed(
        state.iterations() * static_cast<std::int64_t>(entity_count) * static_cast<std::int64_t>(client_count));
}

void BM_ServerTickTracingCallbacks(benchmark::State& state) {
    const int entity_count = static_cast<int>(state.range(0));
    const int client_count = static_cast<int>(state.range(1));

    ashiato::Registry registry;
    const ashiato::sync::SyncArchetypeId archetype = define_archetype(registry);
    const std::vector<ashiato::Entity> entities = create_position_entities(registry, entity_count);

    std::uint64_t sent = 0;
    ashiato::sync::ReplicationServerOptions options;
    options.bandwidth_limit_bytes_per_tick = static_cast<std::size_t>(entity_count) * sizeof(Position);
    options.transport = [&](ashiato::sync::ClientId client, const ashiato::BitBuffer& payload) {
        sent += client + payload.byte_size();
        benchmark::DoNotOptimize(sent);
    };

    std::uint64_t events = 0;
    ashiato::sync::SyncTraceCallbacks callbacks;
    callbacks.on_event = [&](const ashiato::sync::SyncTraceEvent&) {
        ++events;
        benchmark::DoNotOptimize(events);
    };
    ashiato::sync::SyncTracer tracer(callbacks);

    ashiato::sync::ReplicationServer server(registry, options);
    server.set_tracer(&tracer);
    add_clients(server, client_count);
    add_replication_configs(registry, entities, archetype);
    server.rediscover_all_replicated_entities(registry);

    for (auto _ : state) {
        server.tick(registry, server.options().fixed_dt_seconds);
    }

    state.SetItemsProcessed(
        state.iterations() * static_cast<std::int64_t>(entity_count) * static_cast<std::int64_t>(client_count));
}

void BM_ServerTickTracingFrameData(benchmark::State& state) {
    const int entity_count = static_cast<int>(state.range(0));
    const int client_count = static_cast<int>(state.range(1));

    ashiato::Registry registry;
    const ashiato::sync::SyncArchetypeId archetype = define_archetype(registry);
    const std::vector<ashiato::Entity> entities = create_position_entities(registry, entity_count);

    std::uint64_t sent = 0;
    ashiato::sync::ReplicationServerOptions options;
    options.bandwidth_limit_bytes_per_tick = static_cast<std::size_t>(entity_count) * sizeof(Position);
    options.transport = [&](ashiato::sync::ClientId client, const ashiato::BitBuffer& payload) {
        sent += client + payload.byte_size();
        benchmark::DoNotOptimize(sent);
    };

    std::uint64_t events = 0;
    ashiato::sync::SyncTraceCallbacks callbacks;
    callbacks.on_event = [&](const ashiato::sync::SyncTraceEvent&) {
        ++events;
        benchmark::DoNotOptimize(events);
    };
    ashiato::sync::SyncTracer tracer(callbacks);
    tracer.set_frame_data_enabled(true);

    ashiato::sync::ReplicationServer server(registry, options);
    server.set_tracer(&tracer);
    add_clients(server, client_count);
    add_replication_configs(registry, entities, archetype);
    server.rediscover_all_replicated_entities(registry);

    for (auto _ : state) {
        server.tick(registry, server.options().fixed_dt_seconds);
    }

    state.SetItemsProcessed(
        state.iterations() * static_cast<std::int64_t>(entity_count) * static_cast<std::int64_t>(client_count));
}
#endif

void BM_ServerTickSerializedDelta(benchmark::State& state) {
    const int entity_count = static_cast<int>(state.range(0));
    const int client_count = static_cast<int>(state.range(1));

    ashiato::Registry registry;
    const ashiato::sync::SyncArchetypeId archetype = define_delta_archetype(registry);
    const std::vector<ashiato::Entity> entities = create_delta_entities(registry, entity_count);

    std::uint64_t sent = 0;
    ashiato::sync::ReplicationServerOptions options;
    options.bandwidth_limit_bytes_per_tick = static_cast<std::size_t>(entity_count) * sizeof(DeltaPosition);
    options.transport = [&](ashiato::sync::ClientId client, const ashiato::BitBuffer& payload) {
        sent += client + payload.byte_size();
        benchmark::DoNotOptimize(sent);
    };

    ashiato::sync::ReplicationServer server(registry, options);
    add_clients(server, client_count);
    add_replication_configs(registry, entities, archetype);
    server.tick(registry, server.options().fixed_dt_seconds);

    for (auto _ : state) {
        server.tick(registry, server.options().fixed_dt_seconds);
    }

    state.SetItemsProcessed(
        state.iterations() * static_cast<std::int64_t>(entity_count) * static_cast<std::int64_t>(client_count));
}

void BM_ServerTickSerializedBudgetLimited(benchmark::State& state) {
    const int entity_count = static_cast<int>(state.range(0));
    const int client_count = static_cast<int>(state.range(1));
    const int sends_per_client = static_cast<int>(state.range(2));

    ashiato::Registry registry;
    const ashiato::sync::SyncArchetypeId archetype = define_archetype(registry);
    const std::vector<ashiato::Entity> entities = create_position_entities(registry, entity_count);

    std::uint64_t sent = 0;
    ashiato::sync::ReplicationServerOptions options;
    options.bandwidth_limit_bytes_per_tick = static_cast<std::size_t>(sends_per_client) * sizeof(Position);
    options.transport = [&](ashiato::sync::ClientId client, const ashiato::BitBuffer& payload) {
        sent += client + payload.byte_size();
        benchmark::DoNotOptimize(sent);
    };

    ashiato::sync::ReplicationServer server(registry, options);
    add_clients(server, client_count);
    add_replication_configs(registry, entities, archetype);
    server.rediscover_all_replicated_entities(registry);

    for (auto _ : state) {
        server.tick(registry, server.options().fixed_dt_seconds);
    }

    state.SetItemsProcessed(
        state.iterations() * static_cast<std::int64_t>(sends_per_client) * static_cast<std::int64_t>(client_count));
}

// The same starved shape as BM_ServerTickSerializedBudgetLimited, with the send loop bounded
// so it stops looking for more candidates after a few budget refusals. The gap between the
// two is the cost of serializing records the budget then discards.
void BM_ServerTickSerializedBudgetLimitedBounded(benchmark::State& state) {
    const int entity_count = static_cast<int>(state.range(0));
    const int client_count = static_cast<int>(state.range(1));
    const int sends_per_client = static_cast<int>(state.range(2));

    ashiato::Registry registry;
    const ashiato::sync::SyncArchetypeId archetype = define_archetype(registry);
    const std::vector<ashiato::Entity> entities = create_position_entities(registry, entity_count);

    std::uint64_t sent = 0;
    ashiato::sync::ReplicationServerOptions options;
    options.bandwidth_limit_bytes_per_tick = static_cast<std::size_t>(sends_per_client) * sizeof(Position);
    options.max_budget_refusals_per_client_tick = 4U;
    options.transport = [&](ashiato::sync::ClientId client, const ashiato::BitBuffer& payload) {
        sent += client + payload.byte_size();
        benchmark::DoNotOptimize(sent);
    };

    ashiato::sync::ReplicationServer server(registry, options);
    add_clients(server, client_count);
    add_replication_configs(registry, entities, archetype);
    server.rediscover_all_replicated_entities(registry);

    for (auto _ : state) {
        server.tick(registry, server.options().fixed_dt_seconds);
    }

    state.SetItemsProcessed(
        state.iterations() * static_cast<std::int64_t>(sends_per_client) * static_cast<std::int64_t>(client_count));
}

void BM_ServerTickPackedFullBudget(benchmark::State& state) {
    const int entity_count = static_cast<int>(state.range(0));
    const int client_count = static_cast<int>(state.range(1));

    ashiato::Registry registry;
    const ashiato::sync::SyncArchetypeId archetype = define_archetype(registry);
    const std::vector<ashiato::Entity> entities = create_position_entities(registry, entity_count);

    std::uint64_t sent = 0;
    ashiato::sync::ReplicationServerOptions options;
    options.bandwidth_limit_bytes_per_tick = static_cast<std::size_t>(entity_count) * sizeof(Position) * 4U;
    options.mtu_bytes = 1200;
    options.transport = [&](ashiato::sync::ClientId client, const ashiato::BitBuffer& payload) {
        sent += client + payload.byte_size();
        benchmark::DoNotOptimize(sent);
    };

    ashiato::sync::ReplicationServer server(registry, options);
    add_clients(server, client_count);
    add_replication_configs(registry, entities, archetype);
    server.rediscover_all_replicated_entities(registry);

    for (auto _ : state) {
        server.tick(registry, server.options().fixed_dt_seconds);
    }

    state.SetItemsProcessed(
        state.iterations() * static_cast<std::int64_t>(entity_count) * static_cast<std::int64_t>(client_count));
}

void BM_ServerTickPackedAckedDeltaShared(benchmark::State& state) {
    const int entity_count = static_cast<int>(state.range(0));
    const int client_count = static_cast<int>(state.range(1));

    ashiato::Registry registry;
    const ashiato::sync::SyncArchetypeId archetype = define_delta_archetype(registry);
    const std::vector<ashiato::Entity> entities = create_delta_entities(registry, entity_count);

    std::vector<std::pair<ashiato::sync::ClientId, ashiato::BitBuffer>> packets;
    packets.reserve(static_cast<std::size_t>(entity_count) * static_cast<std::size_t>(client_count));

    std::uint64_t sent = 0;
    ashiato::sync::ReplicationServerOptions options;
    options.bandwidth_limit_bytes_per_tick = static_cast<std::size_t>(entity_count) * sizeof(DeltaPosition) * 4U;
    options.mtu_bytes = 1200;
    options.transport = [&](ashiato::sync::ClientId client, const ashiato::BitBuffer& payload) {
        packets.push_back({client, payload});
        sent += client + payload.byte_size();
        benchmark::DoNotOptimize(sent);
    };

    ashiato::sync::ReplicationServer server(registry, options);
    add_clients(server, client_count);
    add_replication_configs(registry, entities, archetype);
    server.tick(registry, server.options().fixed_dt_seconds);
    ack_packets(server, registry, packets);
    packets.clear();

    for (auto _ : state) {
        server.tick(registry, server.options().fixed_dt_seconds);
        packets.clear();
    }

    state.SetItemsProcessed(
        state.iterations() * static_cast<std::int64_t>(entity_count) * static_cast<std::int64_t>(client_count));
}

void BM_ServerTickPackedMtuLimited(benchmark::State& state) {
    const int entity_count = static_cast<int>(state.range(0));
    const int client_count = static_cast<int>(state.range(1));

    ashiato::Registry registry;
    const ashiato::sync::SyncArchetypeId archetype = define_delta_archetype(registry);
    const std::vector<ashiato::Entity> entities = create_delta_entities(registry, entity_count);

    std::uint64_t sent = 0;
    ashiato::sync::ReplicationServerOptions options;
    options.bandwidth_limit_bytes_per_tick = static_cast<std::size_t>(entity_count) * sizeof(DeltaPosition) * 4U;
    options.mtu_bytes = 256;
    options.transport = [&](ashiato::sync::ClientId client, const ashiato::BitBuffer& payload) {
        sent += client + payload.byte_size();
        benchmark::DoNotOptimize(sent);
    };

    ashiato::sync::ReplicationServer server(registry, options);
    add_clients(server, client_count);
    add_replication_configs(registry, entities, archetype);
    server.rediscover_all_replicated_entities(registry);

    for (auto _ : state) {
        server.tick(registry, server.options().fixed_dt_seconds);
    }

    state.SetItemsProcessed(
        state.iterations() * static_cast<std::int64_t>(entity_count) * static_cast<std::int64_t>(client_count));
}

void BM_ServerProcessAckPacket(benchmark::State& state) {
    const int entity_count = static_cast<int>(state.range(0));

    std::int64_t processed = 0;
    for (auto _ : state) {
        state.PauseTiming();
        ashiato::Registry server_registry;
        const ashiato::sync::SyncArchetypeId archetype = define_delta_archetype(server_registry);
        const std::vector<ashiato::Entity> entities = create_delta_entities(server_registry, entity_count);

        std::vector<std::pair<ashiato::sync::ClientId, ashiato::BitBuffer>> sent;
        ashiato::sync::ReplicationServerOptions server_options;
        server_options.bandwidth_limit_bytes_per_tick =
            static_cast<std::size_t>(entity_count) * sizeof(DeltaPosition) * 16U;
        server_options.mtu_bytes = 1200;
        server_options.transport = [&](ashiato::sync::ClientId client, const ashiato::BitBuffer& packet) {
            sent.push_back({client, packet});
        };

        ashiato::sync::ReplicationServer server(server_registry, server_options);
        server.add_client(1);
        add_replication_configs(server_registry, entities, archetype);
        server.tick(server_registry, server.options().fixed_dt_seconds);

        ashiato::Registry client_registry;
        define_client_delta_schema(client_registry, false);
        ashiato::sync::ReplicationClient client(client_registry, make_client_options(ashiato::sync::ReplicationClientMode::Snap));
        for (const auto& packet : sent) {
            benchmark::DoNotOptimize(client.receive(client_registry, packet.second));
        }
        std::vector<ashiato::BitBuffer> acks = client.drain_ack_packets();
        state.ResumeTiming();

        for (const ashiato::BitBuffer& ack : acks) {
            processed += server.process_packet(server_registry, 1, ack) ? 1 : 0;
        }
        benchmark::DoNotOptimize(processed);
    }

    state.SetItemsProcessed(state.iterations() * static_cast<std::int64_t>(entity_count));
}

void BM_ServerTickDestroyBurst(benchmark::State& state) {
    const int entity_count = static_cast<int>(state.range(0));

    std::uint64_t sent = 0;
    for (auto _ : state) {
        state.PauseTiming();
        ashiato::Registry registry;
        const ashiato::sync::SyncArchetypeId archetype = define_delta_archetype(registry);
        const std::vector<ashiato::Entity> entities = create_delta_entities(registry, entity_count);

        std::vector<std::pair<ashiato::sync::ClientId, ashiato::BitBuffer>> packets;
        ashiato::sync::ReplicationServerOptions options;
        options.bandwidth_limit_bytes_per_tick = static_cast<std::size_t>(entity_count) * 32U;
        options.mtu_bytes = 1200;
        options.transport = [&](ashiato::sync::ClientId client, const ashiato::BitBuffer& payload) {
            packets.push_back({client, payload});
            sent += client + payload.byte_size();
        };

        ashiato::sync::ReplicationServer server(registry, options);
        server.add_client(1);
        add_replication_configs(registry, entities, archetype);
        server.tick(registry, server.options().fixed_dt_seconds);
        ack_packets(server, registry, packets);
        packets.clear();
        for (const ashiato::Entity entity : entities) {
            registry.destroy(entity);
        }
        state.ResumeTiming();

        server.tick(registry, server.options().fixed_dt_seconds);
        benchmark::DoNotOptimize(sent);
    }

    state.SetItemsProcessed(state.iterations() * static_cast<std::int64_t>(entity_count));
}

void BM_ServerTickPendingDestroysBudgetLimited(benchmark::State& state) {
    const int entity_count = static_cast<int>(state.range(0));
    const int destroys_per_tick = static_cast<int>(state.range(1));

    ashiato::Registry registry;
    const ashiato::sync::SyncArchetypeId archetype = define_delta_archetype(registry);
    const std::vector<ashiato::Entity> entities = create_delta_entities(registry, entity_count);

    std::uint64_t sent = 0;
    ashiato::sync::ReplicationServerOptions options;
    options.bandwidth_limit_bytes_per_tick = static_cast<std::size_t>(destroys_per_tick) * 16U;
    options.mtu_bytes = 1200;
    options.transport = [&](ashiato::sync::ClientId client, const ashiato::BitBuffer& payload) {
        sent += client + payload.byte_size();
        benchmark::DoNotOptimize(sent);
    };

    ashiato::sync::ReplicationServer server(registry, options);
    server.add_client(1);
    add_replication_configs(registry, entities, archetype);
    server.tick(registry, server.options().fixed_dt_seconds);
    for (const ashiato::Entity entity : entities) {
        registry.destroy(entity);
    }
    server.tick(registry, server.options().fixed_dt_seconds);

    for (auto _ : state) {
        server.tick(registry, server.options().fixed_dt_seconds);
    }

    state.SetItemsProcessed(state.iterations() * static_cast<std::int64_t>(destroys_per_tick));
}

void BM_ServerTickMutatingAckedDelta(benchmark::State& state) {
    const int entity_count = static_cast<int>(state.range(0));
    const int client_count = static_cast<int>(state.range(1));

    ashiato::Registry server_registry;
    const ashiato::sync::SyncArchetypeId archetype = define_delta_archetype(server_registry);
    const std::vector<ashiato::Entity> entities = create_delta_entities(server_registry, entity_count);

    std::vector<std::pair<ashiato::sync::ClientId, ashiato::BitBuffer>> packets;
    packets.reserve(static_cast<std::size_t>(entity_count) * static_cast<std::size_t>(client_count));

    ashiato::sync::ReplicationServerOptions server_options;
    server_options.bandwidth_limit_bytes_per_tick =
        static_cast<std::size_t>(entity_count) * sizeof(DeltaPosition) * 16U;
    server_options.mtu_bytes = 1200;
    server_options.transport = [&](ashiato::sync::ClientId client, const ashiato::BitBuffer& packet) {
        packets.push_back({client, packet});
    };
    ashiato::sync::ReplicationServer server(server_registry, server_options);
    add_clients(server, client_count);
    add_replication_configs(server_registry, entities, archetype);

    std::vector<ashiato::Registry> client_registries(static_cast<std::size_t>(client_count));
    std::vector<ashiato::sync::ReplicationClient> clients;
    clients.reserve(static_cast<std::size_t>(client_count));
    for (int client_index = 0; client_index < client_count; ++client_index) {
        define_client_delta_schema(client_registries[static_cast<std::size_t>(client_index)], false);
        clients.emplace_back(
            client_registries[static_cast<std::size_t>(client_index)],
            make_client_options(ashiato::sync::ReplicationClientMode::Snap));
    }

    std::uint64_t consumed = 0;
    int tick = 0;
    for (auto _ : state) {
        for (int index = 0; index < entity_count; ++index) {
            server_registry.write<DeltaPosition>(entities[static_cast<std::size_t>(index)]) =
                DeltaPosition{index + tick, index + tick + 1};
        }
        ++tick;

        packets.clear();
        server.tick(server_registry, server.options().fixed_dt_seconds);
        consumed += consume_packets(packets);
        for (const auto& packet : packets) {
            const std::size_t client_index = static_cast<std::size_t>(packet.first - 1U);
            benchmark::DoNotOptimize(clients[client_index].receive(client_registries[client_index], packet.second));
        }
        for (int client_index = 0; client_index < client_count; ++client_index) {
            const auto id = static_cast<ashiato::sync::ClientId>(client_index + 1);
            for (const ashiato::BitBuffer& ack :
                 clients[static_cast<std::size_t>(client_index)].drain_ack_packets()) {
                benchmark::DoNotOptimize(server.process_packet(server_registry, id, ack));
            }
        }
        benchmark::DoNotOptimize(consumed);
    }

    state.SetItemsProcessed(
        state.iterations() * static_cast<std::int64_t>(entity_count) * static_cast<std::int64_t>(client_count));
}

void BM_ServerTickOwnerAudienceMixed(benchmark::State& state) {
    const int entity_count = static_cast<int>(state.range(0));
    const int client_count = static_cast<int>(state.range(1));

    ashiato::Registry registry;
    const ashiato::sync::SyncArchetypeId archetype = define_archetype(registry);
    const std::vector<ashiato::Entity> entities = create_owned_entities(registry, entity_count, client_count);

    std::uint64_t sent = 0;
    ashiato::sync::ReplicationServerOptions options;
    options.bandwidth_limit_bytes_per_tick = static_cast<std::size_t>(entity_count) * 64U;
    options.mtu_bytes = 1200;
    options.transport = [&](ashiato::sync::ClientId client, const ashiato::BitBuffer& payload) {
        sent += client + payload.byte_size();
        benchmark::DoNotOptimize(sent);
    };

    ashiato::sync::ReplicationServer server(registry, options);
    add_clients(server, client_count);
    add_replication_configs(registry, entities, archetype);
    server.rediscover_all_replicated_entities(registry);

    for (auto _ : state) {
        server.tick(registry, server.options().fixed_dt_seconds);
    }

    state.SetItemsProcessed(
        state.iterations() * static_cast<std::int64_t>(entity_count) * static_cast<std::int64_t>(client_count));
}

void BM_ServerTickTaggedOwnerMixed(benchmark::State& state) {
    const int entity_count = static_cast<int>(state.range(0));
    const int client_count = static_cast<int>(state.range(1));
    const int tag_count = static_cast<int>(state.range(2));

    ashiato::Registry registry;
    const TaggedSchema schema = define_tagged_archetype(registry, tag_count);
    const std::vector<ashiato::Entity> entities =
        create_tagged_entities(registry, entity_count, client_count, schema.tags);

    std::vector<std::pair<ashiato::sync::ClientId, ashiato::BitBuffer>> packets;
    packets.reserve(static_cast<std::size_t>(entity_count) * static_cast<std::size_t>(client_count));
    std::uint64_t sent = 0;
    ashiato::sync::ReplicationServerOptions options;
    options.bandwidth_limit_bytes_per_tick = static_cast<std::size_t>(entity_count) * 96U;
    options.mtu_bytes = 1200;
    options.transport = [&](ashiato::sync::ClientId client, const ashiato::BitBuffer& payload) {
        packets.push_back({client, payload});
        sent += client + payload.byte_size();
        benchmark::DoNotOptimize(sent);
    };

    ashiato::sync::ReplicationServer server(registry, options);
    add_clients(server, client_count);
    add_replication_configs(registry, entities, schema.archetype);
    server.tick(registry, server.options().fixed_dt_seconds);
    for (int client_index = 0; client_index < client_count; ++client_index) {
        const auto client = static_cast<ashiato::sync::ClientId>(client_index + 1);
        for (const ashiato::Entity entity : entities) {
            benchmark::DoNotOptimize(server.acknowledge_entity(client, entity, 1));
        }
    }
    packets.clear();

    int tick = 0;
    for (auto _ : state) {
        const ashiato::Entity tag = schema.tags[static_cast<std::size_t>(tick) % schema.tags.size()];
        for (const ashiato::Entity entity : entities) {
            if ((tick & 1) == 0) {
                registry.add_tag(entity, tag);
            } else {
                registry.remove_tag(entity, tag);
            }
        }
        ++tick;
        server.tick(registry, server.options().fixed_dt_seconds);
        packets.clear();
    }

    state.SetItemsProcessed(
        state.iterations() * static_cast<std::int64_t>(entity_count) * static_cast<std::int64_t>(client_count));
}

void BM_ServerTickArchetypeDiversity(benchmark::State& state) {
    const int entity_count = static_cast<int>(state.range(0));
    const int client_count = static_cast<int>(state.range(1));

    ashiato::Registry registry;
    const std::vector<ashiato::sync::SyncArchetypeId> archetypes = define_diverse_archetypes(registry);
    const std::vector<ashiato::Entity> entities = create_diverse_entities(registry, entity_count, archetypes, client_count);

    std::uint64_t sent = 0;
    ashiato::sync::ReplicationServerOptions options;
    options.bandwidth_limit_bytes_per_tick = static_cast<std::size_t>(entity_count) * 96U;
    options.mtu_bytes = 1200;
    options.transport = [&](ashiato::sync::ClientId client, const ashiato::BitBuffer& payload) {
        sent += client + payload.byte_size();
        benchmark::DoNotOptimize(sent);
    };

    ashiato::sync::ReplicationServer server(registry, options);
    add_clients(server, client_count);
    add_diverse_replication_configs(registry, entities, archetypes);
    server.rediscover_all_replicated_entities(registry);

    for (auto _ : state) {
        server.tick(registry, server.options().fixed_dt_seconds);
    }

    state.SetItemsProcessed(
        state.iterations() * static_cast<std::int64_t>(entity_count) * static_cast<std::int64_t>(client_count));
}

void BM_ServerTickStressScheduler(benchmark::State& state) {
    const int entity_count = static_cast<int>(state.range(0));
    const int client_count = static_cast<int>(state.range(1));

    ashiato::Registry registry;
    const ashiato::sync::SyncArchetypeId archetype = define_delta_archetype(registry);
    const std::vector<ashiato::Entity> entities = create_delta_entities(registry, entity_count);

    std::uint64_t sent = 0;
    ashiato::sync::ReplicationServerOptions options;
    options.bandwidth_limit_bytes_per_tick = 1024U * 1024U;
    options.mtu_bytes = 1200;
    options.transport = [&](ashiato::sync::ClientId client, const ashiato::BitBuffer& payload) {
        sent += client + payload.byte_size();
        benchmark::DoNotOptimize(sent);
    };

    ashiato::sync::ReplicationServer server(registry, options);
    add_clients(server, client_count);
    add_replication_configs(registry, entities, archetype);
    server.rediscover_all_replicated_entities(registry);

    for (auto _ : state) {
        for (int index = 0; index < entity_count; ++index) {
            registry.write<DeltaPosition>(entities[static_cast<std::size_t>(index)]) =
                DeltaPosition{index + static_cast<int>(state.iterations()), index + 1};
        }
        server.tick(registry, server.options().fixed_dt_seconds);
    }

    state.SetItemsProcessed(
        state.iterations() * static_cast<std::int64_t>(entity_count) * static_cast<std::int64_t>(client_count));
}

void BM_BitBufferUnalignedBytes(benchmark::State& state) {
    const int byte_count = static_cast<int>(state.range(0));
    std::vector<char> bytes(static_cast<std::size_t>(byte_count), 'x');

    for (auto _ : state) {
        ashiato::BitBuffer buffer;
        buffer.reserve_bytes(static_cast<std::size_t>(byte_count) + 1U);
        buffer.write_bool(true);
        buffer.write_bytes(bytes.data(), bytes.size());
        benchmark::DoNotOptimize(buffer.byte_size());
    }

    state.SetBytesProcessed(state.iterations() * static_cast<std::int64_t>(byte_count));
}

void BM_BitBufferUnalignedReadUnsigned(benchmark::State& state) {
    const int bit_count = static_cast<int>(state.range(0));
    ashiato::BitBuffer source;
    source.reserve_bytes(static_cast<std::size_t>(bit_count + 8));
    source.write_bool(true);
    for (int index = 0; index < bit_count; ++index) {
        source.write_unsigned_bits(0xfedcba9876543210ULL + static_cast<std::uint64_t>(index), 64U);
    }

    std::uint64_t sum = 0;
    for (auto _ : state) {
        ashiato::BitBuffer input = source;
        benchmark::DoNotOptimize(input.read_bool());
        for (int index = 0; index < bit_count; ++index) {
            sum += input.read_unsigned_bits(64U);
        }
        benchmark::DoNotOptimize(sum);
    }

    state.SetItemsProcessed(state.iterations() * static_cast<std::int64_t>(bit_count));
}

void BM_BitBufferAppendBits(benchmark::State& state) {
    const int bit_count = static_cast<int>(state.range(0));
    ashiato::BitBuffer source;
    source.reserve_bytes(ashiato::sync::protocol::bytes_for_bits(static_cast<std::size_t>(bit_count)));
    for (int bit = 0; bit < bit_count; ++bit) {
        source.write_bool((bit & 1) != 0);
    }

    for (auto _ : state) {
        ashiato::BitBuffer destination;
        destination.reserve_bytes(source.byte_size());
        destination.write_buffer_bits(source);
        benchmark::DoNotOptimize(destination.byte_size());
    }

    state.SetItemsProcessed(state.iterations() * static_cast<std::int64_t>(bit_count));
}

void BM_QuantizedBytesAssign(benchmark::State& state) {
    const auto byte_count = static_cast<std::size_t>(state.range(0));
    std::vector<std::uint8_t> source(byte_count);
    for (std::size_t index = 0; index < source.size(); ++index) {
        source[index] = static_cast<std::uint8_t>((index * 13U + 7U) & 0xffU);
    }

    for (auto _ : state) {
        ashiato::sync::QuantizedBytes bytes;
        bytes.assign(source.data(), source.size());
        benchmark::DoNotOptimize(bytes.data());
        benchmark::DoNotOptimize(bytes.size());
    }

    state.SetBytesProcessed(state.iterations() * static_cast<std::int64_t>(byte_count));
}

void BM_QuantizedBytesCopy(benchmark::State& state) {
    const auto byte_count = static_cast<std::size_t>(state.range(0));
    std::vector<std::uint8_t> source_bytes(byte_count);
    for (std::size_t index = 0; index < source_bytes.size(); ++index) {
        source_bytes[index] = static_cast<std::uint8_t>((index * 13U + 7U) & 0xffU);
    }
    ashiato::sync::QuantizedBytes source;
    source.assign(source_bytes.data(), source_bytes.size());

    for (auto _ : state) {
        ashiato::sync::QuantizedBytes bytes = source;
        benchmark::DoNotOptimize(bytes.data());
        benchmark::DoNotOptimize(bytes.size());
    }

    state.SetBytesProcessed(state.iterations() * static_cast<std::int64_t>(byte_count));
}

void BM_ServerProcessInputPacket(benchmark::State& state) {
    const int frame_count = static_cast<int>(state.range(0));

    ashiato::Registry client_registry;
    ashiato::sync::register_sync_component<DeltaPosition>(client_registry, "DeltaPosition");
    ashiato::sync::ReplicationClientOptions client_options = make_client_options(ashiato::sync::ReplicationClientMode::Snap);
    ashiato::sync::set_client_input_component<DeltaPosition>(client_registry);
    const ashiato::Entity client_owned = client_registry.create();
    ashiato::sync::set_owner(client_registry, client_owned, 1);
    ashiato::sync::ReplicationClient client(client_registry, client_options);
    for (int frame = 0; frame < frame_count; ++frame) {
        client.set_input(client_registry, DeltaPosition{frame, frame + 1});
        client.tick(client_registry, client.fixed_dt_seconds());
    }
    std::vector<ashiato::BitBuffer> packets = client.drain_packets();
    auto input_packet = std::find_if(packets.begin(), packets.end(), [](ashiato::BitBuffer packet) {
        return static_cast<std::uint8_t>(packet.read_bits(ashiato::sync::protocol::message_bits)) == ashiato::sync::protocol::client_input_message;
    });
    if (input_packet == packets.end()) {
        state.SkipWithError("client did not emit input packet");
        return;
    }

    for (auto _ : state) {
        state.PauseTiming();
        ashiato::Registry registry;
        ashiato::sync::register_sync_component<DeltaPosition>(registry, "DeltaPosition");
        ashiato::sync::register_components(registry);
        registry.write<ashiato::sync::SyncSettings>().role = ashiato::sync::SyncRole::Server;
        registry.write<ashiato::sync::SyncAuthority>().authoritative = true;
        ashiato::sync::set_client_input_component<DeltaPosition>(registry);
        ashiato::sync::ReplicationServer server(registry);
        server.add_client(1);
        ashiato::BitBuffer packet = *input_packet;
        state.ResumeTiming();

        benchmark::DoNotOptimize(server.process_packet(registry, 1, packet));
    }

    state.SetItemsProcessed(state.iterations() * static_cast<std::int64_t>(frame_count));
}

void BM_ServerTickInputUpsert(benchmark::State& state) {
    const int entity_count = static_cast<int>(state.range(0));

    ashiato::Registry registry;
    ashiato::sync::register_sync_component<DeltaPosition>(registry, "DeltaPosition");
    ashiato::sync::register_components(registry);
        registry.write<ashiato::sync::SyncSettings>().role = ashiato::sync::SyncRole::Server;
        registry.write<ashiato::sync::SyncAuthority>().authoritative = true;
    ashiato::sync::set_client_input_component<DeltaPosition>(registry);
    for (int index = 0; index < entity_count; ++index) {
        const ashiato::Entity entity = registry.create();
        ashiato::sync::set_owner(registry, entity, 1);
    }

    ashiato::Registry client_registry;
    ashiato::sync::register_sync_component<DeltaPosition>(client_registry, "DeltaPosition");
    ashiato::sync::ReplicationClientOptions client_options = make_client_options(ashiato::sync::ReplicationClientMode::Snap);
    ashiato::sync::set_client_input_component<DeltaPosition>(client_registry);
    const ashiato::Entity client_owned = client_registry.create();
    ashiato::sync::set_owner(client_registry, client_owned, 1);
    ashiato::sync::ReplicationClient client(client_registry, client_options);
    client.set_input(client_registry, DeltaPosition{10, 20});
    client.tick(client_registry, client.fixed_dt_seconds());
    std::vector<ashiato::BitBuffer> packets = client.drain_packets();

    ashiato::sync::ReplicationServerOptions server_options;
    server_options.transport = [](ashiato::sync::ClientId, const ashiato::BitBuffer&) {};
    ashiato::sync::ReplicationServer server(registry, server_options);
    server.add_client(1);
    for (const ashiato::BitBuffer& packet : packets) {
        ashiato::BitBuffer copy = packet;
        if (static_cast<std::uint8_t>(copy.read_bits(ashiato::sync::protocol::message_bits)) == ashiato::sync::protocol::client_input_message) {
            benchmark::DoNotOptimize(server.process_packet(registry, 1, packet));
        }
    }

    for (auto _ : state) {
        server.tick(registry, server.options().fixed_dt_seconds);
    }

    state.SetItemsProcessed(state.iterations() * static_cast<std::int64_t>(entity_count));
}

void BM_ServerTickCappedLargeDelta(benchmark::State& state) {
    const int entity_count = static_cast<int>(state.range(0));
    const int backlog_frames = static_cast<int>(state.range(1));
    const int cap_frames = static_cast<int>(state.range(2));

    for (auto _ : state) {
        state.PauseTiming();
        ashiato::Registry registry;
        ashiato::sync::register_components(registry);
        registry.write<ashiato::sync::SyncSettings>().role = ashiato::sync::SyncRole::Server;
        registry.write<ashiato::sync::SyncAuthority>().authoritative = true;
        struct TickCounter {
            int frames = 0;
        };
        registry.register_component<TickCounter>("TickCounter");
        for (int index = 0; index < entity_count; ++index) {
            const ashiato::Entity entity = registry.create();
            registry.add<TickCounter>(entity, TickCounter{});
        }
        registry.job<TickCounter>(0).each([](ashiato::Entity, TickCounter& counter) {
            ++counter.frames;
        });
        ashiato::sync::ReplicationServerOptions options;
        options.max_fixed_steps_per_tick = static_cast<std::uint32_t>(cap_frames);
        options.transport = [](ashiato::sync::ClientId, const ashiato::BitBuffer&) {};
        ashiato::sync::ReplicationServer server(registry, options);
        state.ResumeTiming();

        benchmark::DoNotOptimize(server.tick(
            registry,
            static_cast<double>(backlog_frames) * server.options().fixed_dt_seconds));
    }

    state.SetItemsProcessed(
        state.iterations() * static_cast<std::int64_t>(entity_count) * static_cast<std::int64_t>(cap_frames));
}

void TickArgs(benchmark::internal::Benchmark* benchmark) {
    benchmark->Args({1024, 1})->Args({16384, 1})->Args({16384, 8})->Args({65536, 8});
}

void LimitedTickArgs(benchmark::internal::Benchmark* benchmark) {
    benchmark->Args({1024, 1, 64})->Args({16384, 1, 64})->Args({16384, 8, 64})->Args({65536, 8, 64});
}

void ChurnArgs(benchmark::internal::Benchmark* benchmark) {
    benchmark->Arg(1024)->Arg(16384)->Arg(65536);
}

void AddClientArgs(benchmark::internal::Benchmark* benchmark) {
    benchmark->Args({1024, 8})->Args({16384, 8})->Args({65536, 8});
}

void ProcessAckArgs(benchmark::internal::Benchmark* benchmark) {
    benchmark->Arg(128)->Arg(1024)->Arg(4096);
}

void InputFrameArgs(benchmark::internal::Benchmark* benchmark) {
    benchmark->Arg(16)->Arg(64);
}

void PendingDestroyArgs(benchmark::internal::Benchmark* benchmark) {
    benchmark->Args({1024, 64})->Args({16384, 64})->Args({65536, 64});
}

void QuantizedBytesArgs(benchmark::internal::Benchmark* benchmark) {
    benchmark->Arg(8)->Arg(16)->Arg(17)->Arg(32)->Arg(64)->Arg(128)->Arg(1200);
}

BENCHMARK(BM_ServerTickFullBudget)->Apply(TickArgs);
BENCHMARK(BM_ServerTickBudgetLimited)->Apply(LimitedTickArgs);
BENCHMARK(BM_ServerRefreshReplicatedChanges)->Apply(ChurnArgs);
BENCHMARK(BM_ServerAddClientsAfterReplicated)->Apply(AddClientArgs);
BENCHMARK(BM_ServerTickSerializedFullBudget)->Apply(TickArgs);
#ifdef ASHIATO_SYNC_ENABLE_TRACING
BENCHMARK(BM_ServerTickTracingRuntimeDisabled)->Apply(TickArgs);
BENCHMARK(BM_ServerTickTracingCallbacks)->Apply(TickArgs);
BENCHMARK(BM_ServerTickTracingFrameData)->Apply(TickArgs);
#endif
BENCHMARK(BM_ServerTickSerializedDelta)->Apply(TickArgs);
BENCHMARK(BM_ServerTickSerializedBudgetLimited)->Apply(LimitedTickArgs);
BENCHMARK(BM_ServerTickSerializedBudgetLimitedBounded)->Apply(LimitedTickArgs);
BENCHMARK(BM_ServerTickPackedFullBudget)->Apply(TickArgs);
BENCHMARK(BM_ServerTickPackedAckedDeltaShared)->Apply(TickArgs);
BENCHMARK(BM_ServerTickPackedMtuLimited)->Apply(TickArgs);
BENCHMARK(BM_ServerProcessAckPacket)->Apply(ProcessAckArgs);
BENCHMARK(BM_ServerTickDestroyBurst)->Apply(DestroyArgs);
BENCHMARK(BM_ServerTickPendingDestroysBudgetLimited)->Apply(PendingDestroyArgs);
BENCHMARK(BM_ServerTickMutatingAckedDelta)->Apply(TickArgs);
BENCHMARK(BM_ServerTickOwnerAudienceMixed)->Apply(TickArgs);
BENCHMARK(BM_ServerTickTaggedOwnerMixed)->Args({1024, 4, 8})->Args({16384, 4, 16});
BENCHMARK(BM_ServerTickArchetypeDiversity)->Apply(TickArgs);
BENCHMARK(BM_ServerTickStressScheduler)->Args({4096, 4});
BENCHMARK(BM_ServerProcessInputPacket)->Apply(InputFrameArgs);
BENCHMARK(BM_ServerTickInputUpsert)->Arg(1024)->Arg(16384);
BENCHMARK(BM_ServerTickCappedLargeDelta)->Args({1024, 64, 4})->Args({16384, 64, 4});
BENCHMARK(BM_BitBufferUnalignedBytes)->Arg(64)->Arg(1024)->Arg(16384);
BENCHMARK(BM_BitBufferUnalignedReadUnsigned)->Arg(1024)->Arg(16384);
BENCHMARK(BM_BitBufferAppendBits)->Arg(512)->Arg(8192)->Arg(131072);
BENCHMARK(BM_QuantizedBytesAssign)->Apply(QuantizedBytesArgs);
BENCHMARK(BM_QuantizedBytesCopy)->Apply(QuantizedBytesArgs);

}  // namespace
