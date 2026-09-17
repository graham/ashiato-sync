ashiato_sync_fetch_catch2()

add_executable(ashiato_sync_tests
    ball_stress.test.cpp
    bit_buffer.test.cpp
    client_ack_queue.test.cpp
    client_buffered_interpolation.test.cpp
    client_clock.test.cpp
    client_cues.test.cpp
    client_display.test.cpp
    client_frame_ring_store.test.cpp
    client_frame_data.test.cpp
    client_input.test.cpp
    client_timing_stats.test.cpp
    client_prediction.test.cpp
    client_receive.test.cpp
    client_timing.test.cpp
    client_transport.test.cpp
    components.test.cpp
    display.test.cpp
    header_api.test.cpp
    integration_smoke.test.cpp
    options_validation.test.cpp
    quantized_bytes.test.cpp
    replay_network_correctness.test.cpp
    replay_streamer.test.cpp
    replay_writer.test.cpp
    server_connection.test.cpp
    server_bandwidth_controller.test.cpp
    server_budget_serialization.test.cpp
    server_destroy_ack.test.cpp
    server_input_buffer.test.cpp
    server_input.test.cpp
    server_lifecycle.test.cpp
    server_logging.test.cpp
    server_packet_ack_tracker.test.cpp
    server_packet.test.cpp
    server_prioritization.test.cpp
    server_transport.test.cpp
    serialization_context.test.cpp
    tracing_component_rows.test.cpp
    tracing_events.test.cpp
    tracing_packet_log.test.cpp
    tracing_reader.test.cpp
)

if(ASHIATO_SYNC_ENABLE_PROFILING)
    target_sources(ashiato_sync_tests PRIVATE profiling.test.cpp)
endif()

target_link_libraries(ashiato_sync_tests
    PRIVATE
        ashiato::sync
        Catch2::Catch2WithMain
)

target_include_directories(ashiato_sync_tests
    PRIVATE
        ${PROJECT_SOURCE_DIR}/src
)

if(WIN32)
    target_link_libraries(ashiato_sync_tests PRIVATE ws2_32)
endif()

if(ASHIATO_SYNC_ENABLE_COVERAGE)
    ashiato_sync_enable_coverage(ashiato_sync_tests)
endif()

include(Catch)
catch_discover_tests(ashiato_sync_tests)
