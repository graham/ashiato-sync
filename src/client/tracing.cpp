#include "client/tracing.hpp"

#include "client/runtime/cue_runtime.hpp"
#include "client/store/entity_store.hpp"
#include "client/runtime/prediction_runtime.hpp"
#include "client/state.hpp"

#include <limits>
#include <sstream>
#include <string>
#include <utility>
#include <vector>

namespace ashiato::sync {

#ifdef ASHIATO_SYNC_ENABLE_TRACING
namespace {

thread_local const RollbackReasonTraceContext* rollback_reason_trace_context = nullptr;

void emit_rollback_reason(const std::string& reason) {
    const RollbackReasonTraceContext* context = rollback_reason_trace_context;
    if (context == nullptr || context->tracer == nullptr || !context->tracer->enabled() || reason.empty()) {
        return;
    }
    SyncTraceEvent event = make_client_trace_event(SyncTraceEventType::RollbackReason, context->client, context->frame);
    event.server_entity = context->server_entity;
    event.local_entity = context->local_entity;
    event.client_network_id = context->client_network_id;
    event.wire_network_id = context->wire_network_id;
    event.network_version = context->network_version;
    event.archetype = context->archetype;
    event.component = context->component;
    event.component_name = context->component_name;
    event.data = reason;
    context->tracer->trace(event);
}

}  // namespace

SyncTraceEvent make_client_trace_event(SyncTraceEventType type, ClientId client, SyncFrame frame) {
    SyncTraceEvent event;
    event.type = type;
    event.role = SyncTraceRole::Client;
    event.client = client;
    event.frame = frame;
    return event;
}

ScopedRollbackReasonTraceContext::ScopedRollbackReasonTraceContext(const RollbackReasonTraceContext& context)
    : previous_(rollback_reason_trace_context) {
    rollback_reason_trace_context = &context;
}

ScopedRollbackReasonTraceContext::~ScopedRollbackReasonTraceContext() {
    rollback_reason_trace_context = previous_;
}

void trace_rollback_reason(const char* reason) {
    emit_rollback_reason(reason != nullptr ? std::string(reason) : std::string{});
}

void trace_rollback_reason(const std::string& reason) {
    emit_rollback_reason(reason);
}

void append_trace_component_data(
    const SyncTracer* tracer,
    const SyncArchetype& archetype,
    std::size_t component_index,
    const std::uint8_t* bytes,
    SyncTraceEvent& event) {
    if (component_index < archetype.component_ops.size()) {
        event.component_name = archetype.component_ops[component_index].serialization.name;
    }
#ifdef ASHIATO_SYNC_TRACE_COMPONENT_DATA
    if (tracer == nullptr || !tracer->frame_data_enabled() || bytes == nullptr ||
        component_index >= archetype.component_ops.size()) {
        return;
    }
    const SyncComponentOps& ops = archetype.component_ops[component_index];
    if (ops.trace == nullptr) {
        return;
    }
    SyncTraceStringBuilder builder;
    ops.trace(bytes, builder);
    event.data = std::move(builder.value);
#else
    (void)tracer;
    (void)archetype;
    (void)component_index;
    (void)bytes;
    (void)event;
#endif
}

void append_trace_input_component_data(
    const SyncTracer* tracer,
    const SyncComponentOps& ops,
    const std::uint8_t* bytes,
    SyncTraceEvent& event) {
    event.component_name = ops.serialization.name;
#ifdef ASHIATO_SYNC_TRACE_COMPONENT_DATA
    if (tracer == nullptr || !tracer->frame_data_enabled() || bytes == nullptr || ops.trace == nullptr) {
        return;
    }
    SyncTraceStringBuilder builder;
    ops.trace(bytes, builder);
    event.data = std::move(builder.value);
#else
    (void)tracer;
    (void)ops;
    (void)bytes;
    (void)event;
#endif
}

void append_trace_component_name(
    const SyncArchetype& archetype,
    std::size_t component_index,
    SyncTraceEvent& event) {
    if (component_index < archetype.component_ops.size()) {
        event.component_name = archetype.component_ops[component_index].serialization.name;
    }
}

void append_trace_cue_data(
    const SyncTracer* tracer,
    const SyncSettings& settings,
    SyncCueTypeId cue_type,
    const ashiato::BitBuffer& payload,
    SyncTraceEvent& event) {
#ifdef ASHIATO_SYNC_TRACE_COMPONENT_DATA
    if (tracer == nullptr || !tracer->frame_data_enabled() ||
        cue_type >= settings.cue_ops.size() || settings.cue_ops[cue_type].trace == nullptr) {
        return;
    }
    SyncTraceStringBuilder builder;
    if (settings.cue_ops[cue_type].trace(cue_type, settings.cue_ops[cue_type].user_data, payload, builder)) {
        event.data = std::move(builder.value);
    }
#else
    (void)tracer;
    (void)settings;
    (void)cue_type;
    (void)payload;
    (void)event;
#endif
}

void append_trace_cue_value(
    const SyncTracer* tracer,
    const SyncSettings& settings,
    SyncCueTypeId cue_type,
    const CueValue& value,
    SyncTraceEvent& event) {
#ifdef ASHIATO_SYNC_TRACE_COMPONENT_DATA
    if (tracer == nullptr || !tracer->frame_data_enabled() ||
        cue_type >= settings.cue_ops.size() || settings.cue_ops[cue_type].trace_value == nullptr) {
        return;
    }
    SyncTraceStringBuilder builder;
    if (settings.cue_ops[cue_type].trace_value(cue_type, settings.cue_ops[cue_type].user_data, value.data(), builder)) {
        event.data = std::move(builder.value);
    }
#else
    (void)tracer;
    (void)settings;
    (void)cue_type;
    (void)value;
    (void)event;
#endif
}

void append_trace_data_field(SyncTraceEvent& event, const char* key, const char* value) {
    if (key == nullptr || value == nullptr || value[0] == '\0') {
        return;
    }
    if (!event.data.empty()) {
        event.data += ",";
    }
    event.data += key;
    event.data += "=";
    event.data += value;
}

void append_trace_cue_name(const SyncSettings& settings, SyncCueTypeId cue_type, SyncTraceEvent& event) {
    if (cue_type < settings.cue_ops.size()) {
        event.component_name = settings.cue_ops[cue_type].name;
    }
}

#ifdef ASHIATO_SYNC_TRACE_PACKET_LOGS
std::string packet_ack_list(const std::vector<std::uint32_t>& acks) {
    std::ostringstream out;
    out << "[";
    for (std::size_t index = 0; index < acks.size(); ++index) {
        if (index != 0U) {
            out << ",";
        }
        out << acks[index];
    }
    out << "]";
    return out.str();
}
#endif

void ReplicationClient::set_tracer(SyncTracer* tracer) noexcept {
    trace_writer_.reset();
    tracer_ = tracer;
}

void ReplicationClient::set_trace_options(TraceOptions options) {
    options_.trace = std::move(options);
    trace_writer_ = make_trace_writer(options_.trace);
    tracer_ = trace_writer_ != nullptr ? &trace_writer_->tracer() : nullptr;
}

void ReplicationClient::flush_trace() {
    if (trace_writer_ != nullptr) {
        trace_writer_->flush();
    }
}

void ReplicationClient::close_trace() {
    if (trace_writer_ != nullptr) {
        trace_writer_->close();
    }
}

void ReplicationClient::trace_frame_components(
    const ashiato::Registry& registry,
    const SyncSettings& settings,
    SyncFrame frame,
    bool resimulated,
    bool only_pending_rollback,
    TraceFrameComponentScope scope) {
    if (tracer_ == nullptr || !tracer_->enabled() || !tracer_->frame_data_enabled()) {
        return;
    }
    for (const EntityState& state : entity_store_->all_entity_states()) {
        if (state.identity.client_entity_network_id == invalid_client_entity_network_id ||
            !state.identity.local || !registry.alive(state.identity.local) ||
            state.identity.archetype.value >= settings.archetypes.size()) {
            continue;
        }
        if (only_pending_rollback && !state.prediction.rollback_pending) {
            continue;
        }
        if (scope == TraceFrameComponentScope::Predicted && state.mode.current != ReplicationClientMode::Predict) {
            continue;
        }
        if (scope == TraceFrameComponentScope::NonPredicted && state.mode.current == ReplicationClientMode::Predict) {
            continue;
        }
        const SyncArchetype& archetype = settings.archetypes[state.identity.archetype.value];
        for (std::size_t component_index = 0; component_index < archetype.components.size(); ++component_index) {
            const ComponentReplication& replication = archetype.components[component_index];
            const void* value = registry.get(state.identity.local, replication.component);
            if (value == nullptr || component_index >= archetype.component_ops.size()) {
                continue;
            }
            const SyncComponentOps& ops = archetype.component_ops[component_index];
            if (ops.serialization.quantize == nullptr) {
                continue;
            }
            SyncComponentOps::QuantizedBytes bytes;
            bytes.resize(ops.serialization.quantized_size);
            ops.serialization.quantize(value, bytes.data());
            SyncTraceEvent event = make_client_trace_event(
                resimulated ? SyncTraceEventType::ResimulatedFrameComponent : SyncTraceEventType::FrameComponent,
                client_id_,
                frame);
            event.local_entity = state.identity.local;
            event.server_entity = ashiato::Entity{state.identity.client_entity_network_id};
            event.client_network_id = state.identity.client_entity_network_id;
            event.wire_network_id = state.identity.wire_network_id;
            event.network_version = client_entity_network_id_version(state.identity.client_entity_network_id);
            event.archetype = state.identity.archetype;
            event.component = replication.component;
            event.mode = state.mode.current;
            append_trace_component_data(tracer_, archetype, component_index, bytes.data(), event);
            tracer_->trace(event);
        }
    }
}

void ReplicationClient::trace_input_components(
    ashiato::Registry& registry,
    const SyncSettings& settings,
    SyncFrame frame,
    ashiato::Entity component,
    const std::uint8_t* quantized) {
    if (tracer_ == nullptr || !tracer_->enabled() || !tracer_->frame_data_enabled() ||
        settings.local_client == invalid_client_id || quantized == nullptr || !component) {
        return;
    }
    const auto found_ops = settings.component_ops.find(component.value);
    if (found_ops == settings.component_ops.end()) {
        return;
    }
    const SyncComponentOps& ops = found_ops->second;
    registry.view<const NetworkOwner>().each([&](ashiato::Entity entity, const NetworkOwner& owner) {
        if (owner.client != settings.local_client) {
            return;
        }
        SyncTraceEvent event = make_client_trace_event(SyncTraceEventType::FrameComponent, client_id_, frame);
        event.local_entity = entity;
        const EntityState* state = find_entity_state_for_local(entity);
        if (state != nullptr && state->identity.client_entity_network_id != invalid_client_entity_network_id) {
            event.server_entity = ashiato::Entity{state->identity.client_entity_network_id};
            event.client_network_id = state->identity.client_entity_network_id;
            event.wire_network_id = state->identity.wire_network_id;
            event.network_version = client_entity_network_id_version(state->identity.client_entity_network_id);
            event.archetype = state->identity.archetype;
            event.mode = state->mode.current;
        }
        event.component = component;
        append_trace_input_component_data(tracer_, ops, quantized, event);
        tracer_->trace(event);
    });
}

void ReplicationClient::trace_clock_skew(
    const char* stage,
    SyncFrame local_frame,
    SyncFrame server_frame,
    double observed_server_frame,
    SyncFrame observed_buffered_frame,
    SyncFrame last_recorded_input_frame,
    SyncFrame prefill_input_frame) const {
    if (tracer_ == nullptr || !tracer_->enabled()) {
        return;
    }
    const ReplicationClientTimingStats& timing = clock_.stats();
    const double observed_downstream = observed_server_frame > static_cast<double>(server_frame)
        ? observed_server_frame - static_cast<double>(server_frame)
        : 0.0;
    const SyncFrame measured_prediction =
        last_recorded_input_frame >= server_frame ? last_recorded_input_frame - server_frame : 0U;
    const SyncFrame predicted_frame = clock_.predicted_frame();
    const SyncFrame predicted_lead = predicted_frame >= server_frame ? predicted_frame - server_frame : 0U;

    SyncTraceEvent event = make_client_trace_event(SyncTraceEventType::ClockSkew, client_id_, local_frame);
    event.local_entity = ashiato::Entity{client_id_ == invalid_client_id ? 0U : client_id_};
    event.component = ashiato::Entity{std::numeric_limits<std::uint64_t>::max() - 1U};
    event.component_name = "Clock";
    std::ostringstream out;
    out << "stage=" << stage
        << ",server_frame=" << server_frame
        << ",estimated_server_frame=" << observed_server_frame
        << ",buffered_frame=" << observed_buffered_frame
        << ",predicted_frame=" << predicted_frame
        << ",last_recorded_input=" << last_recorded_input_frame
        << ",last_predicted=" << prediction_->last_predicted_frame()
        << ",observed_downstream=" << observed_downstream
        << ",measured_prediction=" << measured_prediction
        << ",predicted_lead=" << predicted_lead
        << ",prediction_current=" << timing.current_prediction_lead_frames
        << ",prediction_target=" << timing.target_prediction_lead_frames
        << ",prediction_desired=" << timing.desired_prediction_lead_frames
        << ",prediction_measured=" << timing.measured_prediction_lead_frames
        << ",predicted_dilation=" << timing.predicted_time_dilation
        << ",buffered_lag_current=" << timing.current_buffered_frame_lag
        << ",buffered_lag_target=" << timing.target_buffered_frame_lag
        << ",buffered_lag_desired=" << timing.desired_buffered_frame_lag
        << ",buffered_lag_measured=" << timing.measured_buffered_frame_lag
        << ",buffered_dilation=" << timing.buffered_time_dilation
        << ",prefill_input=" << prefill_input_frame
        << ",active_snap_lead=" << prediction_->active_snap_lead()
        << ",pending_catchup=" << prediction_->pending_catchup_frame()
        << ",pending_catchup_server=" << prediction_->pending_catchup_server_frame()
        << ",has_predicted_entities=" << (has_predicted_entities() ? 1 : 0)
        << ",has_predicted_frame=" << (prediction_->has_predicted_frame() ? 1 : 0);
    event.data = out.str();
    tracer_->trace(event);
}

void ReplicationClient::trace_cue_event(
    SyncTraceEventType type,
    const SyncSettings& settings,
    const EntityState& state,
    const EntityCue& cue,
    const char* rollback_reason,
    const char* cue_source) const {
    if (tracer_ == nullptr || !tracer_->enabled()) {
        return;
    }
    SyncTraceEvent event = make_client_trace_event(type, client_id_, cue.frame);
    event.server_entity = ashiato::Entity{state.identity.client_entity_network_id};
    event.local_entity = state.identity.local;
    event.client_network_id = state.identity.client_entity_network_id;
    event.wire_network_id = state.identity.wire_network_id;
    event.network_version = client_entity_network_id_version(state.identity.client_entity_network_id);
    event.archetype = state.identity.archetype;
    event.cue_type = cue.type;
    append_trace_cue_name(settings, cue.type, event);
    append_trace_cue_value(tracer_, settings, cue.type, cue.value, event);
    append_trace_data_field(event, "source", cue_source);
    append_trace_data_field(event, "rollback_reason", rollback_reason);
    tracer_->trace(event);
}

void ReplicationClient::trace_cue_event(
    SyncTraceEventType type,
    const SyncSettings& settings,
    const EntityState& state,
    const EntityPlayedCue& cue,
    const char* rollback_reason,
    const char* cue_source) const {
    if (tracer_ == nullptr || !tracer_->enabled()) {
        return;
    }
    SyncTraceEvent event = make_client_trace_event(type, client_id_, cue.frame);
    event.server_entity = ashiato::Entity{state.identity.client_entity_network_id};
    event.local_entity = state.identity.local;
    event.client_network_id = state.identity.client_entity_network_id;
    event.wire_network_id = state.identity.wire_network_id;
    event.network_version = client_entity_network_id_version(state.identity.client_entity_network_id);
    event.archetype = state.identity.archetype;
    event.cue_type = cue.type;
    append_trace_cue_name(settings, cue.type, event);
    append_trace_cue_value(tracer_, settings, cue.type, cue.value, event);
    append_trace_data_field(event, "source", cue_source);
    append_trace_data_field(event, "rollback_reason", rollback_reason);
    tracer_->trace(event);
}

#ifdef ASHIATO_SYNC_TRACE_PACKET_LOGS
void ReplicationClient::trace_outgoing_ack_packet(const std::vector<std::uint32_t>& acks) const {
    if (tracer_ == nullptr || !tracer_->enabled() || !tracer_->packet_logs_enabled()) {
        return;
    }
    SyncTraceEvent event = make_client_trace_event(SyncTraceEventType::PacketLog, client_id_, clock_.predicted_frame());
    event.data = "direction=out,message=client_ack,acks=" + packet_ack_list(acks);
    tracer_->trace(event);
}

void ReplicationClient::trace_outgoing_ping_packet(std::uint32_t sequence) const {
    if (tracer_ == nullptr || !tracer_->enabled() || !tracer_->packet_logs_enabled()) {
        return;
    }
    SyncTraceEvent event = make_client_trace_event(SyncTraceEventType::PacketLog, client_id_, clock_.predicted_frame());
    event.data = "direction=out,message=client_ping,sequence=" + std::to_string(sequence);
    tracer_->trace(event);
}

void ReplicationClient::trace_incoming_pong_packet(
    std::uint32_t sequence,
    SyncFrame local_frame,
    SyncFrame server_receive_frame,
    SyncFrame server_send_frame) const {
    if (tracer_ == nullptr || !tracer_->enabled() || !tracer_->packet_logs_enabled()) {
        return;
    }
    SyncTraceEvent event = make_client_trace_event(SyncTraceEventType::PacketLog, client_id_, local_frame);
    std::ostringstream out;
    out << "direction=in,message=server_pong,sequence=" << sequence
        << ",server_receive_frame=" << server_receive_frame
        << ",server_frame=" << server_send_frame;
    event.data = out.str();
    tracer_->trace(event);
}

void ReplicationClient::trace_outgoing_input_packet(
    const std::vector<std::uint32_t>& acks,
    SyncFrame baseline_frame,
    SyncFrame first_input_frame,
    SyncFrame last_input_frame) const {
    if (tracer_ == nullptr || !tracer_->enabled() || !tracer_->packet_logs_enabled()) {
        return;
    }
    SyncTraceEvent event = make_client_trace_event(SyncTraceEventType::PacketLog, client_id_, clock_.predicted_frame());
    std::ostringstream out;
    out << "direction=out,message=client_input,acks=" << packet_ack_list(acks)
        << ",input_frames=";
    if (first_input_frame != 0U && last_input_frame >= first_input_frame) {
        out << first_input_frame << "-" << last_input_frame;
    } else {
        out << "none";
    }
    out << ",baseline=" << baseline_frame;
    event.data = out.str();
    tracer_->trace(event);
}

void ReplicationClient::trace_incoming_update_packet(
    SyncFrame local_frame,
    SyncFrame server_frame,
    std::uint32_t packet_id,
    SyncFrame input_ack_frame,
    std::uint16_t record_count,
    bool applied,
    const char* apply_failure_reason) const {
    if (tracer_ == nullptr || !tracer_->enabled() || !tracer_->packet_logs_enabled()) {
        return;
    }
    SyncTraceEvent event = make_client_trace_event(SyncTraceEventType::PacketLog, client_id_, local_frame);
    std::ostringstream out;
    out << "direction=in,message=server_update,client=" << static_cast<unsigned>(client_id_)
        << ",sequence=" << packet_id
        << ",server_frame=" << server_frame
        << ",input_ack=" << input_ack_frame
        << ",record_count=" << record_count
        << ",applied=" << (applied ? "true" : "false");
    if (!applied && apply_failure_reason != nullptr && apply_failure_reason[0] != '\0') {
        out << ",apply_failure=" << apply_failure_reason;
    }
    out << ",cues=[";
    const std::vector<std::string>& cue_summaries = cue_runtime_->current_packet_cue_summaries();
    for (std::size_t index = 0; index < cue_summaries.size(); ++index) {
        if (index != 0U) {
            out << ";";
        }
        out << cue_summaries[index];
    }
    out << "]";
    event.data = out.str();
    tracer_->trace(event);
}
#endif
#endif

}  // namespace ashiato::sync
