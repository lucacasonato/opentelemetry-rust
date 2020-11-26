//! OpenTelemetry GCP Propagators
use opentelemetry::{
    propagation::{text_map_propagator::FieldIter, Extractor, Injector, TextMapPropagator},
    trace::{
        SpanContext, SpanId, TraceContextExt, TraceId, TraceState, TRACE_FLAG_DEFERRED,
        TRACE_FLAG_NOT_SAMPLED, TRACE_FLAG_SAMPLED,
    },
    Context,
};

const GCP_CLOUD_TRACE_HEADER: &str = "x-cloud-trace-context";

lazy_static::lazy_static! {
    static ref GCP_CLOUD_TRACE_HEADER_FIELD: [String; 1] = [GCP_CLOUD_TRACE_HEADER.to_string()];
}

/// Extracts and injects `SpanContext`s into `Extractor`s or `Injector`s using GCP Cloud
/// Trace header format.
///
/// Extracts and injects values to/from the `x-cloud-trace-context` header. Converting
/// between OpenTelemetry [SpanContext][otel-spec] and [Cloud Trace format][cloud-trace-id].
///
/// ## Example
///
/// ```
/// use opentelemetry::global;
/// use opentelemetry_contrib::trace::propagator::CloudTracePropagator;
///
/// global::set_text_map_propagator(CloudTracePropagator);
/// ```
///
/// [otel-spec]: https://github.com/open-telemetry/opentelemetry-specification/blob/master/specification/trace/api.md#SpanContext
/// [cloud-trace-id]: https://cloud.google.com/trace/docs/setup#force-trace
#[derive(Clone, Debug)]
pub struct CloudTracePropagator;

impl CloudTracePropagator {
    fn extract_span_context(&self, extractor: &dyn Extractor) -> Result<SpanContext, ()> {
        let header_value: &str = extractor.get(GCP_CLOUD_TRACE_HEADER).unwrap_or("").trim();

        let parts: Vec<&str> = header_value.splitn(2, "/").collect();
        let trace_id = TraceId::from_hex(parts.get(0).ok_or(())?);
        let rest = parts.get(1).ok_or(())?;
        let parts: Vec<&str> = rest.splitn(2, ";").collect();
        let parent_span_id: SpanId =
            SpanId::from_u64(parts.get(0).ok_or(())?.parse().map_err(|_| ())?);
        let rest = parts.get(1);
        let sampling_decision = match rest.cloned() {
            Some("o=0") => TRACE_FLAG_NOT_SAMPLED,
            Some("o=1") => TRACE_FLAG_SAMPLED,
            _ => TRACE_FLAG_DEFERRED,
        };

        if trace_id.to_u128() == 0 {
            return Err(());
        }

        let context: SpanContext = SpanContext::new(
            trace_id,
            parent_span_id,
            sampling_decision,
            true,
            TraceState::default(),
        );

        Ok(context)
    }
}

impl TextMapPropagator for CloudTracePropagator {
    fn inject_context(&self, cx: &Context, injector: &mut dyn Injector) {
        let span_context = cx.span().span_context();
        if span_context.is_valid() {
            let trace_id = span_context.trace_id().to_hex();
            let span_id = span_context.span_id().to_u64();

            let sampling_decision: &str = if span_context.is_deferred() {
                ""
            } else if span_context.is_sampled() {
                "o=1"
            } else {
                "o=0"
            };

            injector.set(
                GCP_CLOUD_TRACE_HEADER,
                format!("{}/{};{}", trace_id, span_id, sampling_decision),
            );
        }
    }

    fn extract_with_context(&self, cx: &Context, extractor: &dyn Extractor) -> Context {
        let extracted = self
            .extract_span_context(extractor)
            .unwrap_or_else(|_| SpanContext::empty_context());

        cx.with_remote_span_context(extracted)
    }

    fn fields(&self) -> FieldIter<'_> {
        FieldIter::new(GCP_CLOUD_TRACE_HEADER_FIELD.as_ref())
    }
}
