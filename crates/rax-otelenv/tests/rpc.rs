//! Server and client spans of gRPC calls, as AX's handler test checks them.

use std::sync::{Arc, Mutex};
use std::time::Duration;

use opentelemetry::trace::SpanKind;
use opentelemetry::{Context, global};
use opentelemetry_sdk::error::OTelSdkResult;
use opentelemetry_sdk::trace::{Sampler, SdkTracerProvider, Span, SpanData, SpanProcessor};
use rax_otelenv::rpc::{client_call, server_call};
use tonic::metadata::MetadataMap;

#[derive(Debug, Clone, Default)]
struct Recorder(Arc<Mutex<Vec<SpanData>>>);

impl SpanProcessor for Recorder {
    fn on_start(&self, _: &mut Span, _: &Context) {}
    fn on_end(&self, span: SpanData) {
        self.0.lock().unwrap().push(span);
    }
    fn force_flush(&self) -> OTelSdkResult {
        Ok(())
    }
    fn shutdown_with_timeout(&self, _: Duration) -> OTelSdkResult {
        Ok(())
    }
}

#[tokio::test]
async fn traces_sampled_callers_only() {
    let spans = Recorder::default();
    global::set_tracer_provider(
        SdkTracerProvider::builder()
            .with_sampler(Sampler::ParentBased(Box::new(Sampler::AlwaysOff)))
            .with_span_processor(spans.clone())
            .build(),
    );
    const TRACE_ID: &str = "4bf92f3577b34da6a3ce929d0e0e4736";
    const SPAN_ID: &str = "00f067aa0ba902b7";
    // A sampled caller is traced although new traces are never sampled here;
    // an unsampled one is not.
    for flags in ["01", "00"] {
        let mut md = MetadataMap::new();
        md.insert(
            "traceparent",
            format!("00-{TRACE_ID}-{SPAN_ID}-{flags}").parse().unwrap(),
        );
        let sent = server_call("ax.v1alpha1.AX/ListTasks", &md, async {
            // A call made while serving is a child of the server span and
            // carries its trace onward.
            client_call(
                "substrate.ateapi.v1.Control/ListActors",
                tonic::Request::new(()),
                |r| async move {
                    let tp = r
                        .metadata()
                        .get("traceparent")
                        .map(|v| v.to_str().unwrap().to_owned());
                    Ok::<_, tonic::Status>(tp)
                },
            )
            .await
        })
        .await
        .unwrap();
        // The trace goes on either way, sampled or not as the caller's.
        let sent = sent.unwrap();
        assert!(
            sent.contains(TRACE_ID) && sent.ends_with(&format!("-{flags}")),
            "{sent}"
        );
    }
    let ended = spans.0.lock().unwrap().clone();
    assert_eq!(ended.len(), 2, "{ended:?}");
    let (client, server) = (&ended[0], &ended[1]);
    assert_eq!(
        (server.name.as_ref(), &server.span_kind),
        ("ax.v1alpha1.AX/ListTasks", &SpanKind::Server)
    );
    assert_eq!(server.span_context.trace_id().to_string(), TRACE_ID);
    assert_eq!(server.parent_span_id.to_string(), SPAN_ID);
    assert_eq!(
        (client.name.as_ref(), &client.span_kind),
        ("substrate.ateapi.v1.Control/ListActors", &SpanKind::Client)
    );
    assert_eq!(client.parent_span_id, server.span_context.span_id());
}
