# Observability

rax-server sets up OpenTelemetry the way AX's ax-server does and needs no
sidecar. It exports traces, metrics and events over OTLP/gRPC to a collector
you choose, and serves metrics for Prometheus as well. The settings,
defaults, ports and attribute names are AX's, so AX's manifests and its
`ax-otel-config` ConfigMap configure rax-server unchanged.

| Signal | Where it goes | Default in the code | Default in AX's `deploy/ax-server.yaml` |
|---|---|---|---|
| Logs | stdout, one JSON object per line | Always on | Always on |
| Metrics | OTLP push, and Prometheus `/metrics` on port 9090 (`--metrics-addr`) | Both on | `/metrics` only |
| Traces | OTLP push; 10% of new traces sampled | On | Off |
| Events | stdout as JSON log lines, or OTLP log records | stdout | stdout |

Health probes and the AX API stay on port 8080. AX's manifests expose port
9090 as `metrics` on the `ax-server` Service, so a Prometheus scrape config
can target it. `--metrics-addr=` turns `/metrics` off; like AX, the flag
takes Go's host-less form, `:9090`.

## Connect a collector

AX's manifests read every `OTEL_*` setting from the `ax-otel-config`
ConfigMap in `ax-system`, and set `OTEL_RESOURCE_ATTRIBUTES` (`k8s.*` and
`service.instance.id` from the downward API) in the Deployment. The
production manifest ships with OTLP export off; its E2E manifest, which
`just deploy` renders, points at the collector Substrate's kind profile
installs and sends events both over OTLP and to stdout. To export, edit the
ConfigMap and restart the server:

```yaml
data:
  OTEL_EXPORTER_OTLP_ENDPOINT: http://otel-collector.observability.svc:4317
  OTEL_LOGS_EXPORTER: otlp        # also send events; add ,console to keep them on stdout
  # delete OTEL_TRACES_EXPORTER: none and OTEL_METRICS_EXPORTER: prometheus
```

```bash
kubectl -n ax-system rollout restart deployment/ax-server
```

## Configuration

Telemetry follows the standard
[OpenTelemetry environment variables](https://opentelemetry.io/docs/specs/otel/configuration/sdk-environment-variables/).
rax-server warns about a value it does not recognize and ignores it, which
keeps the default below, as AX does.

| Variable | Effect |
|---|---|
| `OTEL_EXPORTER_OTLP_ENDPOINT` | Collector address, for example `http://otel-collector.observability.svc:4317`. The SDK default is `http://localhost:4317`. The endpoint must be `http://`, reached without TLS: rax-server does not export over TLS yet. |
| `OTEL_TRACES_EXPORTER` | `otlp` (default) or `none`. With `none`, traces are still sampled and propagated, only not sent. |
| `OTEL_METRICS_EXPORTER` | Controls only the OTLP push: `otlp` (default) turns it on; `prometheus` or `none` turns it off. `/metrics` is controlled by `--metrics-addr`. |
| `OTEL_LOGS_EXPORTER` | Where events go: `none` (default) writes them to stdout; `otlp` sends them to the collector instead; `otlp,console` does both. |
| `OTEL_METRIC_EXPORT_INTERVAL` | Milliseconds between metric pushes; the SDK default is 60000. |
| `OTEL_TRACES_SAMPLER`, `OTEL_TRACES_SAMPLER_ARG` | Replace the default sampler, `parentbased_traceidratio` at `0.1`. An unknown sampler, or a ratio sampler whose argument is missing or outside [0, 1], keeps the default. |
| `OTEL_SERVICE_NAME`, `OTEL_RESOURCE_ATTRIBUTES` | Override the resource. The code defaults are `service.name=rax-server` and `service.instance.id` set to the hostname. |
| `RUST_LOG` | Which log lines are written; the default is `info`, with the PostgreSQL client and the OpenTelemetry SDK at `warn`. |

## Logs

rax-server writes one JSON object per line to stdout, with the keys Go's
`slog` uses in AX: `time`, `level` (`DEBUG`, `INFO`, `WARN`, `ERROR`), `msg`,
then the fields. A line logged while a trace is active carries `trace_id`,
`span_id` and `trace_flags`, so a log backend can link it to the trace. Log
lines are not sent over OTLP; collect them from stdout as for any pod. The
OpenTelemetry SDK's own warnings and errors are log lines too.

## Events

Structured events, such as task state changes, use the OTLP logs signal:
each is a log record with an event name. With `OTEL_LOGS_EXPORTER` unset or
`none`, an event is written to stdout as a JSON log line instead, with the
event name in `event`, so it is never dropped because OTLP is off. OTLP
event records are batched, so a slow collector never delays the operation
that emits them.

As in AX, nothing records spans or emits events yet: this is the setup that
request tracing and task state events build on.

## Metrics

Each instrument keeps at most 2000 attribute sets per collection (the SDK
default); further sets are folded into one overflow series. `/metrics` also
serves the process metrics (`process_*`); they are not pushed over OTLP.

## CLI traces

`rax --trace <command>` traces one command, as `ax --trace` does. The CLI
prints the trace ID to stderr, and every AX RPC of the command sends a
sampled W3C `traceparent` (never baggage). Each RPC is a client span under
the command span, named `ax.v1alpha1.AX/<Method>`, and resolving the server
address, which can start a port-forward, is a child span too. Without
`--trace`, the CLI creates no tracer and sends no trace context, so the
server's own sampler decides for CLI traffic.

The CLI exports its spans over OTLP/gRPC only when
`OTEL_EXPORTER_OTLP_ENDPOINT` or `OTEL_EXPORTER_OTLP_TRACES_ENDPOINT` is set
in your shell. This is unlike rax-server, which exports by default and uses
the SDK default `localhost:4317` when no endpoint is set. The spans have
`service.name=rax`. `OTEL_TRACES_EXPORTER`, `OTEL_SERVICE_NAME` and
`OTEL_RESOURCE_ATTRIBUTES` follow the same rules as for rax-server; both
take them from the `rax-otelenv` crate.

The CLI exports once, when the command ends, and waits at most 2 seconds
(200 ms when Ctrl-C pressed twice leaves `rax ssh`). It does not retry. If
the export fails, the CLI prints a warning that names the endpoint and gives
the error.

Like ax-server, rax-server does not read `traceparent` yet, so for now the
trace has only the CLI spans.

## Attribute names

Spans, events and log attributes use the names in
[`crates/rax-server/src/axattr.rs`](../crates/rax-server/src/axattr.rs), which
are AX's: `ax.task.*`, and Agent Substrate's `ate.atespace` and
`ate.actor.uid` for actor identity, so RAX, AX and Substrate records join on
them. Identity attributes never appear on metrics.
