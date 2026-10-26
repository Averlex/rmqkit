# rmqkit

A small Go RabbitMQ client for confirmed publishing and handler-based consumption.
It provides typed configuration, bounded recovery, caller-controlled cancellation,
typed JSON clients, and acknowledgements after successful processing. Requires Go 1.24.2 or later.

```sh
go get github.com/Averlex/rmqkit
```

The current API intentionally breaks the initial prototype. See migration notes below.

## Usage

Define messages and interfaces in the application layer. They do not import
rmqkit or AMQP and have no required `Headers`/`Payload` fields:

```go
type Job struct {
    ID   string `json:"id"`
    Task string `json:"task"`
}

type JobBroker interface {
    Publish(context.Context, Job) error
    Subscribe(context.Context, func(context.Context, Job) error) error
}
```

Construct the transport and typed wrapper at the application boundary (`app.go`):

```go
cfg := rmqkit.DefaultConfig()
cfg.Username = os.Getenv("RMQ_USERNAME")
cfg.Password = os.Getenv("RMQ_PASSWORD")
cfg.Queue = "jobs"
cfg.Exchange = "events"
cfg.RoutingKey = "jobs.created"

transport, err := rmqkit.NewRabbitMQ(cfg, rmqkit.FullClient)
if err != nil {
    return err
}
defer func() {
    shutdownCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
    defer cancel()

    if err := transport.Close(shutdownCtx); err != nil {
        slog.Error("close RabbitMQ", "error", err)
    }
}()

if err := transport.Connect(ctx); err != nil {
    return err
}

var jobs JobBroker = rmqkit.Typed[Job](transport)
// Pass jobs to the service; only composition code imports rmqkit.
return runJobs(ctx, jobs)
```

The service uses its own interface and receives decoded application values:

```go
func runJobs(ctx context.Context, jobs JobBroker) error {
    if err := jobs.Publish(ctx, Job{ID: "application-id", Task: "demo"}); err != nil {
        return err
    }

    return jobs.Subscribe(ctx, func(ctx context.Context, job Job) error {
        // Log processing failures here if the application needs them reported.
        return processJob(ctx, job)
    })
}
```

`Client[T]` has ordinary typed methods that satisfy application-owned interfaces;
no generic methods or Go 1.27 features are needed. `Publish` JSON-encodes the
entire value, and `Subscribe` decodes the message body into `T`. Standard
`encoding/json` rules apply: pointers, maps and slices work, nil pointers become
`null`, and `[]byte` becomes base64 JSON rather than a raw payload. Unsupported
values fail before publishing with `ErrInvalidMessage` and the JSON cause.

`Subscribe` blocks. Run it in an application-owned goroutine if needed, cancel
its context from outside, and collect its returned error. Each call invokes its
handler sequentially. Multiple calls create independent competing consumers.
Malformed or incompatible JSON never reaches the handler; it is rejected using
`RequeueOnError` and consumption continues. Decoding failures are delivery
failures, not terminal subscription errors. Configure dead-letter policy if the
application needs to inspect rejected messages. rmqkit performs no logging.

Multiple `Typed[T]` wrappers can share one transport without adding connections.
They share its queue/exchange binding, lifetime and publisher serialization.
Different message types on the same queue do not provide type-based routing:
consumers compete for messages regardless of `T`. Use compatible schemas or
separate configured transports for different queue bindings. Close the shared
transport once at the application boundary; wrappers do not own it.

### Transport metadata and raw messages

The existing `RabbitMQ.Produce(ctx, Message)` and `Consume(ctx, Handler)` API is
retained for applications that need raw bodies, AMQP headers or properties.
`Message` aliases `amqp.Publishing`; `Delivery` also includes routing information
and the redelivery flag but no manual acknowledgement methods. Keep this API at
transport boundaries when business layers must remain dependency-free.

The typed API uses JSON content type and configured persistence. Fields such as
an application's `ID` remain JSON data: they are not interpreted as AMQP headers
or message IDs. No identifiers are generated. Use the raw API when explicit
transport metadata is required.

`FullClient` and `ProducerOnly` declare a queue, exchange and binding.
`ConsumerOnly` requires an existing queue and checks its declaration; it never
creates topology. Methods disabled by the client type return `ErrClientType`.
Unsupported client types are rejected at construction.

## Delivery and retry behavior

- Every publish is mandatory and waits for a broker confirmation. An unroutable
  return produces `ErrUnroutable`; a broker nack produces `ErrPublishNack`.
- Transient connection/setup failures are retried up to `Retries` additional
  attempts, with cancellable delays. Authentication, topology mismatch and other
  permanent errors stop immediately. Exhaustion wraps `ErrRetriesExceeded` and
  the last cause.
- Once a publish starts, a connection failure or confirmation timeout returns
  `ErrPublishUncertain`. The library does **not** resend automatically. The message
  may already have reached RabbitMQ; the application decides whether to resend.
  IDs are never generated or changed. Make consumers idempotent if resending.
- A handler returning nil is followed by an acknowledgement. A handler error is
  followed by a nack using `RequeueOnError`, and consumption continues. The
  application owns logging and handler-error reporting; the client has no logger.
- With `RequeueOnError=false`, rejected messages go to a dead-letter destination
  if broker policy provides one, otherwise they are discarded. Requeueing can
  repeatedly deliver a poison message; this is not a delayed retry queue.
- Cancellation during processing leaves the delivery unacknowledged for broker
  redelivery. A process crash, lost acknowledgement, or application resend can
  cause duplicate processing. Confirms do not prove successful application processing.
- Consumer transport failures trigger a new connection and subscription. The
  recovery budget is `Retries` restarts over one `Consume` call, including setup
  failures; successful deliveries do not reset it. Handler failures do not spend
  this budget. A setup timeout never becomes the lifetime of the subscription.

`Durable=true` and `Persistent=true` request durable topology and persistent
messages. Per-message `DeliveryMode` overrides the persistent default. Message
content type also falls back to the configuration when omitted. Bodies and
headers must remain unchanged while `Produce` is running.

## Contexts and resource ownership

Construction performs no I/O or background work. Call `Connect` before producing
or consuming. Repeated `Connect` calls reuse a healthy connection; publishing
reestablishes an unhealthy connection when needed. One serialized publisher
connection and one connection per active consumer isolate consumer cancellation
and publisher flow control. A consumer-only client's initial connection is kept
for its connection/topology check until `Close`.

`ConnectTimeout` bounds connection establishment and topology setup.
`OperationTimeout` bounds each publishing attempt (including waiting for the
shared publisher), subscription registration, and settlement writes. JSON
encoding/decoding runs synchronously outside these transport timeouts. A shorter caller
deadline takes precedence. Context cancellation interrupts blocking dependency
I/O by closing the owned transport. Idle consumers have no operation deadline;
handlers must cooperate with their context and set limits on their own I/O.

`Close(ctx)` permanently disables the client, cancels active operations, closes
its transports and waits for its work, including handlers. It does not drain
in-flight deliveries; unacknowledged work is left for redelivery. A handler that
ignores cancellation can outlive a shutdown deadline. If `Close` times out, call
it again to wait; create a new client to reconnect after closure. Do not call
`Close` synchronously from a handler, as it would wait for that same handler.
Handler panics propagate after connection cleanup.

Classify public errors with `errors.Is`. Causes remain wrapped where available,
including `context.Canceled`, `context.DeadlineExceeded` and AMQP protocol errors.
`ErrTimeout` distinguishes configured transport timeouts; `ErrInvalidConfig`,
`ErrInvalidMessage`, `ErrInvalidHandler`, `ErrUninitialized`, and `ErrClosed`
describe caller/lifecycle failures. Shutdown errors are combined with `errors.Join`.

## Configuration

Initialize `DefaultConfig()` and then decode or override it. Every serializable
field has matching snake_case `json`/`yaml` tags and an `RMQ_` environment tag.
There is no built-in file or environment loader, and no new loader dependency.
Environment tags require an application-selected decoder that understands `env`.

| JSON / YAML key | Environment variable | Default |
| --- | --- | --- |
| `host` | `RMQ_HOST` | `localhost` |
| `port` | `RMQ_PORT` | `0`: 5672, or 5671 with TLS |
| `username` | `RMQ_USERNAME` | Required |
| `password` | `RMQ_PASSWORD` | Required |
| `vhost` | `RMQ_VHOST` | `/` |
| `tls` | `RMQ_TLS` | `false` |
| `connect_timeout` | `RMQ_CONNECT_TIMEOUT` | `10s` |
| `operation_timeout` | `RMQ_OPERATION_TIMEOUT` | `10s` |
| `retry_delay` | `RMQ_RETRY_DELAY` | `1s` |
| `retries` | `RMQ_RETRIES` | `3` additional attempts |
| `queue` | `RMQ_QUEUE` | Required |
| `exchange` | `RMQ_EXCHANGE` | Empty: default exchange |
| `exchange_type` | `RMQ_EXCHANGE_TYPE` | `direct`; also `topic` or `fanout` |
| `routing_key` | `RMQ_ROUTING_KEY` | Empty |
| `durable` | `RMQ_DURABLE` | `true` |
| `persistent` | `RMQ_PERSISTENT` | `true` |
| `content_type` | `RMQ_CONTENT_TYPE` | `application/json` |
| `prefetch` | `RMQ_PREFETCH` | `1`; allowed range 1–65535 |
| `requeue_on_error` | `RMQ_REQUEUE_ON_ERROR` | `false` |

Timeouts and retry delay must be positive; retries cannot be negative. For the
default exchange, set `RoutingKey` equal to `Queue`. Names must fit AMQP short
strings; reserved `amq.` topology names are not supported. Headers exchanges and
custom binding arguments are outside this library's scope.

Durations are `time.Duration`: standard `encoding/json` expects integer
nanoseconds, for example `"operation_timeout": 10000000000`. YAML and environment
duration parsing depends on the application's decoder; configure it explicitly.

Enable `TLS` for certificate-verified TLS using system trust roots. Set the
programmatic `TLSConfig` for custom roots or client certificates; it is excluded
from JSON/YAML/environment decoding. Configuration is cloned at construction,
but referenced certificate pools and callback state must remain immutable.
Credentials and virtual hosts are passed directly to AMQP authentication rather
than interpolated into a URL, so reserved characters do not require escaping.
Do not dump configuration structs to logs or commit secret-bearing files.

## Run locally

Choose local credentials in your shell; keep passwords outside version control:

```sh
export RMQ_USERNAME=local
read -rs -p 'Local RabbitMQ password: ' RMQ_PASSWORD
export RMQ_PASSWORD
docker compose up -d --wait
go run ./examples/basic
# Ctrl+C cancels consumption and closes the client.
docker compose down
```

The example reads credentials, host and port from the environment and uses fixed
demo topology. Its application job ID is illustrative and stays in the JSON
body; real applications supply their own identity. Compose binds only to loopback and intentionally has no persistent
volume. Removing its container removes broker data.

## Development

```sh
go test ./...
go test -race ./...
go vet ./...
golangci-lint run       # requires v2; configuration is versioned
```

Unit tests use a shared testify suite and require no running RabbitMQ broker.
A temporary loopback TCP listener verifies handshake cancellation and cleanup.
The suite covers configuration, retries, handler outcomes, settlement ordering,
cancellation and lifecycle concurrency. Broker integration tests are deferred
until explicitly requested. CI runs unit tests with race detection, formatting,
vet and lint checks.

## Migration from the prototype

- Replace the constructor's `map[string]any` with `DefaultConfig()` plus typed
  overrides. Rename `user` to `Username`, `timeout` to `OperationTimeout`,
  `retry_timeout` to `RetryDelay`, and `requeue` to `RequeueOnError`.
- Replace `topic` with independent `Queue` and `Exchange` names and an explicit
  `ExchangeType`/`RoutingKey`. To preserve the old topology, use the old topic
  value for both names, `direct` for the type, and the old routing key.
- Wrap the transport with `Typed[YourMessage](transport)` and use `Publish` and
  `Subscribe` through application-owned interfaces. For raw bodies or explicit
  AMQP properties, retain `Produce(ctx, Message{Body: payload})` and `Consume`.
- Replace channels returned by `Consume(ctx)` with `Consume(ctx, handler)` and
  collect its returned error. Remove `auto_ack`; settlement always follows
  processing. Remove `resub_timeout`; recovery uses `RetryDelay` and `Retries`.
- Remove the logger argument: construct with `NewRabbitMQ(cfg, clientType)`.
  Logging belongs to the application. Replace handling of `ErrFatal` with the
  specific errors and wrapped causes above.

MIT licensed.
