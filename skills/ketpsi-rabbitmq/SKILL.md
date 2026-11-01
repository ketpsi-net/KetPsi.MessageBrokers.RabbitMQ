---
name: ketpsi-rabbitmq
description: How to use the KetPsi.MessageBrokers.RabbitMQ NuGet package (Native AOT compatible RabbitMQ publish/subscribe for .NET 8/9/10) — AddRabbitMq registration, IRabbitMqHandler<T> and raw IRabbitMqHandler subscribers, IRabbitMqPublisher<T> and raw IRabbitMqPublisher publishers, reading the raw message body, the ConnectionStrings:RabbitMq connection string, JSON contracts for messages, ack/nack/dead-letter behavior, and Native AOT setup. Use when code references KetPsi.MessageBrokers.RabbitMQ, AddRabbitMq, AddSubscriber, AddPublisher, IRabbitMqHandler, IRabbitMqPublisher or MessageContext, when fixing KPRMQ001–KPRMQ006 build errors, or when the user asks to publish or consume RabbitMQ messages in a project using this package.
---

# Using KetPsi.MessageBrokers.RabbitMQ

A publish/subscribe wrapper for RabbitMQ.Client 7 that runs under Native AOT and trimming, and in regular JIT apps.
Subscribers are handler classes resolved from a new DI scope per message; publishers are typed per message. A bundled
source generator infers each handler's message type at compile time, so registration uses no reflection.

All public types are in the `KetPsi.MessageBrokers.RabbitMQ` namespace.

## Setup

```bash
dotnet add package KetPsi.MessageBrokers.RabbitMQ --prerelease
```

- Targets `net8.0`, `net9.0` and `net10.0`.
- Requires the .NET 9.0.300 SDK or later to build, even when targeting `net8.0`, because the generator uses
  C# interceptors.
- The package enables its interceptor namespace (`KetPsi.MessageBrokers.RabbitMQ.Generated`) itself. If a project
  **replaces** `InterceptorsPreviewNamespaces` or `InterceptorsNamespaces` instead of appending to them, add that
  namespace back.
- There is no separate generator package. Do not add one.

## Registration

Call `AddRabbitMq` once and register everything in its callback:

```csharp
builder.Services.AddRabbitMq(rabbit => rabbit
    .UseJsonContext(MessagingJsonContext.Default)
    .AddSubscriber<OrderPlacedHandler>("Messaging:OrderPlaced")   // options bound from configuration
    .AddSubscriber<RefundHandler>(options =>                       // options set in code
    {
        options.Queue = "billing.refunds";
        options.Exchange = "orders";
        options.ExchangeType = RabbitMqExchangeType.Topic;
        options.RoutingKey = "order.refunded";
    })
    .AddPublisher<OrderPlaced>(options =>
    {
        options.Exchange = "orders";
        options.ExchangeType = RabbitMqExchangeType.Topic;
        options.RoutingKey = "order.placed";
    }));
```

- Pass **only the handler** to `AddSubscriber<THandler>`. The generator reads the message type from the handler's
  `IRabbitMqHandler<T>`. Do not write `AddSubscriber<THandler, TMessage>` unless the handler handles more than one
  message type.
- Register **one publisher per message type**. A second `AddPublisher<T>` for the same `T` throws.
- Do not register handlers in DI yourself; `AddSubscriber` registers them as scoped.
- Configuration sections bind to `RabbitMqSubscriberOptions` / `RabbitMqPublisherOptions` property names:

```json
{
  "Messaging": {
    "OrderPlaced": { "Queue": "billing.order-placed", "Exchange": "orders", "ExchangeType": "topic", "RoutingKey": "order.placed", "PrefetchCount": 20 }
  }
}
```

## Connection

The connection string is read from **`ConnectionStrings:RabbitMq`**. No other configuration section is read.

```text
amqp[s]://[user[:password]@]host[:port][/virtualHost]
```

- The user and password are optional. When missing, `RabbitMqOptions.Username` / `Password` are used (both
  `guest` by default).
- `%NAME%` environment variables are expanded, so prefer
  `amqps://app:%RABBITMQ_PASSWORD%@broker/shop` to putting secrets in `appsettings.json`.
- The password may contain a raw `@`, `:`, `/`, `?` or `#`, because credentials end at the last `@`. Percent-encoding
  also works.
- `amqps` turns on TLS; the port defaults to 5672, or 5671 for `amqps`; the virtual host defaults to `/`. Write a `/`
  inside a virtual host name as `%2F`.
- Query parameters such as `?heartbeat=30` are rejected.
- An invalid or missing connection fails host startup.

To set the connection in code, or override parts of it, use `ConfigureConnection`, which runs after the connection
string is read:

```csharp
rabbit.ConfigureConnection(options =>
{
    options.HostName = "broker";            // used only when there is no connection string
    options.ClientProvidedName = "billing"; // shown in the management UI; defaults to the application name
});
```

For client certificates, configure RabbitMQ.Client's `SslOption` with `services.Configure<SslOption>(...)`.

## Handlers

```csharp
internal sealed class OrderPlacedHandler(IBillingService billing) : IRabbitMqHandler<OrderPlaced>
{
    public async Task HandleAsync(MessageContext<OrderPlaced> context, CancellationToken cancellationToken)
        => await billing.ChargeAsync(context.Message.OrderId, context.Message.Total, cancellationToken);
}
```

- Handlers may depend on scoped services such as a `DbContext`, because each message gets its own DI scope.
- Pass `cancellationToken` to every call. It is canceled on shutdown, and a message canceled that way is requeued.
- Settling is automatic, so do not ack in the handler:

| Outcome | Result |
|---|---|
| Handler returns | ack |
| Handler throws | nack; requeued when `RequeueOnError` is true (the default) |
| Body is invalid JSON or `null` (typed handlers only) | reject without requeue (dead-lettered if a dead-letter exchange is set) |
| Host shuts down mid-handler | nack with requeue |

- To settle early or differently, call `context.AckAsync()` or `context.NackAsync(requeue)`. A message is settled
  at most once, so the automatic settle then does nothing.
- `MessageContext<T>` also has the raw `Body`, `Redelivered`, `Exchange`, `RoutingKey`, `DeliveryTag` and
  `Properties`, which includes the content type and headers. See "Raw bytes" below for raw handlers.

**Poison messages.** With the default `RequeueOnError = true`, a message that always fails is redelivered forever. For
anything non-trivial, set:

```csharp
options.RequeueOnError = false;
options.DeadLetterExchange = "orders.dlx";   // declare this exchange and a queue bound to it
```

Queue arguments such as the dead-letter exchange cannot change once the queue exists. Changing them fails with
`PRECONDITION_FAILED` until the queue is deleted.

Other subscriber options: `PrefetchCount` (unlimited by default) and `ConsumerDispatchConcurrency` (1 by default, so
messages are handled in order one at a time). When `ExchangeType` is null, the exchange must already exist; when it
is set, the exchange is declared durable.

## Publishers

```csharp
public sealed class CheckoutService(IRabbitMqPublisher<OrderPlaced> publisher)
{
    public Task CompleteAsync(Order order, CancellationToken cancellationToken)
        => publisher.PublishAsync(new OrderPlaced(order.Id, order.Total), cancellationToken);
}
```

- Inject `IRabbitMqPublisher<T>`. It is a singleton and safe to use concurrently.
- `PublishAsync(message, routingKey, headers, cancellationToken)` overrides the routing key and adds headers.
- `PublisherConfirms` (true by default) makes each publish wait for the broker and throw if the broker nacks it.
  `Mandatory = true` also throws when no queue receives the message; it only takes effect with confirms on.
- The publisher's `Queue` option declares and binds a queue, so messages are kept before any subscriber starts. Leave
  it empty when a subscriber declares the same queue with a `DeadLetterExchange`: the declarations would differ and
  the broker rejects the second one.
- To publish bytes serialized some other way, use the raw `IRabbitMqPublisher` (see "Raw bytes").

## Raw bytes

When the user needs the bytes on the wire, pick one of these three.

**1. Read the raw body in a typed handler.** `MessageContext<T>` derives from `MessageContext`, so every handler has
the body exactly as it was published, next to the deserialized `Message`. Use it to log, archive, verify a signature
or forward the original bytes:

```csharp
internal sealed class OrderPlacedHandler(IArchive archive) : IRabbitMqHandler<OrderPlaced>
{
    public async Task HandleAsync(MessageContext<OrderPlaced> context, CancellationToken cancellationToken)
    {
        await archive.SaveAsync(context.Message.OrderId, context.Body.ToArray(), cancellationToken);
        // ... handle context.Message
    }
}
```

**2. Receive raw messages: implement `IRabbitMqHandler`.** The non-generic handler gets `MessageContext`, without
`Message`, and nothing is deserialized. Use it for formats other than JSON (protobuf, Avro, text), or to deserialize
the body yourself:

```csharp
internal sealed class TelemetryHandler(ITelemetryStore store) : IRabbitMqHandler
{
    public Task HandleAsync(MessageContext context, CancellationToken cancellationToken)
        => store.WriteAsync(context.Properties.ContentType, context.Body, cancellationToken);
}

rabbit.AddSubscriber<TelemetryHandler>("Messaging:Telemetry");
```

Raw handlers are registered with the same `AddSubscriber<THandler>`. They need no JSON contract and no source
generator, and every body reaches them, including bodies that are not valid JSON. A class implements either
`IRabbitMqHandler` or `IRabbitMqHandler<T>`, not both (`KPRMQ006`).

**3. Publish raw bytes: inject `IRabbitMqPublisher`.** The non-generic publisher sends a body you have already
serialized, as given:

```csharp
rabbit.AddPublisher(options => { options.Exchange = "telemetry"; options.RoutingKey = "reading"; });

public sealed class TelemetrySender(IRabbitMqPublisher publisher)
{
    public Task SendAsync(Reading reading, CancellationToken cancellationToken)
        => publisher.PublishAsync(reading.ToByteArray(), "reading", "application/x-protobuf", headers: null, cancellationToken);
}
```

It takes a `ReadOnlyMemory<byte>`, which a `byte[]` converts to implicitly. The content type defaults to
`application/octet-stream`. Options and confirms work as for typed publishers, and there is one raw publisher.

**`Body` is RabbitMQ.Client's own buffer.** It is valid until `HandleAsync` completes, including across `await`s,
and is reused afterwards, so reading it costs no copy. Never keep it beyond the handler, for example in background
work; copy it with `context.Body.ToArray()` instead.


## Message types and JSON

Write messages as records with explicit JSON names, so a C# rename never changes the wire format:

```csharp
public sealed record OrderPlaced(
    [property: JsonPropertyName("orderId")] string OrderId,
    [property: JsonPropertyName("total")] decimal Total);

[JsonSerializable(typeof(OrderPlaced))]
[JsonSerializable(typeof(RefundIssued))]
internal sealed partial class MessagingJsonContext : JsonSerializerContext;
```

Contracts are looked up in this order:

1. Contexts added with `UseJsonContext`.
2. `JsonSerializerContext` services in DI.
3. The `TypeInfoResolverChain` set with `ConfigureJsonSerializer`.
4. `JsonSerializerOptions` services in DI.
5. Reflection, only where it is enabled.

- **Under Native AOT or trimming, reflection is off.** Every message type then needs `[JsonSerializable]` in a
  registered context, and a missing one fails host startup with an error naming the attribute to add.
- In a JIT app, unregistered types fall back to reflection. Still prefer a context, so the app can move to AOT later.
- Use `ConfigureJsonSerializer(o => ...)` for settings such as the naming policy of the reflection fallback.

## Native AOT

- Use `AddSubscriber<THandler>`; the generator makes registration reflection-free.
- Register every message type in a `JsonSerializerContext`, as above.
- Bind options from configuration as usual. The package uses the configuration binding source generator.
- The package adds no trim or AOT warnings. Verify with `dotnet publish -r linux-x64 -c Release` and treat any
  `IL2xxx` / `IL3xxx` warning as an error.

## Fixing KPRMQ diagnostics

| Id | Meaning | Fix |
|---|---|---|
| KPRMQ001 | The class implements neither `IRabbitMqHandler<T>` nor `IRabbitMqHandler` | Implement `IRabbitMqHandler<TMessage>`, or `IRabbitMqHandler` for raw messages |
| KPRMQ002 | The class implements `IRabbitMqHandler<T>` more than once | Call `AddSubscriber<THandler, TMessage>` once per message type |
| KPRMQ003 | The handler is a generic type parameter | Call `AddSubscriber<THandler, TMessage>` |
| KPRMQ004 | The handler is abstract or an interface | Register a concrete class |
| KPRMQ005 | The handler or message type is private, nested in a private type, or `file`-scoped | Make it `internal` or `public` |
| KPRMQ006 | The class implements both `IRabbitMqHandler` and `IRabbitMqHandler<T>` | Keep one: raw or typed |

A runtime `InvalidOperationException` saying `AddSubscriber<...> was not rewritten by the ... source generator` means
the generator did not run. Check the SDK version and the interceptor namespace setting, or call
`AddSubscriber<THandler, TMessage>`.

## Startup and shutdown

- Options are validated when the host starts. A missing or invalid connection, a subscriber without `Queue`, a
  publisher without `Exchange`, or (without reflection) a message type without a JSON contract fails startup.
- All consumers start at the same time. The first one that fails cancels the others and fails host startup.
- On shutdown, consumers stop taking messages, wait for running handlers up to the host's `ShutdownTimeout`, then
  close. Messages still unacked go back to the queue.

## Testing

- Unit-test handlers directly; they are plain classes. `MessageContext` and `MessageContext<T>` have no public
  constructor, so put the logic in a method that takes the message or bytes, and call it from `HandleAsync`.
- Mock `IRabbitMqPublisher<T>` in services that publish.
- For integration tests, run a broker with `Testcontainers.RabbitMq`, and point the app at it with
  `ConfigureConnection(o => o.ConnectionString = container.GetConnectionString())`. Give each test unique
  queue and exchange names.
