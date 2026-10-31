# KetPsi.MessageBrokers.RabbitMQ 1.0.0-preview.1 — Release Notes

The first public preview.

**Publish/subscribe on RabbitMQ.Client 7 that runs under Native AOT.** Handlers are plain classes, publishers are
typed per message, and registration needs no reflection: a bundled source generator reads each handler's message type
at compile time. The same package runs unchanged in regular JIT apps.

```csharp
builder.Services.AddRabbitMq(rabbit => rabbit
    .UseJsonContext(MessagingJsonContext.Default)
    .AddSubscriber<OrderPlacedHandler>("Messaging:OrderPlaced")
    .AddPublisher<OrderPlaced>(options => options.Exchange = "orders"));
```

Package: `KetPsi.MessageBrokers.RabbitMQ`. It includes the source generator; there is no separate package to install.

Targets `net8.0`, `net9.0` and `net10.0`, each with its own `Microsoft.Extensions.*` major version (8.0, 9.0, 10.0).
Using the generator needs the .NET 9.0.300 SDK or later, also for `net8.0`.

> ⚠️ **Preview.** The public API can still change between previews.

---

## Highlights

- **Native AOT on .NET 8, 9 and 10.** A sample is published with `PublishAot` on all three frameworks with zero trim
  and AOT warnings, and run against a RabbitMQ broker.
- **Message type inferred at compile time.** `AddSubscriber<OrderPlacedHandler>()` is rewritten by an interceptor into
  `AddSubscriber<OrderPlacedHandler, OrderPlaced>()`. A handler that cannot be registered this way is a build error
  (`KPRMQ001`–`KPRMQ005`).
- **Connection strings with optional credentials.** `amqp[s]://[user[:password]@]host[:port][/virtualHost]`.
  Environment variables are expanded, passwords may contain raw special characters, and `amqps` turns on TLS. An
  invalid value fails host startup without echoing the credentials.
- **Consumers start at the same time.** A slow consumer does not hold up the others. The first failure cancels the
  rest and fails host startup.
- **JSON from wherever you keep it.** `UseJsonContext`, `JsonSerializerContext` and `JsonSerializerOptions` services,
  and `ConfigureJsonSerializer`, with a reflection fallback only where reflection is available.

---

## What's included

| Area | Supported |
|---|---|
| Registration | `AddRabbitMq(rabbit => ...)` with `ConfigureConnection`, `UseJsonContext`, `ConfigureJsonSerializer`, `AddSubscriber<THandler>`, `AddSubscriber<THandler, TMessage>`, `AddPublisher<TMessage>`; subscriber and publisher options from code or a configuration section |
| Connection | `ConnectionStrings:RabbitMq` or individual options; one lazily opened, shared connection with automatic and topology recovery; `SslOption` for custom TLS |
| Subscribers | Scoped handler per message; durable queue, optional exchange declaration and binding, dead-letter exchange, prefetch, dispatch concurrency |
| Settling | Ack on success; nack with `RequeueOnError` on failure; reject without requeue for invalid JSON; requeue on shutdown; manual `AckAsync` / `NackAsync`, settled at most once |
| Publishers | Singleton per message type on one long-lived channel; publisher confirms, mandatory publishing, routing key and header overrides; persistent JSON messages |
| Validation | Missing connection, invalid connection string, missing queue or exchange, and (without reflection) message types without a JSON contract all fail host startup |
| Logging | Through `ILogger` with [KetPsi.Extensions.Logging](https://github.com/ketpsi-net/KetPsi.Logging.Extensions) |

## Known limitations

- One publisher per message type.
- Query parameters in the connection string (such as `?heartbeat=30`) are rejected rather than applied.
- Tracing (`ActivitySource`) is not included yet.
