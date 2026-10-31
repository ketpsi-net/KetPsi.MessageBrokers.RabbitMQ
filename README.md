# KetPsi.MessageBrokers.RabbitMQ

[![NuGet](https://img.shields.io/nuget/v/KetPsi.MessageBrokers.RabbitMQ.svg)](https://www.nuget.org/packages/KetPsi.MessageBrokers.RabbitMQ/)
[![.NET](https://img.shields.io/badge/.NET-8.0%20%7C%209.0%20%7C%2010.0-purple.svg?style=flat-square)](https://dotnet.microsoft.com/)
[![License: MIT](https://img.shields.io/badge/License-MIT-blue.svg?style=flat-square)](LICENSE)

**KetPsi.MessageBrokers.RabbitMQ** is a publish/subscribe wrapper for [RabbitMQ.Client 7](https://github.com/rabbitmq/rabbitmq-dotnet-client)
that runs under **Native AOT** and trimming, and just as well in a regular JIT app.

- **Subscribers are plain handler classes.** Each message is handled by a handler resolved from a new DI scope, then
  acked, nacked or dead-lettered for you.
- **Publishers are typed per message**: inject `IRabbitMqPublisher<OrderPlaced>` and call `PublishAsync`.
- **No reflection in registration.** A source generator, bundled in the package, reads the message type from your
  handler at compile time.
- **JSON your way.** Source-generated `JsonSerializerContext`s, from the builder or from DI, with a reflection fallback
  only where reflection is available.

---

## Quick start

```bash
dotnet add package KetPsi.MessageBrokers.RabbitMQ
```

```jsonc
// appsettings.json
{
  "ConnectionStrings": {
    "RabbitMq": "amqp://orders:%RABBITMQ_PASSWORD%@broker:5672/shop"
  },
  "Messaging": {
    "OrderPlaced": { "Queue": "billing.order-placed", "Exchange": "orders", "ExchangeType": "topic", "RoutingKey": "order.placed" }
  }
}
```

```csharp
public sealed record OrderPlaced(
    [property: JsonPropertyName("orderId")] string OrderId,
    [property: JsonPropertyName("total")] decimal Total);

[JsonSerializable(typeof(OrderPlaced))]
internal sealed partial class MessagingJsonContext : JsonSerializerContext;

internal sealed class OrderPlacedHandler(IBillingService billing) : IRabbitMqHandler<OrderPlaced>
{
    public async Task HandleAsync(MessageContext<OrderPlaced> context, CancellationToken cancellationToken)
        => await billing.ChargeAsync(context.Message.OrderId, context.Message.Total, cancellationToken);
}

builder.Services.AddRabbitMq(rabbit => rabbit
    .UseJsonContext(MessagingJsonContext.Default)
    .AddSubscriber<OrderPlacedHandler>("Messaging:OrderPlaced")
    .AddPublisher<OrderPlaced>(options =>
    {
        options.Exchange = "orders";
        options.ExchangeType = RabbitMqExchangeType.Topic;
        options.RoutingKey = "order.placed";
    }));
```

```csharp
public sealed class CheckoutService(IRabbitMqPublisher<OrderPlaced> publisher)
{
    public Task CompleteAsync(Order order, CancellationToken cancellationToken)
        => publisher.PublishAsync(new OrderPlaced(order.Id, order.Total), cancellationToken);
}
```

---

## Connection

The connection string is read from **`ConnectionStrings:RabbitMq`**:

```text
amqp[s]://[user[:password]@]host[:port][/virtualHost]
```

| Part | Notes |
|---|---|
| `amqp` / `amqps` | `amqps` turns on TLS, with the host as the server name. |
| `user`, `password` | Optional. When missing, `RabbitMqOptions.Username` and `Password` are used (both `guest` by default). |
| `port` | Defaults to 5672, or 5671 for `amqps`. |
| `virtualHost` | Defaults to `/`. Write a `/` inside the name as `%2F`, e.g. `/prod%2Forders` for `prod/orders`. |

- **Environment variables** written as `%NAME%` are expanded, so secrets can stay out of configuration files:
  `amqps://orders:%RABBITMQ_PASSWORD%@%RABBITMQ_HOST%/shop`.
- **Passwords may contain raw `@`, `:`, `/`, `?` or `#`.** Credentials end at the last `@`. Percent-encoded parts are
  decoded, so `p%40ss` is also `p@ss`.
- An invalid connection string **fails host startup** with a message that never repeats the credentials.

The string is parsed by the library itself rather than with `System.Uri`. `Uri` works under Native AOT, but it splits
credentials at the first reserved character, so an unencoded password breaks it.

Set or override anything in code, including connecting without a connection string:

```csharp
rabbit.ConfigureConnection(options =>
{
    options.HostName = "broker";             // used when there is no connection string
    options.Username = "orders";
    options.Password = secrets.RabbitMqPassword;
    options.ClientProvidedName = "billing";  // shown in the management UI; defaults to the application name
});
```

For client certificates or custom TLS settings, configure RabbitMQ.Client's `SslOption`:
`services.Configure<SslOption>(ssl => { ssl.Enabled = true; ssl.ServerName = "broker"; ssl.CertPath = "..."; })`.

The connection uses automatic and topology recovery and is opened once, lazily, then shared by every subscriber and
publisher.

---

## Subscribers

```csharp
rabbit.AddSubscriber<OrderPlacedHandler>(options => { options.Queue = "billing.order-placed"; /* ... */ });
rabbit.AddSubscriber<OrderPlacedHandler>("Messaging:OrderPlaced");   // bind the options from configuration
```

| Option | Default | Meaning |
|---|---|---|
| `Queue` | *(required)* | The queue to consume, declared durable. |
| `Exchange` | none | Binds the queue to this exchange. When empty, the queue only gets messages sent to it through the default exchange. |
| `ExchangeType` | none | When set, the exchange is declared (durable); otherwise it must already exist. |
| `RoutingKey` | `""` | The binding key. |
| `RequeueOnError` | `true` | Whether a message whose handler throws goes back to the queue. |
| `DeadLetterExchange` | none | Sets the queue's `x-dead-letter-exchange`. Pair it with `RequeueOnError = false` so a poison message is moved aside instead of looping forever. |
| `PrefetchCount` | unlimited | How many unacked messages the broker sends at once. |
| `ConsumerDispatchConcurrency` | 1 | How many messages are handled concurrently. |

**How a message is settled:**

- The handler returns: the message is **acked**.
- The handler throws: it is **nacked**, and requeued or not according to `RequeueOnError`.
- The body is not valid JSON, or is `null`: it is **rejected without requeue**, so it is dead-lettered when a
  dead-letter exchange is set.
- The host shuts down while the handler runs: the handler's token is canceled and the message is **requeued**.
- The handler can also settle the message itself with `context.AckAsync()` or `context.NackAsync(requeue)`. A message
  is settled only once.

`MessageContext<T>` also exposes `Redelivered`, `Exchange`, `RoutingKey`, `DeliveryTag` and `Properties`, which
includes the headers.

**Startup and shutdown.** All consumers **start at the same time** with the host, so a slow queue declaration does not
hold up the others. The first consumer that fails cancels the rest and **fails host startup**. On shutdown each
consumer stops taking deliveries, waits for in-flight handlers until the host's shutdown timeout, then closes its
channel.

### The source generator

`AddSubscriber<OrderPlacedHandler>` is rewritten at compile time into
`AddSubscriber<OrderPlacedHandler, OrderPlaced>`, with the message type read from the `IRabbitMqHandler<T>` the handler
implements. The rewrite uses C# interceptors and needs no reflection. The package enables the interceptors for you.

When a handler can't be registered this way, the build fails:

| ID | Problem | Fix |
|---|---|---|
| `KPRMQ001` | The class implements no `IRabbitMqHandler<T>`. | Implement it. |
| `KPRMQ002` | The class handles more than one message type. | Call `AddSubscriber<THandler, TMessage>` once per message type. |
| `KPRMQ003` | The handler is a generic type parameter. | Call `AddSubscriber<THandler, TMessage>`. |
| `KPRMQ004` | The handler is abstract or an interface. | Register a concrete class. |
| `KPRMQ005` | The handler or message type is private or `file`-scoped. | Make it `internal` or `public`. |

`AddSubscriber<THandler, TMessage>` can always be called directly.

---

## Publishers

```csharp
rabbit.AddPublisher<OrderPlaced>(options => { options.Exchange = "orders"; /* ... */ });
rabbit.AddPublisher<OrderPlaced>("Messaging:Publishers:OrderPlaced");
```

| Option | Default | Meaning |
|---|---|---|
| `Exchange` | *(required)* | Declared durable when the publisher first publishes. |
| `ExchangeType` | `direct` | Use the constants in `RabbitMqExchangeType`. |
| `RoutingKey` | `""` | Used when `PublishAsync` is not given one. |
| `Queue` | none | A queue to declare and bind, so messages are kept before any subscriber exists. |
| `PublisherConfirms` | `true` | Each publish waits for the broker's confirm and throws if the broker nacks it. |
| `Mandatory` | `false` | A message no queue receives throws instead of being dropped. Requires `PublisherConfirms`. |

`IRabbitMqPublisher<T>` is a singleton with one long-lived channel. Publishes are serialized on it, and messages are
sent as persistent `application/json`. `PublishAsync(message, routingKey, headers)` overrides the routing key and adds
headers. There is one publisher per message type.

---

## JSON

The contract for a message type is looked up in this order:

1. Contexts added with `UseJsonContext(...)`, in the order they were added.
2. Every `JsonSerializerContext` registered in DI, for example `services.AddSingleton<JsonSerializerContext>(MyContext.Default)`.
3. The `TypeInfoResolverChain` set with `ConfigureJsonSerializer(options => ...)`.
4. Every `JsonSerializerOptions` registered in DI that has a `TypeInfoResolver`.
5. **Reflection**, only when `JsonSerializer.IsReflectionEnabledByDefault` is true. It uses the settings from
   `ConfigureJsonSerializer`, such as the naming policy.

Under Native AOT and in trimmed apps, the SDK turns reflection off. There, a message type without a source-generated
contract **fails host startup** with an error naming the missing `[JsonSerializable(typeof(...))]`. In a regular JIT
app, such types simply fall back to reflection.

---

## Native AOT and trimming

The library is marked `IsAotCompatible`. The sample in [`samples/AotSample`](samples/AotSample) is published with
`PublishAot` for `net8.0`, `net9.0` and `net10.0` with **zero** trim and AOT warnings, then run against a real broker.

The library contains one warning suppression, in the reflection fallback, the same one ASP.NET Core's `JsonOptions`
makes. That code runs only when `JsonSerializer.IsReflectionEnabledByDefault` is true. The SDK sets that switch to false
in every trimmed and Native AOT app, so the trimmer removes the code there.

---

## Target frameworks and requirements

| Target | Microsoft.Extensions.* |
|---|---|
| `net8.0` | 8.0 |
| `net9.0` | 9.0 |
| `net10.0` | 10.0 |

The bundled source generator needs a compiler with interceptor support: the **.NET 9.0.300 SDK or later**, also when
targeting `net8.0`.

---

## For AI coding agents

[skills/ketpsi-rabbitmq/SKILL.md](./skills/ketpsi-rabbitmq/SKILL.md) is a usage guide written for coding agents. It
covers registration, handlers, publishers, connection strings, JSON contracts, Native AOT and the `KPRMQ` diagnostics.
Copy the `ketpsi-rabbitmq` folder into your agent's skills directory, for example `~/.claude/skills/` or
`.claude/skills/` in your repository.

---

## Building this repository

```bash
dotnet build KetPsi.MessageBrokers.RabbitMQ.slnx
dotnet test --solution KetPsi.MessageBrokers.RabbitMQ.slnx    # integration tests need Docker (Testcontainers)
```

The SDK is pinned in `global.json`. The library's own logging goes through
[KetPsi.Extensions.Logging](https://github.com/ketpsi-net/KetPsi.Logging.Extensions), whose generator needs a
newer compiler than consumers do (Roslyn 5.6+).

To check Native AOT, publish the sample for Linux, for example inside `mcr.microsoft.com/dotnet/sdk:10.0-noble-aot`:

```bash
dotnet publish samples/AotSample -f net10.0 -r linux-x64 -c Release
ConnectionStrings__RabbitMq=amqp://localhost samples/AotSample/bin/Release/net10.0/linux-x64/publish/AotSample
```

## License

[MIT](LICENSE)
