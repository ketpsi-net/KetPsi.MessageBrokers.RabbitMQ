# KetPsi.MessageBrokers.RabbitMQ

A publish/subscribe wrapper for RabbitMQ.Client 7 that runs under **Native AOT** and trimming, and just as well in a
regular JIT app. Targets `net8.0`, `net9.0` and `net10.0`.

- **Subscribers** are plain handler classes, resolved from a new DI scope for every message, then acked, nacked or
  dead-lettered for you. `IRabbitMqHandler<T>` gets the deserialized message and its raw body; `IRabbitMqHandler`
  gets only the raw body, for non-JSON payloads.
- **Publishers** are typed per message: `IRabbitMqPublisher<OrderPlaced>`, with publisher confirms by default. The
  non-generic `IRabbitMqPublisher` sends bytes you have serialized yourself.
- **No reflection in registration**: a bundled source generator reads each handler's message type at compile time.
- **JSON** comes from source-generated contexts, from the builder or from DI, with a reflection fallback only where
  reflection is available.

## Usage

```json
{ "ConnectionStrings": { "RabbitMq": "amqp://orders:%RABBITMQ_PASSWORD%@broker:5672/shop" } }
```

```csharp
[JsonSerializable(typeof(OrderPlaced))]
internal sealed partial class MessagingJsonContext : JsonSerializerContext;

internal sealed class OrderPlacedHandler(IBillingService billing) : IRabbitMqHandler<OrderPlaced>
{
    public async Task HandleAsync(MessageContext<OrderPlaced> context, CancellationToken cancellationToken)
        => await billing.ChargeAsync(context.Message.OrderId, cancellationToken);
}

builder.Services.AddRabbitMq(rabbit => rabbit
    .UseJsonContext(MessagingJsonContext.Default)
    .AddSubscriber<OrderPlacedHandler>("Messaging:OrderPlaced")   // binds RabbitMqSubscriberOptions
    .AddPublisher<OrderPlaced>(options =>
    {
        options.Exchange = "orders";
        options.RoutingKey = "order.placed";
    }));
```

## Connection string

`ConnectionStrings:RabbitMq` has the form `amqp[s]://[user[:password]@]host[:port][/virtualHost]`.

- The user and password are optional; when missing, `RabbitMqOptions.Username` and `Password` apply.
- `%NAME%` environment variables are expanded.
- The password may contain a raw `@`, `:` or `/`.
- `amqps` turns on TLS.
- An invalid value fails host startup.

Use `ConfigureConnection(...)` to set or override the connection in code.

## Requirements

The bundled source generator needs the .NET 9.0.300 SDK or later, also when targeting `net8.0`. Without it,
`AddSubscriber<THandler>` cannot infer the message type; call `AddSubscriber<THandler, TMessage>` instead.

Documentation: https://github.com/ketpsi-net/KetPsi.RabbitMQ
