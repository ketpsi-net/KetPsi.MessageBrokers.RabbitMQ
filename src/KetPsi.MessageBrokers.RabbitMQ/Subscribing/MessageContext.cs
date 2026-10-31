using RabbitMQ.Client;

namespace KetPsi.MessageBrokers.RabbitMQ;

/// <summary>
/// A deserialized message and its delivery details. The raw body is deliberately not exposed: RabbitMQ.Client reuses
/// its buffer once the delivery callback returns.
/// </summary>
/// <typeparam name="TMessage">The message type.</typeparam>
public sealed class MessageContext<TMessage>
{
    private readonly IMessageSettler _settler;
    private int _settled;

    internal MessageContext(
        TMessage message,
        IMessageSettler settler,
        ulong deliveryTag,
        bool redelivered,
        string exchange,
        string routingKey,
        IReadOnlyBasicProperties properties)
    {
        Message = message;
        _settler = settler;
        DeliveryTag = deliveryTag;
        Redelivered = redelivered;
        Exchange = exchange;
        RoutingKey = routingKey;
        Properties = properties;
    }

    /// <summary>The deserialized message.</summary>
    public TMessage Message { get; }

    /// <summary>The delivery tag, unique per channel.</summary>
    public ulong DeliveryTag { get; }

    /// <summary>Whether the broker delivered this message before without it being acked.</summary>
    public bool Redelivered { get; }

    /// <summary>The exchange the message was published to; empty for the default exchange.</summary>
    public string Exchange { get; }

    /// <summary>The routing key the message was published with.</summary>
    public string RoutingKey { get; }

    /// <summary>The message properties, including headers.</summary>
    public IReadOnlyBasicProperties Properties { get; }

    /// <summary>Whether the message has been acked or nacked.</summary>
    public bool IsSettled => Volatile.Read(ref _settled) != 0;

    /// <summary>Acks the message. Does nothing if it is already settled.</summary>
    /// <param name="cancellationToken">Cancels the ack.</param>
    public ValueTask AckAsync(CancellationToken cancellationToken = default)
        => TrySettle() ? _settler.AckAsync(DeliveryTag, cancellationToken) : ValueTask.CompletedTask;

    /// <summary>Nacks the message. Does nothing if it is already settled.</summary>
    /// <param name="requeue">Whether the broker should requeue it; otherwise it is dead-lettered or dropped.</param>
    /// <param name="cancellationToken">Cancels the nack.</param>
    public ValueTask NackAsync(bool requeue, CancellationToken cancellationToken = default)
        => TrySettle() ? _settler.NackAsync(DeliveryTag, requeue, cancellationToken) : ValueTask.CompletedTask;

    private bool TrySettle() => Interlocked.Exchange(ref _settled, 1) == 0;
}
