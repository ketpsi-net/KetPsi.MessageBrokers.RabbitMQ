namespace KetPsi.MessageBrokers.RabbitMQ;

/// <summary>Where a subscriber consumes from and how it settles messages.</summary>
public sealed class RabbitMqSubscriberOptions
{
    /// <summary>The queue to consume. Required. It is declared durable.</summary>
    public string Queue { get; set; } = string.Empty;

    /// <summary>
    /// The exchange to bind <see cref="Queue"/> to. When empty the queue is not bound (it still receives messages
    /// published to the default exchange with the queue name as routing key).
    /// </summary>
    public string? Exchange { get; set; }

    /// <summary>
    /// The type of <see cref="Exchange"/>, from <see cref="RabbitMqExchangeType"/>. When set, the exchange is declared
    /// durable; when null, it must already exist.
    /// </summary>
    public string? ExchangeType { get; set; }

    /// <summary>The binding key between <see cref="Exchange"/> and <see cref="Queue"/>.</summary>
    public string RoutingKey { get; set; } = string.Empty;

    /// <summary>
    /// Whether a message whose handler throws is requeued. Defaults to true.
    /// <para>
    /// A message that always fails (a poison message) is then redelivered forever, blocking the queue and burning
    /// CPU. Set this to false together with <see cref="DeadLetterExchange"/> to move failed messages aside instead.
    /// </para>
    /// </summary>
    public bool RequeueOnError { get; set; } = true;

    /// <summary>
    /// The exchange that rejected and expired messages go to, set as the queue's <c>x-dead-letter-exchange</c> argument.
    /// Queue arguments cannot change after the queue exists: changing this for an existing queue fails with
    /// PRECONDITION_FAILED until the queue is deleted.
    /// </summary>
    public string? DeadLetterExchange { get; set; }

    /// <summary>How many unacked messages the broker sends this consumer at once. Unlimited when null.</summary>
    public ushort? PrefetchCount { get; set; }

    /// <summary>
    /// How many messages this consumer's channel handles concurrently. When null, the client default (1) applies, so
    /// messages are handled one at a time in order.
    /// </summary>
    public ushort? ConsumerDispatchConcurrency { get; set; }
}
