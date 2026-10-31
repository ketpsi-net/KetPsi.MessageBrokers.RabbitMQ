namespace KetPsi.MessageBrokers.RabbitMQ;

/// <summary>Where a publisher sends messages and how it confirms them.</summary>
public sealed class RabbitMqPublisherOptions
{
    /// <summary>The exchange to publish to. Required. It is declared durable when the publisher's channel opens.</summary>
    public string Exchange { get; set; } = string.Empty;

    /// <summary>The type of <see cref="Exchange"/>, from <see cref="RabbitMqExchangeType"/>.</summary>
    public string ExchangeType { get; set; } = RabbitMqExchangeType.Direct;

    /// <summary>The routing key used when a publish call does not pass one.</summary>
    public string RoutingKey { get; set; } = string.Empty;

    /// <summary>
    /// A queue to declare (durable, without arguments) and bind to <see cref="Exchange"/> with <see cref="RoutingKey"/>,
    /// so messages are kept before any subscriber starts. If a subscriber declares the same queue with a
    /// <see cref="RabbitMqSubscriberOptions.DeadLetterExchange"/>, leave this empty: the arguments would differ and the
    /// broker rejects the second declaration.
    /// </summary>
    public string? Queue { get; set; }

    /// <summary>Whether each publish waits for the broker to confirm it, throwing if the broker nacks it. Defaults to true.</summary>
    public bool PublisherConfirms { get; set; } = true;

    /// <summary>
    /// Whether a message that no queue receives is returned as an error instead of dropped. Only takes effect when
    /// <see cref="PublisherConfirms"/> is true, since only then does the publish wait to learn the outcome.
    /// </summary>
    public bool Mandatory { get; set; }
}
