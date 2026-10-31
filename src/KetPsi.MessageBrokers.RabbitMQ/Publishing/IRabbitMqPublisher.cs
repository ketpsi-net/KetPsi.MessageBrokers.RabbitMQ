namespace KetPsi.MessageBrokers.RabbitMQ;

/// <summary>
/// Publishes messages of one type, registered with
/// <see cref="IRabbitMqConfigBuilder.AddPublisher{TMessage}(Action{RabbitMqPublisherOptions})"/>. Messages are
/// persistent JSON. Failures, including a broker nack or an unroutable mandatory message, are thrown.
/// </summary>
/// <typeparam name="TMessage">The message type. Under Native AOT it needs a source-generated JSON contract.</typeparam>
public interface IRabbitMqPublisher<in TMessage>
{
    /// <summary>Publishes a message with the configured <see cref="RabbitMqPublisherOptions.RoutingKey"/>.</summary>
    /// <param name="message">The message.</param>
    /// <param name="cancellationToken">Cancels the publish.</param>
    Task PublishAsync(TMessage message, CancellationToken cancellationToken = default);

    /// <summary>Publishes a message with an explicit routing key and optional headers.</summary>
    /// <param name="message">The message.</param>
    /// <param name="routingKey">The routing key, replacing the configured one.</param>
    /// <param name="headers">Headers to send; they are copied.</param>
    /// <param name="cancellationToken">Cancels the publish.</param>
    Task PublishAsync(
        TMessage message,
        string routingKey,
        IReadOnlyDictionary<string, object?>? headers,
        CancellationToken cancellationToken = default);
}
