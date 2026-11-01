namespace KetPsi.MessageBrokers.RabbitMQ;

/// <summary>
/// Publishes raw bodies you have already serialized, in any format, registered with
/// <see cref="IRabbitMqConfigBuilder.AddPublisher(Action{RabbitMqPublisherOptions})"/>. Messages are persistent.
/// Failures, including a broker nack or an unroutable mandatory message, are thrown.
/// </summary>
public interface IRabbitMqPublisher
{
    /// <summary>
    /// Publishes a body with the configured <see cref="RabbitMqPublisherOptions.RoutingKey"/> and the content type
    /// <c>application/octet-stream</c>.
    /// </summary>
    /// <param name="body">The message body, sent as is. A <c>byte[]</c> converts to it implicitly.</param>
    /// <param name="cancellationToken">Cancels the publish.</param>
    Task PublishAsync(ReadOnlyMemory<byte> body, CancellationToken cancellationToken = default);

    /// <summary>Publishes a body with an explicit routing key, content type and optional headers.</summary>
    /// <param name="body">The message body, sent as is. A <c>byte[]</c> converts to it implicitly.</param>
    /// <param name="routingKey">The routing key, replacing the configured one.</param>
    /// <param name="contentType">The content type, such as <c>application/x-protobuf</c>; <c>application/octet-stream</c> when null.</param>
    /// <param name="headers">Headers to send; they are copied.</param>
    /// <param name="cancellationToken">Cancels the publish.</param>
    Task PublishAsync(
        ReadOnlyMemory<byte> body,
        string routingKey,
        string? contentType,
        IReadOnlyDictionary<string, object?>? headers,
        CancellationToken cancellationToken = default);
}

/// <summary>
/// Publishes messages of one type, serialized to JSON, registered with
/// <see cref="IRabbitMqConfigBuilder.AddPublisher{TMessage}(Action{RabbitMqPublisherOptions})"/>. Messages are
/// persistent <c>application/json</c>. Failures, including a broker nack or an unroutable mandatory message, are thrown.
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
