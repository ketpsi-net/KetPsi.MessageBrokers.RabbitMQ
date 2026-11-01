namespace KetPsi.MessageBrokers.RabbitMQ;

/// <summary>
/// Handles raw messages: the body is passed through as bytes, with no deserialization, so any format works (JSON
/// you parse yourself, protobuf, text). Register it with
/// <see cref="IRabbitMqConfigBuilder.AddSubscriber{THandler}(Action{RabbitMqSubscriberOptions})"/>; it is resolved
/// from a new DI scope for every message, so it may depend on scoped services.
/// </summary>
public interface IRabbitMqHandler
{
    /// <summary>
    /// Handles one message. Returning normally acks it unless the handler already settled it through
    /// <paramref name="context"/>; throwing nacks it according to <see cref="RabbitMqSubscriberOptions.RequeueOnError"/>.
    /// </summary>
    /// <param name="context">The raw body and delivery details. <see cref="MessageContext.Body"/> is valid only until this method completes.</param>
    /// <param name="cancellationToken">Canceled when the consumer stops; a message canceled this way is requeued.</param>
    Task HandleAsync(MessageContext context, CancellationToken cancellationToken);
}

/// <summary>
/// Handles messages of one type, deserialized from JSON by the library. Register it with
/// <see cref="IRabbitMqConfigBuilder.AddSubscriber{THandler}(Action{RabbitMqSubscriberOptions})"/>; it is resolved from
/// a new DI scope for every message, so it may depend on scoped services.
/// </summary>
/// <typeparam name="TMessage">The message type. Under Native AOT it needs a source-generated JSON contract.</typeparam>
public interface IRabbitMqHandler<TMessage>
{
    /// <summary>
    /// Handles one message. Returning normally acks it unless the handler already settled it through
    /// <paramref name="context"/>; throwing nacks it according to <see cref="RabbitMqSubscriberOptions.RequeueOnError"/>.
    /// A body that is not valid JSON for <typeparamref name="TMessage"/> never reaches the handler; it is rejected.
    /// </summary>
    /// <param name="context">The deserialized message, its raw body and delivery details.</param>
    /// <param name="cancellationToken">Canceled when the consumer stops; a message canceled this way is requeued.</param>
    Task HandleAsync(MessageContext<TMessage> context, CancellationToken cancellationToken);
}
