namespace KetPsi.MessageBrokers.RabbitMQ;

/// <summary>
/// Handles messages of one type. Register it with
/// <see cref="IRabbitMqConfigBuilder.AddSubscriber{THandler}(Action{RabbitMqSubscriberOptions})"/>; it is resolved from
/// a new DI scope for every message, so it may depend on scoped services.
/// </summary>
/// <typeparam name="TMessage">The message type. Under Native AOT it needs a source-generated JSON contract.</typeparam>
public interface IRabbitMqHandler<TMessage>
{
    /// <summary>
    /// Handles one message. Returning normally acks it unless the handler already settled it through
    /// <paramref name="context"/>; throwing nacks it according to <see cref="RabbitMqSubscriberOptions.RequeueOnError"/>.
    /// </summary>
    /// <param name="context">The message and its delivery details.</param>
    /// <param name="cancellationToken">Canceled when the consumer stops; a message canceled this way is requeued.</param>
    Task HandleAsync(MessageContext<TMessage> context, CancellationToken cancellationToken);
}
