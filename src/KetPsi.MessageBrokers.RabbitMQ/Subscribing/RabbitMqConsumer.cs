using System.Text.Json;
using System.Text.Json.Serialization.Metadata;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;
using RabbitMQ.Client.Events;

namespace KetPsi.MessageBrokers.RabbitMQ;

/// <summary>Deserializes each message to <typeparamref name="TMessage"/> and dispatches it to a scoped <typeparamref name="THandler"/>.</summary>
internal sealed class RabbitMqConsumer<THandler, TMessage>(
    RabbitMqConnectionProvider connectionProvider,
    IServiceScopeFactory scopeFactory,
    RabbitMqSubscriberOptions options,
    JsonTypeInfo<TMessage> typeInfo,
    ILogger logger) : RabbitMqConsumerBase<MessageContext<TMessage>>(connectionProvider, scopeFactory, options, logger)
    where THandler : class, IRabbitMqHandler<TMessage>
{
    protected override MessageContext<TMessage>? CreateContext(BasicDeliverEventArgs args, IMessageSettler settler)
    {
        TMessage? message;
        try
        {
            message = JsonSerializer.Deserialize(args.Body.Span, typeInfo);
        }
        catch (JsonException exception)
        {
            Logger.LogError(
                exception,
                $"Rejecting message {args.DeliveryTag:@DeliveryTag} from queue {Options.Queue:@Queue}: "
                + $"the body is not valid JSON for {typeof(TMessage).Name:@MessageType}");
            return null;
        }

        if (message is null)
        {
            Logger.LogError(
                $"Rejecting message {args.DeliveryTag:@DeliveryTag} from queue {Options.Queue:@Queue}: "
                + $"the body deserialized to a null {typeof(TMessage).Name:@MessageType}");
            return null;
        }

        return new MessageContext<TMessage>(
            message,
            settler,
            args.Body,
            args.DeliveryTag,
            args.Redelivered,
            args.Exchange,
            args.RoutingKey,
            args.BasicProperties);
    }

    protected override Task HandleAsync(IServiceProvider services, MessageContext<TMessage> context, CancellationToken cancellationToken)
        => services.GetRequiredService<THandler>().HandleAsync(context, cancellationToken);
}
