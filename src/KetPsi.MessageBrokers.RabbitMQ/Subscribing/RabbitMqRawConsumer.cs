using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;
using RabbitMQ.Client.Events;

namespace KetPsi.MessageBrokers.RabbitMQ;

/// <summary>Dispatches each message's raw body, without deserializing it, to a scoped <typeparamref name="THandler"/>.</summary>
/// <typeparam name="THandler">
/// A class implementing <see cref="IRabbitMqHandler"/>. Not constrained to it, so that
/// <see cref="RabbitMqConfigBuilder"/> can register handlers it has only checked at runtime.
/// </typeparam>
internal sealed class RabbitMqRawConsumer<THandler>(
    RabbitMqConnectionProvider connectionProvider,
    IServiceScopeFactory scopeFactory,
    RabbitMqSubscriberOptions options,
    ILogger logger) : RabbitMqConsumerBase<MessageContext>(connectionProvider, scopeFactory, options, logger)
    where THandler : class
{
    protected override MessageContext CreateContext(BasicDeliverEventArgs args, IMessageSettler settler)
        => new(settler, args.Body, args.DeliveryTag, args.Redelivered, args.Exchange, args.RoutingKey, args.BasicProperties);

    protected override Task HandleAsync(IServiceProvider services, MessageContext context, CancellationToken cancellationToken)
        => ((IRabbitMqHandler)services.GetRequiredService<THandler>()).HandleAsync(context, cancellationToken);
}
