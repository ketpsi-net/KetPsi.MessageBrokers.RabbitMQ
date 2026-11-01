using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;
using RabbitMQ.Client;
using RabbitMQ.Client.Events;

namespace KetPsi.MessageBrokers.RabbitMQ;

/// <summary>
/// Consumes one queue on its own channel: declares the topology, runs a handler from a new DI scope per message and
/// settles the message from the outcome. Derived consumers decide how a delivery becomes a context and which handler
/// runs.
/// </summary>
/// <typeparam name="TContext">The context the handler receives.</typeparam>
internal abstract class RabbitMqConsumerBase<TContext>(
    RabbitMqConnectionProvider connectionProvider,
    IServiceScopeFactory scopeFactory,
    RabbitMqSubscriberOptions options,
    ILogger logger) : IRabbitMqConsumer, IMessageSettler
    where TContext : MessageContext
{
    private readonly CancellationTokenSource _stopping = new();
    private readonly TaskCompletionSource _drained = new(TaskCreationOptions.RunContinuationsAsynchronously);
    private IChannel? _channel;
    private string? _consumerTag;
    private int _inFlight;

    public string Queue => options.Queue;

    protected RabbitMqSubscriberOptions Options => options;

    protected ILogger Logger => logger;

    private IChannel Channel => _channel ?? throw new InvalidOperationException($"The consumer for queue '{options.Queue}' has not started.");

    public async Task StartAsync(CancellationToken cancellationToken)
    {
        var connection = await connectionProvider.GetConnectionAsync(cancellationToken).ConfigureAwait(false);
        var channelOptions = new CreateChannelOptions(
            publisherConfirmationsEnabled: false,
            publisherConfirmationTrackingEnabled: false,
            consumerDispatchConcurrency: options.ConsumerDispatchConcurrency);
        var channel = await connection.CreateChannelAsync(channelOptions, cancellationToken).ConfigureAwait(false);
        _channel = channel;

        Dictionary<string, object?>? queueArguments = null;
        if (!string.IsNullOrWhiteSpace(options.DeadLetterExchange))
        {
            queueArguments = new() { [RabbitMqQueueArguments.DeadLetterExchange] = options.DeadLetterExchange };
        }

        await channel.QueueDeclareAsync(
            options.Queue,
            durable: true,
            exclusive: false,
            autoDelete: false,
            arguments: queueArguments,
            cancellationToken: cancellationToken).ConfigureAwait(false);

        if (!string.IsNullOrEmpty(options.Exchange))
        {
            if (!string.IsNullOrEmpty(options.ExchangeType))
            {
                await channel.ExchangeDeclareAsync(
                    options.Exchange,
                    options.ExchangeType,
                    durable: true,
                    autoDelete: false,
                    cancellationToken: cancellationToken).ConfigureAwait(false);
            }

            await channel.QueueBindAsync(options.Queue, options.Exchange, options.RoutingKey, cancellationToken: cancellationToken)
                .ConfigureAwait(false);
        }

        if (options.PrefetchCount is { } prefetchCount)
        {
            await channel.BasicQosAsync(prefetchSize: 0, prefetchCount, global: false, cancellationToken).ConfigureAwait(false);
        }

        // Attach the handler before consuming, so no delivery arrives without one.
        var consumer = new AsyncEventingBasicConsumer(channel);
        consumer.ReceivedAsync += OnReceivedAsync;
        _consumerTag = await channel.BasicConsumeAsync(options.Queue, autoAck: false, consumer, cancellationToken).ConfigureAwait(false);

        logger.LogInformation($"RabbitMQ consumer started on queue {options.Queue:@Queue}");
    }

    public async Task StopAsync(CancellationToken cancellationToken)
    {
        if (_channel is not { } channel)
        {
            return;
        }

        try
        {
            if (_consumerTag is { } consumerTag && channel.IsOpen)
            {
                await channel.BasicCancelAsync(consumerTag, noWait: false, cancellationToken).ConfigureAwait(false);
            }
        }
        finally
        {
            await _stopping.CancelAsync().ConfigureAwait(false);
        }

        await WaitForInFlightAsync(cancellationToken).ConfigureAwait(false);

        if (channel.IsOpen)
        {
            await channel.CloseAsync(cancellationToken).ConfigureAwait(false);
        }

        logger.LogInformation($"RabbitMQ consumer stopped on queue {options.Queue:@Queue}");
    }

    public async ValueTask DisposeAsync()
    {
        if (_channel is { } channel)
        {
            _channel = null;
            await channel.DisposeAsync().ConfigureAwait(false);
        }

        _stopping.Dispose();
    }

    ValueTask IMessageSettler.AckAsync(ulong deliveryTag, CancellationToken cancellationToken)
        => Channel.BasicAckAsync(deliveryTag, multiple: false, cancellationToken);

    ValueTask IMessageSettler.NackAsync(ulong deliveryTag, bool requeue, CancellationToken cancellationToken)
        => Channel.BasicNackAsync(deliveryTag, multiple: false, requeue, cancellationToken);

    /// <summary>
    /// Turns a delivery into the handler's context, settling through <paramref name="settler"/>. Returns null to
    /// reject the message without requeue, after logging why.
    /// </summary>
    protected abstract TContext? CreateContext(BasicDeliverEventArgs args, IMessageSettler settler);

    /// <summary>Resolves the handler from <paramref name="services"/> and runs it.</summary>
    protected abstract Task HandleAsync(IServiceProvider services, TContext context, CancellationToken cancellationToken);

    private async Task OnReceivedAsync(object sender, BasicDeliverEventArgs args)
    {
        Interlocked.Increment(ref _inFlight);
        try
        {
            await ProcessAsync(args).ConfigureAwait(false);
        }
        catch (Exception exception)
        {
            // Settling failed, typically because the channel closed. The broker requeues unacked messages itself.
            logger.LogError(exception, $"Failed to settle message {args.DeliveryTag:@DeliveryTag} on queue {options.Queue:@Queue}");
        }
        finally
        {
            if (Interlocked.Decrement(ref _inFlight) == 0 && _stopping.IsCancellationRequested)
            {
                _drained.TrySetResult();
            }
        }
    }

    // RabbitMQ.Client returns the body buffer once this delivery callback completes, so the context's Body is valid
    // for as long as the handler runs, and no longer.
    private async Task ProcessAsync(BasicDeliverEventArgs args)
    {
        if (CreateContext(args, this) is not { } context)
        {
            await Channel.BasicRejectAsync(args.DeliveryTag, requeue: false, CancellationToken.None).ConfigureAwait(false);
            return;
        }

        var stoppingToken = _stopping.Token;
        try
        {
            var scope = scopeFactory.CreateAsyncScope();
            await using (scope.ConfigureAwait(false))
            {
                await HandleAsync(scope.ServiceProvider, context, stoppingToken).ConfigureAwait(false);
            }
        }
        catch (OperationCanceledException) when (stoppingToken.IsCancellationRequested)
        {
            logger.LogWarning(
                $"Handler for message {args.DeliveryTag:@DeliveryTag} on queue {options.Queue:@Queue} was canceled by shutdown; requeuing it");
            await context.NackAsync(requeue: true, CancellationToken.None).ConfigureAwait(false);
            return;
        }
        catch (Exception exception)
        {
            logger.LogError(
                exception,
                $"Handler failed for message {args.DeliveryTag:@DeliveryTag} on queue {options.Queue:@Queue}; "
                + $"requeue: {options.RequeueOnError:@Requeue}");
            await context.NackAsync(options.RequeueOnError, CancellationToken.None).ConfigureAwait(false);
            return;
        }

        // Outside the handler's try: a failed ack is a settle failure, not a handler failure.
        await context.AckAsync(CancellationToken.None).ConfigureAwait(false);
    }

    private async Task WaitForInFlightAsync(CancellationToken cancellationToken)
    {
        if (Volatile.Read(ref _inFlight) == 0)
        {
            _drained.TrySetResult();
        }

        try
        {
            await _drained.Task.WaitAsync(cancellationToken).ConfigureAwait(false);
        }
        catch (OperationCanceledException) when (cancellationToken.IsCancellationRequested)
        {
            // The host's shutdown timeout elapsed. Closing the channel makes the broker requeue anything still unacked.
        }
    }
}
