using Microsoft.Extensions.Logging;
using RabbitMQ.Client;

namespace KetPsi.MessageBrokers.RabbitMQ;

/// <summary>Publishes bodies on one long-lived channel, one publish at a time, declaring the topology when the channel opens.</summary>
internal abstract class RabbitMqPublisherBase(
    RabbitMqConnectionProvider connectionProvider,
    RabbitMqPublisherOptions options,
    ILogger logger) : IAsyncDisposable
{
    private readonly SemaphoreSlim _lock = new(1, 1);
    private IChannel? _channel;

    protected RabbitMqPublisherOptions Options => options;

    public async ValueTask DisposeAsync()
    {
        if (_channel is { } channel)
        {
            _channel = null;
            await channel.DisposeAsync().ConfigureAwait(false);
        }

        _lock.Dispose();
    }

    /// <summary>
    /// Publishes <paramref name="body"/>, logging and rethrowing any failure other than the caller's cancellation. The
    /// log names what was published by <paramref name="description"/>, such as the message type name.
    /// </summary>
    protected async Task PublishCoreAsync(
        ReadOnlyMemory<byte> body,
        string routingKey,
        string contentType,
        IReadOnlyDictionary<string, object?>? headers,
        string description,
        CancellationToken cancellationToken)
    {
        ArgumentNullException.ThrowIfNull(routingKey);

        try
        {
            var properties = CreateProperties(contentType, headers);

            await _lock.WaitAsync(cancellationToken).ConfigureAwait(false);
            try
            {
                var channel = await GetChannelAsync(cancellationToken).ConfigureAwait(false);
                await channel.BasicPublishAsync(
                    options.Exchange,
                    routingKey,
                    mandatory: options.Mandatory && options.PublisherConfirms,
                    properties,
                    body,
                    cancellationToken).ConfigureAwait(false);
            }
            finally
            {
                _lock.Release();
            }
        }
        catch (Exception exception) when (exception is not OperationCanceledException || !cancellationToken.IsCancellationRequested)
        {
            logger.LogError(
                exception,
                $"Failed to publish {description:@MessageType} to exchange {options.Exchange:@Exchange} "
                + $"with routing key {routingKey:@RoutingKey}");
            throw;
        }
    }

    private static BasicProperties CreateProperties(string contentType, IReadOnlyDictionary<string, object?>? headers)
    {
        var properties = new BasicProperties
        {
            ContentType = contentType,
            DeliveryMode = DeliveryModes.Persistent,
        };

        if (headers is { Count: > 0 })
        {
            var copy = new Dictionary<string, object?>(headers.Count);
            foreach (var (name, value) in headers)
            {
                copy[name] = value;
            }

            properties.Headers = copy;
        }

        return properties;
    }

    /// <summary>Returns the open channel, or opens one and declares the topology on it. Called under the publish lock.</summary>
    private async Task<IChannel> GetChannelAsync(CancellationToken cancellationToken)
    {
        if (_channel is { IsOpen: true } open)
        {
            return open;
        }

        if (_channel is { } closed)
        {
            _channel = null;
            await closed.DisposeAsync().ConfigureAwait(false);
        }

        var connection = await connectionProvider.GetConnectionAsync(cancellationToken).ConfigureAwait(false);
        var channelOptions = new CreateChannelOptions(
            publisherConfirmationsEnabled: options.PublisherConfirms,
            publisherConfirmationTrackingEnabled: options.PublisherConfirms);
        var channel = await connection.CreateChannelAsync(channelOptions, cancellationToken).ConfigureAwait(false);

        try
        {
            await DeclareTopologyAsync(channel, cancellationToken).ConfigureAwait(false);
        }
        catch
        {
            await channel.DisposeAsync().ConfigureAwait(false);
            throw;
        }

        _channel = channel;
        logger.LogDebug($"Opened RabbitMQ publisher channel for exchange {options.Exchange:@Exchange}");
        return channel;
    }

    private async Task DeclareTopologyAsync(IChannel channel, CancellationToken cancellationToken)
    {
        await channel.ExchangeDeclareAsync(
            options.Exchange,
            options.ExchangeType,
            durable: true,
            autoDelete: false,
            cancellationToken: cancellationToken).ConfigureAwait(false);

        if (string.IsNullOrWhiteSpace(options.Queue))
        {
            return;
        }

        await channel.QueueDeclareAsync(
            options.Queue,
            durable: true,
            exclusive: false,
            autoDelete: false,
            cancellationToken: cancellationToken).ConfigureAwait(false);
        await channel.QueueBindAsync(options.Queue, options.Exchange, options.RoutingKey, cancellationToken: cancellationToken)
            .ConfigureAwait(false);
    }
}
