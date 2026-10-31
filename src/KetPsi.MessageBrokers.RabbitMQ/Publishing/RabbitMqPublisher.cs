using System.Text.Json;
using System.Text.Json.Serialization.Metadata;
using Microsoft.Extensions.Logging;
using RabbitMQ.Client;

namespace KetPsi.MessageBrokers.RabbitMQ;

/// <summary>Publishes <typeparamref name="TMessage"/> on one long-lived channel, one publish at a time.</summary>
internal sealed class RabbitMqPublisher<TMessage>(
    RabbitMqConnectionProvider connectionProvider,
    RabbitMqPublisherOptions options,
    JsonTypeInfo<TMessage> typeInfo,
    ILogger logger) : IRabbitMqPublisher<TMessage>, IAsyncDisposable
{
    private readonly SemaphoreSlim _lock = new(1, 1);
    private IChannel? _channel;

    public Task PublishAsync(TMessage message, CancellationToken cancellationToken = default)
        => PublishAsync(message, options.RoutingKey, headers: null, cancellationToken);

    public async Task PublishAsync(
        TMessage message,
        string routingKey,
        IReadOnlyDictionary<string, object?>? headers,
        CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(message);
        ArgumentNullException.ThrowIfNull(routingKey);

        try
        {
            var body = JsonSerializer.SerializeToUtf8Bytes(message, typeInfo);
            var properties = CreateProperties(headers);

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
                $"Failed to publish {typeof(TMessage).Name:@MessageType} to exchange {options.Exchange:@Exchange} "
                + $"with routing key {routingKey:@RoutingKey}");
            throw;
        }
    }

    public async ValueTask DisposeAsync()
    {
        if (_channel is { } channel)
        {
            _channel = null;
            await channel.DisposeAsync().ConfigureAwait(false);
        }

        _lock.Dispose();
    }

    private static BasicProperties CreateProperties(IReadOnlyDictionary<string, object?>? headers)
    {
        var properties = new BasicProperties
        {
            ContentType = RabbitMqContentTypes.Json,
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
