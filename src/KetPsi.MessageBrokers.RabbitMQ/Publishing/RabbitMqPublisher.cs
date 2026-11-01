using System.Text.Json;
using System.Text.Json.Serialization.Metadata;
using Microsoft.Extensions.Logging;

namespace KetPsi.MessageBrokers.RabbitMQ;

/// <summary>Serializes <typeparamref name="TMessage"/> to JSON and publishes it.</summary>
internal sealed class RabbitMqPublisher<TMessage>(
    RabbitMqConnectionProvider connectionProvider,
    RabbitMqPublisherOptions options,
    JsonTypeInfo<TMessage> typeInfo,
    ILogger logger) : RabbitMqPublisherBase(connectionProvider, options, logger), IRabbitMqPublisher<TMessage>
{
    public Task PublishAsync(TMessage message, CancellationToken cancellationToken = default)
        => PublishAsync(message, Options.RoutingKey, headers: null, cancellationToken);

    public Task PublishAsync(
        TMessage message,
        string routingKey,
        IReadOnlyDictionary<string, object?>? headers,
        CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(message);

        var body = JsonSerializer.SerializeToUtf8Bytes(message, typeInfo);
        return PublishCoreAsync(body, routingKey, RabbitMqContentTypes.Json, headers, typeof(TMessage).Name, cancellationToken);
    }
}
