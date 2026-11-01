using Microsoft.Extensions.Logging;

namespace KetPsi.MessageBrokers.RabbitMQ;

/// <summary>Publishes bodies as given, without serializing anything.</summary>
internal sealed class RabbitMqRawPublisher(
    RabbitMqConnectionProvider connectionProvider,
    RabbitMqPublisherOptions options,
    ILogger logger) : RabbitMqPublisherBase(connectionProvider, options, logger), IRabbitMqPublisher
{
    private const string Description = "raw message";

    public Task PublishAsync(ReadOnlyMemory<byte> body, CancellationToken cancellationToken = default)
        => PublishCoreAsync(body, Options.RoutingKey, RabbitMqContentTypes.OctetStream, headers: null, Description, cancellationToken);

    public Task PublishAsync(
        ReadOnlyMemory<byte> body,
        string routingKey,
        string? contentType,
        IReadOnlyDictionary<string, object?>? headers,
        CancellationToken cancellationToken = default)
        => PublishCoreAsync(body, routingKey, contentType ?? RabbitMqContentTypes.OctetStream, headers, Description, cancellationToken);
}
