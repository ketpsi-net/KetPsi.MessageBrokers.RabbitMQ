namespace KetPsi.MessageBrokers.RabbitMQ;

/// <summary>Acks or nacks a delivery on the channel it arrived on.</summary>
internal interface IMessageSettler
{
    ValueTask AckAsync(ulong deliveryTag, CancellationToken cancellationToken);

    ValueTask NackAsync(ulong deliveryTag, bool requeue, CancellationToken cancellationToken);
}
