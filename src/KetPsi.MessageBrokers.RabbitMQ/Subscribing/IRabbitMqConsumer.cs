namespace KetPsi.MessageBrokers.RabbitMQ;

/// <summary>One subscriber's consumer, started and stopped by <see cref="RabbitMqConsumerHostedService"/>.</summary>
internal interface IRabbitMqConsumer : IAsyncDisposable
{
    string Queue { get; }

    Task StartAsync(CancellationToken cancellationToken);

    Task StopAsync(CancellationToken cancellationToken);
}
