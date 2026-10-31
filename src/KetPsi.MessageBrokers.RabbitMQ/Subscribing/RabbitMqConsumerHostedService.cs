using System.Runtime.ExceptionServices;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;

namespace KetPsi.MessageBrokers.RabbitMQ;

/// <summary>
/// Starts every registered consumer at once with the host, so a slow consumer does not hold up the others. The first
/// consumer that fails to start cancels the rest and fails host startup.
/// </summary>
internal sealed class RabbitMqConsumerHostedService(
    IEnumerable<IRabbitMqConsumer> consumers,
    ILogger<RabbitMqConsumerHostedService> logger) : IHostedService
{
    public async Task StartAsync(CancellationToken cancellationToken)
    {
        using var startup = CancellationTokenSource.CreateLinkedTokenSource(cancellationToken);
        var starts = consumers.Select(consumer => StartConsumerAsync(consumer, startup)).ToArray();

        try
        {
            await Task.WhenAll(starts).ConfigureAwait(false);
        }
        catch when (Array.Find(starts, start => start.IsFaulted) is { Exception.InnerException: { } failure })
        {
            // Report the failure that stopped startup, not a consumer canceled because of it.
            ExceptionDispatchInfo.Throw(failure);
        }
    }

    public Task StopAsync(CancellationToken cancellationToken)
        => Task.WhenAll(consumers.Select(consumer => StopConsumerAsync(consumer, cancellationToken)));

    private async Task StartConsumerAsync(IRabbitMqConsumer consumer, CancellationTokenSource startup)
    {
        try
        {
            await consumer.StartAsync(startup.Token).ConfigureAwait(false);
        }
        catch (OperationCanceledException) when (startup.IsCancellationRequested)
        {
            throw;
        }
        catch (Exception exception)
        {
            logger.LogCritical(exception, $"Failed to start the RabbitMQ consumer for queue {consumer.Queue:@Queue}");
            await startup.CancelAsync().ConfigureAwait(false);
            throw;
        }
    }

    private async Task StopConsumerAsync(IRabbitMqConsumer consumer, CancellationToken cancellationToken)
    {
        try
        {
            await consumer.StopAsync(cancellationToken).ConfigureAwait(false);
        }
        catch (Exception exception)
        {
            logger.LogError(exception, $"Failed to stop the RabbitMQ consumer for queue {consumer.Queue:@Queue}");
        }
    }
}
