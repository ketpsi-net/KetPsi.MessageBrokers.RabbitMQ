using Microsoft.Extensions.Logging.Abstractions;

namespace KetPsi.MessageBrokers.RabbitMQ.Tests;

public sealed class ConsumerHostedServiceTests
{
    private static CancellationToken CancellationToken => TestContext.Current.CancellationToken;

    [Fact]
    public async Task A_slow_consumer_does_not_hold_up_the_others()
    {
        var fastStarted = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        // The slow consumer comes first and only finishes once the fast one has started, so starting one at a time never ends.
        var service = CreateService(
            new FakeConsumer("slow", cancellationToken => fastStarted.Task.WaitAsync(cancellationToken)),
            new FakeConsumer("fast", _ =>
            {
                fastStarted.SetResult();
                return Task.CompletedTask;
            }));

        await service.StartAsync(CancellationToken).WaitAsync(TimeSpan.FromSeconds(5), CancellationToken);
    }

    [Fact]
    public async Task A_failing_consumer_cancels_the_others_and_fails_startup()
    {
        var slow = new FakeConsumer("slow", cancellationToken => Task.Delay(Timeout.Infinite, cancellationToken));
        var service = CreateService(
            slow,
            new FakeConsumer("failing", async _ =>
            {
                await Task.Yield();
                throw new InvalidOperationException("Start failure for tests.");
            }));

        var exception = await Assert.ThrowsAsync<InvalidOperationException>(() => service.StartAsync(CancellationToken));

        Assert.Equal("Start failure for tests.", exception.Message);
        Assert.True(slow.StartCanceled);
    }

    [Fact]
    public async Task Stopping_stops_every_consumer_even_when_one_fails()
    {
        var stopped = new FakeConsumer("stopped", _ => Task.CompletedTask);
        var service = CreateService(
            new FakeConsumer("failing", _ => Task.CompletedTask, stop: _ => throw new InvalidOperationException("Stop failure for tests.")),
            stopped);

        await service.StopAsync(CancellationToken);

        Assert.True(stopped.Stopped);
    }

    private static RabbitMqConsumerHostedService CreateService(params IRabbitMqConsumer[] consumers)
        => new(consumers, NullLogger<RabbitMqConsumerHostedService>.Instance);

    private sealed class FakeConsumer(
        string queue,
        Func<CancellationToken, Task> start,
        Func<CancellationToken, Task>? stop = null) : IRabbitMqConsumer
    {
        public string Queue => queue;

        public bool StartCanceled { get; private set; }

        public bool Stopped { get; private set; }

        public async Task StartAsync(CancellationToken cancellationToken)
        {
            try
            {
                await start(cancellationToken);
            }
            catch (OperationCanceledException)
            {
                StartCanceled = true;
                throw;
            }
        }

        public async Task StopAsync(CancellationToken cancellationToken)
        {
            await (stop ?? (_ => Task.CompletedTask))(cancellationToken);
            Stopped = true;
        }

        public ValueTask DisposeAsync() => ValueTask.CompletedTask;
    }
}
