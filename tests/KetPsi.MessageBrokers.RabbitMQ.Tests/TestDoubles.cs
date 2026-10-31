using System.Text.Json.Serialization;
using System.Threading.Channels;

namespace KetPsi.MessageBrokers.RabbitMQ.Tests;

internal sealed record TestMessage([property: JsonPropertyName("id")] string Id);

/// <summary>Deliberately missing from <see cref="TestJsonContext"/>.</summary>
internal sealed record UnregisteredMessage([property: JsonPropertyName("id")] string Id);

internal sealed record OtherMessage([property: JsonPropertyName("value")] int Value);

/// <summary>In no context and without property names, so it shows the reflection fallback and its naming policy.</summary>
internal sealed record PlainMessage(string OrderId);

[JsonSourceGenerationOptions(PropertyNamingPolicy = JsonKnownNamingPolicy.CamelCase)]
[JsonSerializable(typeof(TestMessage))]
internal sealed partial class TestJsonContext : JsonSerializerContext;

[JsonSerializable(typeof(OtherMessage))]
internal sealed partial class OtherJsonContext : JsonSerializerContext;

internal sealed record RecordedMessage(string Id, Guid ProbeId, bool Redelivered);

/// <summary>Collects what handlers saw, so a test can wait for it.</summary>
internal sealed class MessageRecorder
{
    private readonly Channel<RecordedMessage> _messages = Channel.CreateUnbounded<RecordedMessage>();
    private int _count;

    public int Count => Volatile.Read(ref _count);

    public void Record(MessageContext<TestMessage> context, Guid probeId)
    {
        Interlocked.Increment(ref _count);
        _messages.Writer.TryWrite(new RecordedMessage(context.Message.Id, probeId, context.Redelivered));
    }

    public async Task<RecordedMessage> NextAsync(CancellationToken cancellationToken)
    {
        using var timeout = CancellationTokenSource.CreateLinkedTokenSource(cancellationToken);
        timeout.CancelAfter(TimeSpan.FromSeconds(15));
        return await _messages.Reader.ReadAsync(timeout.Token);
    }
}

/// <summary>A scoped dependency whose identity shows which DI scope a handler ran in.</summary>
internal sealed class ScopedProbe
{
    public Guid Id { get; } = Guid.NewGuid();
}

internal sealed class RecordingHandler(MessageRecorder recorder, ScopedProbe probe) : IRabbitMqHandler<TestMessage>
{
    public Task HandleAsync(MessageContext<TestMessage> context, CancellationToken cancellationToken)
    {
        recorder.Record(context, probe.Id);
        return Task.CompletedTask;
    }
}

internal sealed class ThrowingHandler(MessageRecorder recorder) : IRabbitMqHandler<TestMessage>
{
    public Task HandleAsync(MessageContext<TestMessage> context, CancellationToken cancellationToken)
    {
        recorder.Record(context, Guid.Empty);
        throw new InvalidOperationException("Handler failure for tests.");
    }
}

/// <summary>Never finishes on its own; only shutdown cancellation ends it.</summary>
internal sealed class BlockingHandler(MessageRecorder recorder) : IRabbitMqHandler<TestMessage>
{
    public async Task HandleAsync(MessageContext<TestMessage> context, CancellationToken cancellationToken)
    {
        recorder.Record(context, Guid.Empty);
        await Task.Delay(Timeout.Infinite, cancellationToken);
    }
}

internal sealed class NoOpHandler : IRabbitMqHandler<TestMessage>
{
    public Task HandleAsync(MessageContext<TestMessage> context, CancellationToken cancellationToken) => Task.CompletedTask;
}

internal sealed class RecordingSettler : IMessageSettler
{
    private int _acks;
    private int _nacks;

    public int Acks => Volatile.Read(ref _acks);

    public int Nacks => Volatile.Read(ref _nacks);

    public bool? LastRequeue { get; private set; }

    public ValueTask AckAsync(ulong deliveryTag, CancellationToken cancellationToken)
    {
        Interlocked.Increment(ref _acks);
        return ValueTask.CompletedTask;
    }

    public ValueTask NackAsync(ulong deliveryTag, bool requeue, CancellationToken cancellationToken)
    {
        Interlocked.Increment(ref _nacks);
        LastRequeue = requeue;
        return ValueTask.CompletedTask;
    }
}
