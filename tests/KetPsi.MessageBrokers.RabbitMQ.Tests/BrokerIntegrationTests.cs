using System.Text;
using System.Text.Json;
using Microsoft.Extensions.DependencyInjection;
using RabbitMQ.Client;

namespace KetPsi.MessageBrokers.RabbitMQ.Tests;

[Collection(BrokerCollection.Name)]
public sealed class BrokerIntegrationTests(RabbitMqFixture broker)
{
    private static CancellationToken CancellationToken => TestContext.Current.CancellationToken;

    [Fact]
    public async Task A_published_message_reaches_the_handler()
    {
        var topology = TestTopology.Create();
        using var host = TestHosts.CreateConnected(broker, rabbit => rabbit
            .AddSubscriber<RecordingHandler>(options => Bind(options, topology))
            .AddPublisher<TestMessage>(options => Target(options, topology)));
        await host.StartAsync(CancellationToken);

        await host.Services.GetRequiredService<IRabbitMqPublisher<TestMessage>>().PublishAsync(new TestMessage("round-trip"), CancellationToken);
        var received = await host.Services.GetRequiredService<MessageRecorder>().NextAsync(CancellationToken);

        Assert.Equal("round-trip", received.Id);
        Assert.False(received.Redelivered);
        await host.StopAsync(CancellationToken);
    }

    [Fact]
    public async Task A_connection_string_without_credentials_uses_the_credential_options_and_expands_variables()
    {
        var topology = TestTopology.Create();
        var brokerUri = new Uri(broker.ConnectionString);
        var credentials = Uri.UnescapeDataString(brokerUri.UserInfo).Split(':', 2);
        var hostVariable = $"KETPSI_RMQ_TEST_HOST_{Guid.NewGuid():N}";
        Environment.SetEnvironmentVariable(hostVariable, brokerUri.Host);
        try
        {
            using var host = TestHosts.CreateConnected(broker, rabbit => rabbit
                .ConfigureConnection(options =>
                {
                    options.ConnectionString = $"amqp://%{hostVariable}%:{brokerUri.Port}";
                    options.Username = credentials[0];
                    options.Password = credentials[1];
                })
                .AddSubscriber<RecordingHandler>(options => Bind(options, topology))
                .AddPublisher<TestMessage>(options => Target(options, topology)));
            await host.StartAsync(CancellationToken);

            await host.Services.GetRequiredService<IRabbitMqPublisher<TestMessage>>().PublishAsync(new TestMessage("no-credentials"), CancellationToken);
            var received = await host.Services.GetRequiredService<MessageRecorder>().NextAsync(CancellationToken);

            Assert.Equal("no-credentials", received.Id);
            await host.StopAsync(CancellationToken);
        }
        finally
        {
            Environment.SetEnvironmentVariable(hostVariable, null);
        }
    }

    [Fact]
    public async Task A_failing_handler_without_requeue_dead_letters_the_message()
    {
        var topology = TestTopology.Create();
        await broker.DeclareDeadLetterAsync(topology, CancellationToken);
        using var host = TestHosts.CreateConnected(broker, rabbit => rabbit
            .AddSubscriber<ThrowingHandler>(options =>
            {
                Bind(options, topology);
                options.RequeueOnError = false;
                options.DeadLetterExchange = topology.DeadLetterExchange;
            })
            .AddPublisher<TestMessage>(options => Target(options, topology)));
        await host.StartAsync(CancellationToken);

        await host.Services.GetRequiredService<IRabbitMqPublisher<TestMessage>>().PublishAsync(new TestMessage("poison"), CancellationToken);
        var deadLettered = await broker.WaitForMessageAsync(topology.DeadLetterQueue, CancellationToken);

        Assert.NotNull(deadLettered);
        Assert.Equal("poison", JsonSerializer.Deserialize(deadLettered.Body.Span, TestJsonContext.Default.TestMessage)!.Id);
        Assert.Equal(1, host.Services.GetRequiredService<MessageRecorder>().Count);
        await host.StopAsync(CancellationToken);
    }

    [Fact]
    public async Task Malformed_json_is_rejected_without_requeue()
    {
        var topology = TestTopology.Create();
        await broker.DeclareDeadLetterAsync(topology, CancellationToken);
        using var host = TestHosts.CreateConnected(broker, rabbit => rabbit
            .AddSubscriber<RecordingHandler>(options =>
            {
                options.Queue = topology.Queue;
                options.DeadLetterExchange = topology.DeadLetterExchange;
                options.RequeueOnError = true;
            }));
        await host.StartAsync(CancellationToken);

        // Published raw to the default exchange, bypassing the typed publisher.
        await using (var channel = await broker.CreateChannelAsync(CancellationToken))
        {
            await channel.BasicPublishAsync(string.Empty, topology.Queue, Encoding.UTF8.GetBytes("{not json"), CancellationToken);
        }

        // A requeued message would loop back to the consumer and never reach the dead-letter queue.
        var deadLettered = await broker.WaitForMessageAsync(topology.DeadLetterQueue, CancellationToken);

        Assert.NotNull(deadLettered);
        Assert.Equal("{not json", Encoding.UTF8.GetString(deadLettered.Body.Span));
        Assert.Equal("rejected", DeathReason(deadLettered.BasicProperties));
        Assert.Equal(0, host.Services.GetRequiredService<MessageRecorder>().Count);
        await host.StopAsync(CancellationToken);
    }

    [Fact]
    public async Task The_connection_provider_returns_the_shared_open_connection()
    {
        using var host = TestHosts.CreateConnected(broker, _ => { });
        await host.StartAsync(CancellationToken);
        var provider = host.Services.GetRequiredService<IRabbitMqConnectionProvider>();

        var connection = await provider.GetConnectionAsync(CancellationToken);

        Assert.True(connection.IsOpen);
        Assert.Same(connection, await provider.GetConnectionAsync(CancellationToken));
        await host.StopAsync(CancellationToken);
    }

    [Fact]
    public async Task A_raw_handler_receives_any_body_unchanged()
    {
        var topology = TestTopology.Create();
        using var host = TestHosts.CreateConnected(broker, rabbit => rabbit
            .AddSubscriber<RawRecordingHandler>(options => options.Queue = topology.Queue));
        await host.StartAsync(CancellationToken);

        // Not JSON: a typed handler would reject it, a raw handler gets the bytes as published.
        var body = Encoding.UTF8.GetBytes("{not json, just text");
        await using (var channel = await broker.CreateChannelAsync(CancellationToken))
        {
            var properties = new BasicProperties { ContentType = "text/plain" };
            await channel.BasicPublishAsync(string.Empty, topology.Queue, mandatory: false, properties, body, CancellationToken);
        }

        var received = await host.Services.GetRequiredService<RawMessageRecorder>().NextAsync(CancellationToken);

        Assert.Equal(body, received.Body);
        Assert.Equal("text/plain", received.ContentType);
        Assert.Equal(topology.Queue, received.RoutingKey);
        await host.StopAsync(CancellationToken);
    }

    [Fact]
    public async Task The_raw_publisher_sends_the_body_and_content_type_as_given()
    {
        var topology = TestTopology.Create();
        using var host = TestHosts.CreateConnected(broker, rabbit => rabbit
            .AddSubscriber<RawRecordingHandler>(options => Bind(options, topology))
            .AddPublisher(options => Target(options, topology)));
        await host.StartAsync(CancellationToken);

        var publisher = host.Services.GetRequiredService<IRabbitMqPublisher>();
        var recorder = host.Services.GetRequiredService<RawMessageRecorder>();
        byte[] protobufLike = [0x08, 0x96, 0x01];

        await publisher.PublishAsync(protobufLike, topology.RoutingKey, "application/x-protobuf", headers: null, CancellationToken);
        var explicitType = await recorder.NextAsync(CancellationToken);
        await publisher.PublishAsync(protobufLike, CancellationToken);
        var defaultType = await recorder.NextAsync(CancellationToken);

        Assert.Equal(protobufLike, explicitType.Body);
        Assert.Equal("application/x-protobuf", explicitType.ContentType);
        Assert.Equal(protobufLike, defaultType.Body);
        Assert.Equal("application/octet-stream", defaultType.ContentType);
        await host.StopAsync(CancellationToken);
    }

    [Fact]
    public async Task Json_published_raw_reaches_a_typed_handler()
    {
        var topology = TestTopology.Create();
        using var host = TestHosts.CreateConnected(broker, rabbit => rabbit
            .AddSubscriber<RecordingHandler>(options => Bind(options, topology))
            .AddPublisher(options => Target(options, topology)));
        await host.StartAsync(CancellationToken);

        var body = JsonSerializer.SerializeToUtf8Bytes(new TestMessage("raw-json"), TestJsonContext.Default.TestMessage);
        await host.Services.GetRequiredService<IRabbitMqPublisher>()
            .PublishAsync(body, topology.RoutingKey, "application/json", headers: null, CancellationToken);
        var received = await host.Services.GetRequiredService<MessageRecorder>().NextAsync(CancellationToken);

        Assert.Equal("raw-json", received.Id);
        await host.StopAsync(CancellationToken);
    }

    [Fact]
    public async Task A_typed_handler_can_read_the_raw_body_after_awaiting()
    {
        var topology = TestTopology.Create();
        using var host = TestHosts.CreateConnected(broker, rabbit => rabbit
            .AddSubscriber<DelayedBodyHandler>(options =>
            {
                Bind(options, topology);
                options.ConsumerDispatchConcurrency = 4;
            })
            .AddPublisher<TestMessage>(options => Target(options, topology)));
        await host.StartAsync(CancellationToken);

        // Several messages in flight at once, so a body buffer returned too early would be overwritten by another.
        var publisher = host.Services.GetRequiredService<IRabbitMqPublisher<TestMessage>>();
        var sent = Enumerable.Range(0, 8).Select(i => new TestMessage($"body-{i}")).ToArray();
        foreach (var message in sent)
        {
            await publisher.PublishAsync(message, CancellationToken);
        }

        var recorder = host.Services.GetRequiredService<RawMessageRecorder>();
        var received = new List<TestMessage>();
        for (var i = 0; i < sent.Length; i++)
        {
            var raw = await recorder.NextAsync(CancellationToken);
            received.Add(JsonSerializer.Deserialize(raw.Body, TestJsonContext.Default.TestMessage)!);
        }

        Assert.Equal(sent.Select(m => m.Id).Order(), received.Select(m => m.Id).Order());
        await host.StopAsync(CancellationToken);
    }

    [Fact]
    public async Task Each_message_gets_its_own_scope()
    {
        var topology = TestTopology.Create();
        using var host = TestHosts.CreateConnected(broker, rabbit => rabbit
            .AddSubscriber<RecordingHandler>(options => Bind(options, topology))
            .AddPublisher<TestMessage>(options => Target(options, topology)));
        await host.StartAsync(CancellationToken);
        var publisher = host.Services.GetRequiredService<IRabbitMqPublisher<TestMessage>>();
        var recorder = host.Services.GetRequiredService<MessageRecorder>();

        await publisher.PublishAsync(new TestMessage("first"), CancellationToken);
        await publisher.PublishAsync(new TestMessage("second"), CancellationToken);
        var first = await recorder.NextAsync(CancellationToken);
        var second = await recorder.NextAsync(CancellationToken);

        Assert.NotEqual(Guid.Empty, first.ProbeId);
        Assert.NotEqual(first.ProbeId, second.ProbeId);
        await host.StopAsync(CancellationToken);
    }

    [Fact]
    public async Task Graceful_shutdown_requeues_an_in_flight_message()
    {
        var topology = TestTopology.Create();
        using var host = TestHosts.CreateConnected(broker, rabbit => rabbit
            .AddSubscriber<BlockingHandler>(options => Bind(options, topology))
            .AddPublisher<TestMessage>(options => Target(options, topology)));
        await host.StartAsync(CancellationToken);

        await host.Services.GetRequiredService<IRabbitMqPublisher<TestMessage>>().PublishAsync(new TestMessage("in-flight"), CancellationToken);
        await host.Services.GetRequiredService<MessageRecorder>().NextAsync(CancellationToken);
        await host.StopAsync(CancellationToken);

        var requeued = await broker.WaitForMessageAsync(topology.Queue, CancellationToken);

        Assert.NotNull(requeued);
        Assert.True(requeued.Redelivered);
        Assert.Equal("in-flight", JsonSerializer.Deserialize(requeued.Body.Span, TestJsonContext.Default.TestMessage)!.Id);
    }

    private static void Bind(RabbitMqSubscriberOptions options, TestTopology topology)
    {
        options.Queue = topology.Queue;
        options.Exchange = topology.Exchange;
        options.ExchangeType = RabbitMqExchangeType.Direct;
        options.RoutingKey = topology.RoutingKey;
    }

    private static void Target(RabbitMqPublisherOptions options, TestTopology topology)
    {
        options.Exchange = topology.Exchange;
        options.RoutingKey = topology.RoutingKey;
        options.Mandatory = true;
    }

    /// <summary>The <c>reason</c> of the first <c>x-death</c> entry the broker adds when dead-lettering.</summary>
    private static string? DeathReason(IReadOnlyBasicProperties properties)
    {
        if (properties.Headers?.TryGetValue("x-death", out var deaths) is not true
            || deaths is not IList<object?> { Count: > 0 } entries
            || entries[0] is not IDictionary<string, object?> entry
            || !entry.TryGetValue("reason", out var reason))
        {
            return null;
        }

        return reason is byte[] bytes ? Encoding.UTF8.GetString(bytes) : reason?.ToString();
    }
}
