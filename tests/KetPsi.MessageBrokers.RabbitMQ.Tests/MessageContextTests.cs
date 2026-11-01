using RabbitMQ.Client;

namespace KetPsi.MessageBrokers.RabbitMQ.Tests;

public sealed class MessageContextTests
{
    private readonly RecordingSettler _settler = new();

    [Fact]
    public async Task AckAsync_twice_acks_once()
    {
        var context = CreateContext();

        await context.AckAsync(TestContext.Current.CancellationToken);
        await context.AckAsync(TestContext.Current.CancellationToken);

        Assert.Equal(1, _settler.Acks);
        Assert.True(context.IsSettled);
    }

    [Fact]
    public async Task NackAsync_after_AckAsync_does_nothing()
    {
        var context = CreateContext();

        await context.AckAsync(TestContext.Current.CancellationToken);
        await context.NackAsync(requeue: true, TestContext.Current.CancellationToken);

        Assert.Equal(1, _settler.Acks);
        Assert.Equal(0, _settler.Nacks);
    }

    [Fact]
    public async Task NackAsync_passes_requeue_and_blocks_a_later_ack()
    {
        var context = CreateContext();

        await context.NackAsync(requeue: false, TestContext.Current.CancellationToken);
        await context.AckAsync(TestContext.Current.CancellationToken);

        Assert.Equal(1, _settler.Nacks);
        Assert.False(_settler.LastRequeue);
        Assert.Equal(0, _settler.Acks);
    }

    [Fact]
    public async Task Concurrent_settles_reach_the_channel_once()
    {
        var context = CreateContext();
        using var start = new ManualResetEventSlim();

        var settles = Enumerable.Range(0, 64)
            .Select(i => Task.Run(
                async () =>
                {
                    start.Wait(TestContext.Current.CancellationToken);
                    if (i % 2 == 0)
                    {
                        await context.AckAsync(TestContext.Current.CancellationToken);
                    }
                    else
                    {
                        await context.NackAsync(requeue: true, TestContext.Current.CancellationToken);
                    }
                },
                TestContext.Current.CancellationToken))
            .ToArray();
        start.Set();
        await Task.WhenAll(settles);

        Assert.Equal(1, _settler.Acks + _settler.Nacks);
    }

    [Fact]
    public void A_new_context_is_not_settled()
        => Assert.False(CreateContext().IsSettled);

    [Fact]
    public async Task A_typed_context_is_a_raw_context_with_the_same_body_and_settling()
    {
        MessageContext context = CreateContext();

        await context.AckAsync(TestContext.Current.CancellationToken);

        Assert.Equal(s_body, context.Body.ToArray());
        Assert.Equal("m", Assert.IsType<MessageContext<TestMessage>>(context).Message.Id);
        Assert.Equal(1, _settler.Acks);
    }

    [Fact]
    public void A_raw_context_exposes_the_body_and_delivery_details()
    {
        var context = new MessageContext(_settler, s_body, deliveryTag: 9, redelivered: true, "exchange", "key", new BasicProperties());

        Assert.Equal(s_body, context.Body.ToArray());
        Assert.Equal(9UL, context.DeliveryTag);
        Assert.True(context.Redelivered);
        Assert.Equal("exchange", context.Exchange);
        Assert.Equal("key", context.RoutingKey);
    }

    private static readonly byte[] s_body = """{"id":"m"}"""u8.ToArray();

    private MessageContext<TestMessage> CreateContext()
        => new(new TestMessage("m"), _settler, s_body, deliveryTag: 7, redelivered: false, "exchange", "key", new BasicProperties());
}
