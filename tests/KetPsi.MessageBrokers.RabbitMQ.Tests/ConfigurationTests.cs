using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Options;

namespace KetPsi.MessageBrokers.RabbitMQ.Tests;

public sealed class ConfigurationTests
{
    [Fact]
    public async Task Host_start_fails_without_a_connection_string_or_host_name()
    {
        var builder = TestHosts.CreateBuilder();
        builder.Services.AddRabbitMq(rabbit => rabbit.UseJsonContext(TestJsonContext.Default));
        using var host = builder.Build();

        var exception = await Assert.ThrowsAsync<OptionsValidationException>(() => host.StartAsync(TestContext.Current.CancellationToken));

        Assert.Contains("no connection configured", exception.Message, StringComparison.Ordinal);
    }

    [Fact]
    public void The_connection_string_is_read_from_ConnectionStrings_RabbitMq()
    {
        var builder = TestHosts.CreateBuilder(new Dictionary<string, string?>
        {
            ["ConnectionStrings:RabbitMq"] = "amqp://user:pass@broker:5672/",
        });
        builder.Services.AddRabbitMq(rabbit => rabbit.UseJsonContext(TestJsonContext.Default));
        using var host = builder.Build();

        var options = host.Services.GetRequiredService<IOptions<RabbitMqOptions>>().Value;

        Assert.Equal("amqp://user:pass@broker:5672/", options.ConnectionString);
    }

    [Fact]
    public void A_RabbitMq_section_is_not_read()
    {
        var builder = TestHosts.CreateBuilder(new Dictionary<string, string?>
        {
            ["RabbitMq:ConnectionString"] = "amqp://ignored:5672/",
            ["RabbitMq:HostName"] = "ignored",
        });
        builder.Services.AddRabbitMq(rabbit => rabbit
            .UseJsonContext(TestJsonContext.Default)
            .ConfigureConnection(options => options.HostName ??= "code"));
        using var host = builder.Build();

        var options = host.Services.GetRequiredService<IOptions<RabbitMqOptions>>().Value;

        Assert.Null(options.ConnectionString);
        Assert.Equal("code", options.HostName);
    }

    [Fact]
    public void ConfigureConnection_overrides_the_configured_connection_string()
    {
        var builder = TestHosts.CreateBuilder(Connection("amqp://configured:5672/"));
        builder.Services.AddRabbitMq(rabbit => rabbit
            .UseJsonContext(TestJsonContext.Default)
            .ConfigureConnection(options => options.ConnectionString = "amqp://code:5672/"));
        using var host = builder.Build();

        Assert.Equal("amqp://code:5672/", host.Services.GetRequiredService<IOptions<RabbitMqOptions>>().Value.ConnectionString);
    }

    [Fact]
    public void Subscriber_options_bind_from_a_configuration_section()
    {
        var builder = TestHosts.CreateBuilder(new Dictionary<string, string?>
        {
            ["ConnectionStrings:RabbitMq"] = "amqp://localhost:5672/",
            ["Messaging:Orders:Queue"] = "orders",
            ["Messaging:Orders:PrefetchCount"] = "20",
        });
        builder.Services.AddRabbitMq(rabbit => rabbit
            .UseJsonContext(TestJsonContext.Default)
            .AddSubscriber<NoOpHandler>("Messaging:Orders"));
        using var host = builder.Build();

        var consumer = Assert.Single(host.Services.GetServices<IRabbitMqConsumer>());

        // The generator inferred TestMessage from NoOpHandler's IRabbitMqHandler<TestMessage>.
        Assert.IsType<RabbitMqConsumer<NoOpHandler, TestMessage>>(consumer);
        Assert.Equal("orders", consumer.Queue);
    }

    [Fact]
    public void A_raw_handler_registers_a_raw_consumer_without_any_JSON_contract()
    {
        var builder = TestHosts.CreateBuilder(new Dictionary<string, string?>
        {
            ["ConnectionStrings:RabbitMq"] = "amqp://localhost:5672/",
            ["Messaging:Raw:Queue"] = "raw",
        });
        builder.Services.AddRabbitMq(rabbit => rabbit.AddSubscriber<RawRecordingHandler>("Messaging:Raw"));
        builder.Services.Configure<RabbitMqJsonOptions>(options => options.ReflectionEnabled = false);
        using var host = builder.Build();

        var consumer = Assert.Single(host.Services.GetServices<IRabbitMqConsumer>());

        Assert.IsType<RabbitMqRawConsumer<RawRecordingHandler>>(consumer);
        Assert.Equal("raw", consumer.Queue);
        // No message type was registered, so there is nothing for the JSON check to fail on.
        _ = host.Services.GetRequiredService<IOptions<RabbitMqJsonOptions>>().Value;
    }

    [Fact]
    public async Task Host_start_fails_when_a_subscriber_has_no_queue()
    {
        var builder = TestHosts.CreateBuilder(Connection("amqp://localhost:5672/"));
        builder.Services.AddRabbitMq(rabbit => rabbit
            .UseJsonContext(TestJsonContext.Default)
            .AddSubscriber<NoOpHandler, TestMessage>(options => options.Exchange = "orders"));
        using var host = builder.Build();

        var exception = await Assert.ThrowsAsync<OptionsValidationException>(() => host.StartAsync(TestContext.Current.CancellationToken));

        Assert.Contains(typeof(NoOpHandler).FullName!, exception.Message, StringComparison.Ordinal);
    }

    [Fact]
    public async Task Host_start_fails_when_a_publisher_has_no_exchange()
    {
        var builder = TestHosts.CreateBuilder(Connection("amqp://localhost:5672/"));
        builder.Services.AddRabbitMq(rabbit => rabbit
            .UseJsonContext(TestJsonContext.Default)
            .AddPublisher<TestMessage>(options => options.RoutingKey = "key"));
        using var host = builder.Build();

        var exception = await Assert.ThrowsAsync<OptionsValidationException>(() => host.StartAsync(TestContext.Current.CancellationToken));

        Assert.Contains("no Exchange configured", exception.Message, StringComparison.Ordinal);
    }

    [Fact]
    public void A_second_publisher_for_the_same_message_type_throws()
    {
        var services = new ServiceCollection();

        Assert.Throws<InvalidOperationException>(() => services.AddRabbitMq(rabbit => rabbit
            .UseJsonContext(TestJsonContext.Default)
            .AddPublisher<TestMessage>(options => options.Exchange = "a")
            .AddPublisher<TestMessage>(options => options.Exchange = "b")));
    }

    private static Dictionary<string, string?> Connection(string connectionString)
        => new() { ["ConnectionStrings:RabbitMq"] = connectionString };
}
