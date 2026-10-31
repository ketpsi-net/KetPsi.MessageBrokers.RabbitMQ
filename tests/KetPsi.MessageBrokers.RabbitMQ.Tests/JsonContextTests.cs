using System.Text.Json;
using System.Text.Json.Serialization;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Options;

namespace KetPsi.MessageBrokers.RabbitMQ.Tests;

public sealed class JsonContextTests
{
    private static readonly Dictionary<string, string?> s_connection = new() { ["ConnectionStrings:RabbitMq"] = "amqp://localhost:5672/" };

    [Fact]
    public void GetTypeInfo_for_an_unregistered_type_names_the_missing_attribute()
    {
        var options = new RabbitMqJsonOptions { ReflectionEnabled = false };
        options.Contexts.Add(TestJsonContext.Default);

        var exception = Assert.Throws<InvalidOperationException>(options.GetTypeInfo<UnregisteredMessage>);

        Assert.Contains("[JsonSerializable(typeof(UnregisteredMessage))]", exception.Message, StringComparison.Ordinal);
        Assert.Contains(typeof(UnregisteredMessage).FullName!, exception.Message, StringComparison.Ordinal);
    }

    [Fact]
    public void GetTypeInfo_uses_the_options_of_the_context_that_knows_the_type()
    {
        var options = new RabbitMqJsonOptions();
        options.Contexts.Add(TestJsonContext.Default);
        options.Contexts.Add(OtherJsonContext.Default);

        Assert.Same(TestJsonContext.Default.Options, options.GetTypeInfo<TestMessage>().Options);
        Assert.Same(OtherJsonContext.Default.Options, options.GetTypeInfo<OtherMessage>().Options);
    }

    [Fact]
    public async Task Host_start_fails_when_a_message_type_has_no_contract_and_reflection_is_disabled()
    {
        var builder = TestHosts.CreateBuilder(s_connection);
        builder.Services.AddRabbitMq(rabbit => rabbit
            .UseJsonContext(TestJsonContext.Default)
            .AddPublisher<UnregisteredMessage>(options => options.Exchange = "orders"));
        DisableReflection(builder.Services);
        using var host = builder.Build();

        var exception = await Assert.ThrowsAsync<OptionsValidationException>(() => host.StartAsync(TestContext.Current.CancellationToken));

        Assert.Contains("[JsonSerializable(typeof(UnregisteredMessage))]", exception.Message, StringComparison.Ordinal);
    }

    [Fact]
    public async Task Without_a_contract_reflection_is_used_with_the_configured_serializer_settings()
    {
        var builder = TestHosts.CreateBuilder(s_connection);
        builder.Services.AddRabbitMq(rabbit => rabbit
            .ConfigureJsonSerializer(options => options.PropertyNamingPolicy = JsonNamingPolicy.SnakeCaseLower)
            .AddPublisher<PlainMessage>(options => options.Exchange = "orders"));
        using var host = builder.Build();
        await host.StartAsync(TestContext.Current.CancellationToken);

        var typeInfo = JsonOptions(host).GetTypeInfo<PlainMessage>();

        Assert.Equal("""{"order_id":"42"}""", JsonSerializer.Serialize(new PlainMessage("42"), typeInfo));
        await host.StopAsync(TestContext.Current.CancellationToken);
    }

    [Fact]
    public void Every_JsonSerializerContext_service_is_used()
    {
        var builder = TestHosts.CreateBuilder(s_connection);
        builder.Services.AddSingleton<JsonSerializerContext>(TestJsonContext.Default);
        builder.Services.AddSingleton<JsonSerializerContext>(OtherJsonContext.Default);
        builder.Services.AddRabbitMq(_ => { });
        DisableReflection(builder.Services);
        using var host = builder.Build();

        var options = JsonOptions(host);

        Assert.Same(TestJsonContext.Default.Options, options.GetTypeInfo<TestMessage>().Options);
        Assert.Same(OtherJsonContext.Default.Options, options.GetTypeInfo<OtherMessage>().Options);
    }

    [Fact]
    public void UseJsonContext_is_consulted_before_JsonSerializerContext_services()
    {
        var builder = TestHosts.CreateBuilder(s_connection);
        builder.Services.AddSingleton<JsonSerializerContext>(new TestJsonContext(new JsonSerializerOptions()));
        builder.Services.AddRabbitMq(rabbit => rabbit.UseJsonContext(TestJsonContext.Default));
        using var host = builder.Build();

        Assert.Same(TestJsonContext.Default.Options, JsonOptions(host).GetTypeInfo<TestMessage>().Options);
    }

    [Fact]
    public void The_resolver_chain_of_a_JsonSerializerOptions_service_is_used()
    {
        var builder = TestHosts.CreateBuilder(s_connection);
        builder.Services.AddSingleton(new JsonSerializerOptions { TypeInfoResolver = OtherJsonContext.Default });
        builder.Services.AddRabbitMq(_ => { });
        DisableReflection(builder.Services);
        using var host = builder.Build();

        Assert.NotNull(JsonOptions(host).FindTypeInfo<OtherMessage>());
    }

    [Fact]
    public void The_resolver_chain_of_ConfigureJsonSerializer_is_used()
    {
        var builder = TestHosts.CreateBuilder(s_connection);
        builder.Services.AddRabbitMq(rabbit => rabbit
            .ConfigureJsonSerializer(options => options.TypeInfoResolverChain.Add(OtherJsonContext.Default)));
        DisableReflection(builder.Services);
        using var host = builder.Build();

        Assert.NotNull(JsonOptions(host).FindTypeInfo<OtherMessage>());
        Assert.Null(JsonOptions(host).FindTypeInfo<TestMessage>());
    }

    private static void DisableReflection(IServiceCollection services)
        => services.Configure<RabbitMqJsonOptions>(options => options.ReflectionEnabled = false);

    private static RabbitMqJsonOptions JsonOptions(IHost host)
        => host.Services.GetRequiredService<IOptions<RabbitMqJsonOptions>>().Value;
}
