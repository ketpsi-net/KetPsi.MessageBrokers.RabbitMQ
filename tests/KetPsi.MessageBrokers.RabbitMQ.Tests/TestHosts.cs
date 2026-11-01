using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;

namespace KetPsi.MessageBrokers.RabbitMQ.Tests;

internal static class TestHosts
{
    /// <summary>A host with no default configuration sources, so only <paramref name="configuration"/> applies.</summary>
    public static HostApplicationBuilder CreateBuilder(IReadOnlyDictionary<string, string?>? configuration = null)
    {
        var builder = Host.CreateApplicationBuilder(new HostApplicationBuilderSettings { DisableDefaults = true });
        if (configuration is not null)
        {
            builder.Configuration.AddInMemoryCollection(configuration);
        }

        builder.Services.Configure<HostOptions>(options => options.ShutdownTimeout = TimeSpan.FromSeconds(10));
        builder.Services.AddSingleton<MessageRecorder>();
        builder.Services.AddSingleton<RawMessageRecorder>();
        builder.Services.AddScoped<ScopedProbe>();
        return builder;
    }

    /// <summary>A host connected to the test broker, with <see cref="TestJsonContext"/> registered.</summary>
    public static IHost CreateConnected(RabbitMqFixture broker, Action<IRabbitMqConfigBuilder> configure)
    {
        var builder = CreateBuilder();
        builder.Services.AddRabbitMq(rabbit =>
        {
            rabbit.UseJsonContext(TestJsonContext.Default)
                .ConfigureConnection(options => options.ConnectionString = broker.ConnectionString);
            configure(rabbit);
        });
        return builder.Build();
    }
}

/// <summary>Unique names per test, so tests sharing one broker do not see each other's messages.</summary>
internal sealed record TestTopology(string Queue, string Exchange, string RoutingKey, string DeadLetterExchange, string DeadLetterQueue)
{
    public static TestTopology Create()
    {
        var id = Guid.NewGuid().ToString("N")[..12];
        return new TestTopology($"q-{id}", $"ex-{id}", $"rk-{id}", $"dlx-{id}", $"dlq-{id}");
    }
}
