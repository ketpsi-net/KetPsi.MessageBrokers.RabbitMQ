using System.Text.Json.Serialization;
using System.Threading.Channels;
using KetPsi.Extensions.Logging.Console;
using KetPsi.MessageBrokers.RabbitMQ;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;

var builder = Host.CreateApplicationBuilder(args);
builder.Logging.ClearProviders().AddZeroAllocConsole();
builder.Services.AddSingleton<ReceivedGreetings>();
builder.Services.AddRabbitMq(rabbit => rabbit
    .UseJsonContext(SampleJsonContext.Default)
    .AddSubscriber<GreetingHandler>("Sample:Subscriber")
    .AddPublisher<Greeting>(options =>
    {
        options.Exchange = "aot-sample";
        options.RoutingKey = "greeting";
        options.Mandatory = true;
    }));

using var host = builder.Build();
await host.StartAsync();

var sent = new Greeting(Guid.NewGuid().ToString("N"), "Hello from Native AOT");
await host.Services.GetRequiredService<IRabbitMqPublisher<Greeting>>().PublishAsync(sent);

using var timeout = new CancellationTokenSource(TimeSpan.FromSeconds(30));
int exitCode;
try
{
    var greeting = await host.Services.GetRequiredService<ReceivedGreetings>().WaitForAsync(sent.Id, timeout.Token);
    Console.WriteLine($"Round-trip OK: \"{greeting.Text}\" ({greeting.Id})");
    exitCode = 0;
}
catch (OperationCanceledException)
{
    Console.WriteLine($"Round-trip FAILED: message {sent.Id} did not arrive within 30 seconds.");
    exitCode = 1;
}

await host.StopAsync();
return exitCode;

internal sealed record Greeting(
    [property: JsonPropertyName("id")] string Id,
    [property: JsonPropertyName("text")] string Text);

[JsonSerializable(typeof(Greeting))]
internal sealed partial class SampleJsonContext : JsonSerializerContext;

internal sealed class GreetingHandler(ReceivedGreetings received) : IRabbitMqHandler<Greeting>
{
    public Task HandleAsync(MessageContext<Greeting> context, CancellationToken cancellationToken)
    {
        received.Add(context.Message);
        return Task.CompletedTask;
    }
}

/// <summary>Messages the handler received; the durable queue may still hold messages from earlier runs.</summary>
internal sealed class ReceivedGreetings
{
    private readonly Channel<Greeting> _received = Channel.CreateUnbounded<Greeting>();

    public void Add(Greeting greeting) => _received.Writer.TryWrite(greeting);

    public async Task<Greeting> WaitForAsync(string id, CancellationToken cancellationToken)
    {
        while (true)
        {
            var greeting = await _received.Reader.ReadAsync(cancellationToken);
            if (greeting.Id == id)
            {
                return greeting;
            }
        }
    }
}
