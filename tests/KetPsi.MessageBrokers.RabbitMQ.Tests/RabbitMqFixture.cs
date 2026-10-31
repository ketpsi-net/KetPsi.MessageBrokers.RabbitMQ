using RabbitMQ.Client;
using Testcontainers.RabbitMq;

namespace KetPsi.MessageBrokers.RabbitMQ.Tests;

/// <summary>Integration tests share one broker; unit tests stay outside this collection and need no Docker.</summary>
[CollectionDefinition(Name)]
public sealed class BrokerCollection : ICollectionFixture<RabbitMqFixture>
{
    public const string Name = "RabbitMQ broker";
}

/// <summary>One RabbitMQ broker for the whole test run, plus a raw connection for setting up and inspecting topology.</summary>
public sealed class RabbitMqFixture : IAsyncLifetime
{
    private readonly RabbitMqContainer _container = new RabbitMqBuilder("rabbitmq:4.1-alpine").Build();
    private IConnection? _connection;

    public string ConnectionString => _container.GetConnectionString();

    public async ValueTask InitializeAsync()
    {
        await _container.StartAsync();
        _connection = await new ConnectionFactory { Uri = new Uri(ConnectionString) }.CreateConnectionAsync();
    }

    public async ValueTask DisposeAsync()
    {
        if (_connection is not null)
        {
            await _connection.DisposeAsync();
        }

        await _container.DisposeAsync();
    }

    public Task<IChannel> CreateChannelAsync(CancellationToken cancellationToken)
        => (_connection ?? throw new InvalidOperationException("The broker has not started.")).CreateChannelAsync(cancellationToken: cancellationToken);

    /// <summary>Declares a fanout dead-letter exchange with one queue bound to it.</summary>
    internal async Task DeclareDeadLetterAsync(TestTopology topology, CancellationToken cancellationToken)
    {
        await using var channel = await CreateChannelAsync(cancellationToken);
        await channel.ExchangeDeclareAsync(topology.DeadLetterExchange, ExchangeType.Fanout, durable: true, autoDelete: false, cancellationToken: cancellationToken);
        await channel.QueueDeclareAsync(topology.DeadLetterQueue, durable: true, exclusive: false, autoDelete: false, cancellationToken: cancellationToken);
        await channel.QueueBindAsync(topology.DeadLetterQueue, topology.DeadLetterExchange, routingKey: string.Empty, cancellationToken: cancellationToken);
    }

    /// <summary>Polls a queue until a message arrives, taking it off the queue.</summary>
    public async Task<BasicGetResult?> WaitForMessageAsync(string queue, CancellationToken cancellationToken, TimeSpan? timeout = null)
    {
        await using var channel = await CreateChannelAsync(cancellationToken);
        var deadline = DateTime.UtcNow + (timeout ?? TimeSpan.FromSeconds(15));
        while (DateTime.UtcNow < deadline)
        {
            var result = await channel.BasicGetAsync(queue, autoAck: true, cancellationToken);
            if (result is not null)
            {
                return result;
            }

            await Task.Delay(TimeSpan.FromMilliseconds(100), cancellationToken);
        }

        return null;
    }
}
