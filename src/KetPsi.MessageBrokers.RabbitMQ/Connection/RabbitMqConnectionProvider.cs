using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using RabbitMQ.Client;

namespace KetPsi.MessageBrokers.RabbitMQ;

/// <summary>Owns the single <see cref="IConnection"/> shared by every consumer and publisher.</summary>
internal sealed class RabbitMqConnectionProvider(
    IOptions<RabbitMqOptions> options,
    IOptions<SslOption> sslOptions,
    ILogger<RabbitMqConnectionProvider> logger,
    IHostEnvironment? hostEnvironment = null) : IAsyncDisposable
{
    private readonly SemaphoreSlim _lock = new(1, 1);
    private volatile IConnection? _connection;
    private bool _disposed;

    public async Task<IConnection> GetConnectionAsync(CancellationToken cancellationToken)
    {
        ObjectDisposedException.ThrowIf(_disposed, this);

        if (_connection is { } connection)
        {
            return connection;
        }

        await _lock.WaitAsync(cancellationToken).ConfigureAwait(false);
        try
        {
            ObjectDisposedException.ThrowIf(_disposed, this);

            if (_connection is { } existing)
            {
                return existing;
            }

            var clientProvidedName = ResolveClientProvidedName(options.Value);
            var factory = CreateFactory(clientProvidedName);
            var created = await factory.CreateConnectionAsync(cancellationToken).ConfigureAwait(false);
            _connection = created;
            logger.LogInformation($"Connected to RabbitMQ as {clientProvidedName:@ClientProvidedName}");
            return created;
        }
        finally
        {
            _lock.Release();
        }
    }

    public async ValueTask DisposeAsync()
    {
        await _lock.WaitAsync().ConfigureAwait(false);
        try
        {
            if (_disposed)
            {
                return;
            }

            _disposed = true;
            if (_connection is { } connection)
            {
                _connection = null;
                await connection.DisposeAsync().ConfigureAwait(false);
            }
        }
        finally
        {
            _lock.Release();
        }
    }

    private ConnectionFactory CreateFactory(string clientProvidedName)
    {
        var value = options.Value;
        var factory = new ConnectionFactory
        {
            AutomaticRecoveryEnabled = true,
            TopologyRecoveryEnabled = true,
            NetworkRecoveryInterval = value.NetworkRecoveryInterval,
            ClientProvidedName = clientProvidedName,
        };

        var useTls = false;
        if (!string.IsNullOrWhiteSpace(value.ConnectionString))
        {
            var connectionString = RabbitMqConnectionString.Parse(value.ConnectionString);
            factory.HostName = connectionString.HostName;
            factory.Port = connectionString.Port;
            factory.VirtualHost = connectionString.VirtualHost;
            // Credentials are optional in the connection string; the options supply the rest.
            factory.UserName = connectionString.UserName ?? value.Username;
            factory.Password = connectionString.Password ?? value.Password;
            useTls = connectionString.UseTls;
        }
        else
        {
            // Options validation guarantees a host name whenever there is no connection string.
            factory.HostName = value.HostName
                ?? throw new InvalidOperationException("RabbitMqOptions needs a ConnectionString or a HostName.");
            factory.Port = value.Port;
            factory.UserName = value.Username;
            factory.Password = value.Password;
            factory.VirtualHost = value.VirtualHost;
        }

        var ssl = sslOptions.Value;
        if (ssl.Enabled)
        {
            factory.Ssl = ssl;
        }
        else if (useTls)
        {
            factory.Ssl = new SslOption(factory.HostName, enabled: true);
        }

        return factory;
    }

    private string ResolveClientProvidedName(RabbitMqOptions value)
    {
        if (!string.IsNullOrWhiteSpace(value.ClientProvidedName))
        {
            return value.ClientProvidedName;
        }

        var applicationName = hostEnvironment?.ApplicationName;
        return string.IsNullOrWhiteSpace(applicationName) ? RabbitMqDefaults.ClientProvidedName : applicationName;
    }
}
