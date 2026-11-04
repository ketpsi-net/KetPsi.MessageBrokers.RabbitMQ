using RabbitMQ.Client;

namespace KetPsi.MessageBrokers.RabbitMQ;

/// <summary>
/// The single RabbitMQ connection that every subscriber and publisher shares, for health checks and for code that needs
/// the client directly.
/// </summary>
/// <remarks>
/// The connection belongs to the library: do not close or dispose it. Open your own channels on it and dispose those.
/// It uses automatic recovery, so after a network failure the same instance reports <see cref="IConnection.IsOpen"/>
/// as false until it reconnects.
/// </remarks>
public interface IRabbitMqConnectionProvider
{
    /// <summary>Returns the shared connection, opening it on first use.</summary>
    /// <param name="cancellationToken">Cancels waiting for the connection to open.</param>
    /// <exception cref="global::RabbitMQ.Client.Exceptions.BrokerUnreachableException">The connection could not be opened.</exception>
    Task<IConnection> GetConnectionAsync(CancellationToken cancellationToken = default);
}
