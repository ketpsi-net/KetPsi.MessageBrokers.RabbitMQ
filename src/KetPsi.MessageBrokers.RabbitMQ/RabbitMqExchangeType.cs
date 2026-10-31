namespace KetPsi.MessageBrokers.RabbitMQ;

/// <summary>
/// AMQP exchange types. Named <c>RabbitMqExchangeType</c> rather than <c>ExchangeType</c> so it does not clash with
/// <c>RabbitMQ.Client.ExchangeType</c> in files that import both namespaces.
/// </summary>
public static class RabbitMqExchangeType
{
    /// <summary>Routes to queues whose binding key equals the routing key.</summary>
    public const string Direct = "direct";

    /// <summary>Routes to every bound queue, ignoring the routing key.</summary>
    public const string Fanout = "fanout";

    /// <summary>Routes by matching message headers against binding arguments.</summary>
    public const string Headers = "headers";

    /// <summary>Routes by matching the routing key against binding patterns (<c>*</c> and <c>#</c>).</summary>
    public const string Topic = "topic";
}
