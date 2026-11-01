namespace KetPsi.MessageBrokers.RabbitMQ;

internal static class RabbitMqConfigurationKeys
{
    /// <summary>Name under <c>ConnectionStrings</c> that <see cref="RabbitMqOptions.ConnectionString"/> is read from.</summary>
    public const string ConnectionStringName = "RabbitMq";
}

internal static class RabbitMqQueueArguments
{
    public const string DeadLetterExchange = "x-dead-letter-exchange";
}

internal static class RabbitMqContentTypes
{
    public const string Json = "application/json";

    /// <summary>The content type of raw publishes that do not name one.</summary>
    public const string OctetStream = "application/octet-stream";
}

internal static class RabbitMqDefaults
{
    public const string ClientProvidedName = "unknown-app";
}
