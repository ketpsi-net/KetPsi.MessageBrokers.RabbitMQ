namespace KetPsi.MessageBrokers.RabbitMQ;

/// <summary>
/// Connection settings. <see cref="ConnectionString"/> is read from <c>ConnectionStrings:RabbitMq</c>; everything else,
/// and any override, is set in code with <see cref="IRabbitMqConfigBuilder.ConfigureConnection"/>. Either
/// <see cref="ConnectionString"/> or <see cref="HostName"/> must be set; this is validated when the host starts.
/// </summary>
public sealed class RabbitMqOptions
{
    /// <summary>
    /// A connection string of the form <c>amqp[s]://[user[:password]@]host[:port][/virtualHost]</c>, such as
    /// <c>amqps://orders:%RABBITMQ_PASSWORD%@broker/orders</c>. Takes precedence over <see cref="HostName"/>,
    /// <see cref="Port"/> and <see cref="VirtualHost"/>.
    /// <list type="bullet">
    /// <item>Environment variables written as <c>%NAME%</c> are expanded.</item>
    /// <item>The user and password are optional; when missing, <see cref="Username"/> and <see cref="Password"/> are used.</item>
    /// <item>The password may contain a raw <c>@</c>, <c>:</c> or <c>/</c>; percent-encoded parts such as <c>%2F</c> are decoded.</item>
    /// <item>The port defaults to 5672, or 5671 for <c>amqps</c>, which also turns on TLS. The virtual host defaults to <c>/</c>.</item>
    /// </list>
    /// </summary>
    public string? ConnectionString { get; set; }

    /// <summary>Broker host name, used when <see cref="ConnectionString"/> is empty.</summary>
    public string? HostName { get; set; }

    /// <summary>Broker port, used when <see cref="ConnectionString"/> is empty.</summary>
    public int Port { get; set; } = 5672;

    /// <summary>User name, used when <see cref="ConnectionString"/> is empty or has no user.</summary>
    public string Username { get; set; } = "guest";

    /// <summary>Password, used when <see cref="ConnectionString"/> is empty or has no password.</summary>
    public string Password { get; set; } = "guest";

    /// <summary>Virtual host, used when <see cref="ConnectionString"/> is empty.</summary>
    public string VirtualHost { get; set; } = "/";

    /// <summary>Delay between automatic connection recovery attempts.</summary>
    public TimeSpan NetworkRecoveryInterval { get; set; } = TimeSpan.FromSeconds(10);

    /// <summary>
    /// Connection name shown in the RabbitMQ management UI. Defaults to the host's application name, or
    /// <c>unknown-app</c> when there is no host environment.
    /// </summary>
    public string? ClientProvidedName { get; set; }
}
