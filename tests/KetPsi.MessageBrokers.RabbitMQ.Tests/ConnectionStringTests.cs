using Microsoft.Extensions.Options;

namespace KetPsi.MessageBrokers.RabbitMQ.Tests;

public sealed class ConnectionStringTests
{
    [Theory]
    [InlineData("amqp://broker", false, "broker", 5672, null, null, "/")]
    [InlineData("amqps://broker", true, "broker", 5671, null, null, "/")]
    [InlineData("AMQP://broker:5673/", false, "broker", 5673, null, null, "/")]
    [InlineData("amqp://orders@broker/orders", false, "broker", 5672, "orders", null, "orders")]
    [InlineData("amqp://orders:secret@broker:5673/orders", false, "broker", 5673, "orders", "secret", "orders")]
    [InlineData("amqp://:secret@broker", false, "broker", 5672, null, "secret", "/")]
    [InlineData("amqp://orders:p@ss@broker:5673/shop", false, "broker", 5673, "orders", "p@ss", "shop")]
    [InlineData("amqp://orders:@@@broker", false, "broker", 5672, "orders", "@@", "/")]
    [InlineData("amqp://orders:p@ss:w/rd#1?@broker", false, "broker", 5672, "orders", "p@ss:w/rd#1?", "/")]
    [InlineData("amqp://u%40x:p%23w@broker/%2F", false, "broker", 5672, "u@x", "p#w", "/")]
    [InlineData("amqp://broker/prod%2Forders", false, "broker", 5672, null, null, "prod/orders")]
    [InlineData("amqps://[::1]:5999/v", true, "::1", 5999, null, null, "v")]
    public void Parses_the_parts(string value, bool useTls, string host, int port, string? user, string? password, string virtualHost)
    {
        var connectionString = RabbitMqConnectionString.Parse(value);

        Assert.Equal(useTls, connectionString.UseTls);
        Assert.Equal(host, connectionString.HostName);
        Assert.Equal(port, connectionString.Port);
        Assert.Equal(user, connectionString.UserName);
        Assert.Equal(password, connectionString.Password);
        Assert.Equal(virtualHost, connectionString.VirtualHost);
    }

    [Fact]
    public void Environment_variables_are_expanded()
    {
        var hostVariable = $"KETPSI_RMQ_TEST_HOST_{Guid.NewGuid():N}";
        var passwordVariable = $"KETPSI_RMQ_TEST_PASSWORD_{Guid.NewGuid():N}";
        Environment.SetEnvironmentVariable(hostVariable, "broker.internal");
        Environment.SetEnvironmentVariable(passwordVariable, "from-env");
        try
        {
            var connectionString = RabbitMqConnectionString.Parse($"amqp://orders:%{passwordVariable}%@%{hostVariable}%:5673/orders");

            Assert.Equal("broker.internal", connectionString.HostName);
            Assert.Equal("from-env", connectionString.Password);
        }
        finally
        {
            Environment.SetEnvironmentVariable(hostVariable, null);
            Environment.SetEnvironmentVariable(passwordVariable, null);
        }
    }

    [Theory]
    [InlineData("broker:5672", "must start with amqp:// or amqps://")]
    [InlineData("%KETPSI_RMQ_TEST_UNSET_VARIABLE%", "environment variables")]
    [InlineData("http://secret@broker", "scheme 'http'")]
    [InlineData("amqp://", "has no host")]
    [InlineData("amqp://user:secret@:5672", "has no host")]
    [InlineData("amqp://user:secret@broker:99999", "invalid port '99999'")]
    [InlineData("amqp://user:secret@broker:", "invalid port ''")]
    [InlineData("amqp://broker:abc", "invalid port 'abc'")]
    [InlineData("amqp://broker/a/b", "more than one path segment")]
    [InlineData("amqp://broker/v?heartbeat=10", "query string")]
    [InlineData("amqp://[::1:5672", "closing ']'")]
    public void Rejects_invalid_values_without_echoing_credentials(string value, string expected)
    {
        Assert.False(RabbitMqConnectionString.TryParse(value, out _, out var error));

        Assert.Contains(expected, error, StringComparison.Ordinal);
        Assert.DoesNotContain("secret", error, StringComparison.Ordinal);
    }

    [Fact]
    public void ToString_does_not_print_the_password()
        => Assert.DoesNotContain("secret", RabbitMqConnectionString.Parse("amqp://user:secret@broker").ToString(), StringComparison.Ordinal);

    [Fact]
    public async Task Host_start_fails_for_an_invalid_connection_string()
    {
        var builder = TestHosts.CreateBuilder(new Dictionary<string, string?> { ["ConnectionStrings:RabbitMq"] = "http://broker" });
        builder.Services.AddRabbitMq(_ => { });
        using var host = builder.Build();

        var exception = await Assert.ThrowsAsync<OptionsValidationException>(() => host.StartAsync(TestContext.Current.CancellationToken));

        Assert.Contains("use amqp or amqps", exception.Message, StringComparison.Ordinal);
    }
}
