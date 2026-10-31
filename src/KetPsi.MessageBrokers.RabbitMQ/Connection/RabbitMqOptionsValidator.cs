using Microsoft.Extensions.Options;

namespace KetPsi.MessageBrokers.RabbitMQ;

/// <summary>Fails host startup when there is no connection configured, or the connection string cannot be parsed.</summary>
internal sealed class RabbitMqOptionsValidator : IValidateOptions<RabbitMqOptions>
{
    public ValidateOptionsResult Validate(string? name, RabbitMqOptions options)
    {
        if (!string.IsNullOrWhiteSpace(options.ConnectionString))
        {
            return RabbitMqConnectionString.TryParse(options.ConnectionString, out _, out var error)
                ? ValidateOptionsResult.Success
                : ValidateOptionsResult.Fail(error);
        }

        return string.IsNullOrWhiteSpace(options.HostName)
            ? ValidateOptionsResult.Fail(
                $"RabbitMQ has no connection configured. Set ConnectionStrings:{RabbitMqConfigurationKeys.ConnectionStringName}, "
                + "or set ConnectionString or HostName with ConfigureConnection.")
            : ValidateOptionsResult.Success;
    }
}
