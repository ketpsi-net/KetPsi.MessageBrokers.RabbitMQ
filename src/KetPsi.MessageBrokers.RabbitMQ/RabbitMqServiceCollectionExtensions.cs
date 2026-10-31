using System.Text.Json;
using System.Text.Json.Serialization;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.DependencyInjection.Extensions;
using Microsoft.Extensions.Options;
using RabbitMQ.Client;

namespace KetPsi.MessageBrokers.RabbitMQ;

/// <summary>Registers RabbitMQ publishers and subscribers.</summary>
public static class RabbitMqServiceCollectionExtensions
{
    /// <summary>
    /// Adds the RabbitMQ connection, the hosted service that runs subscribers, and whatever <paramref name="configure"/>
    /// registers. The connection string is read from <c>ConnectionStrings:RabbitMq</c>; options are validated when the
    /// host starts.
    /// </summary>
    /// <param name="services">The service collection.</param>
    /// <param name="configure">Registers JSON contracts, subscribers and publishers.</param>
    /// <returns>The service collection.</returns>
    public static IServiceCollection AddRabbitMq(this IServiceCollection services, Action<IRabbitMqConfigBuilder> configure)
    {
        ArgumentNullException.ThrowIfNull(services);
        ArgumentNullException.ThrowIfNull(configure);

        AddCoreServices(services);

        configure(new RabbitMqConfigBuilder(services));
        return services;
    }

    private static void AddCoreServices(IServiceCollection services)
    {
        if (services.Any(descriptor => descriptor.ServiceType == typeof(RabbitMqConnectionProvider)))
        {
            return;
        }

        services.AddOptions<RabbitMqOptions>()
            .Configure<IServiceProvider>(static (options, serviceProvider) =>
                options.ConnectionString = serviceProvider.GetService<IConfiguration>()
                    ?.GetConnectionString(RabbitMqConfigurationKeys.ConnectionStringName))
            .ValidateOnStart();
        services.TryAddEnumerable(ServiceDescriptor.Singleton<IValidateOptions<RabbitMqOptions>, RabbitMqOptionsValidator>());

        services.AddOptions<SslOption>();
        services.AddOptions<RabbitMqJsonOptions>()
            .PostConfigure<IEnumerable<JsonSerializerContext>, IEnumerable<JsonSerializerOptions>>(
                static (options, contexts, serializerOptions) =>
                {
                    options.Contexts.AddRange(contexts);
                    options.ServiceSerializerOptions.AddRange(serializerOptions);
                })
            .ValidateOnStart();
        services.TryAddSingleton<RabbitMqConnectionProvider>();
        services.AddHostedService<RabbitMqConsumerHostedService>();
    }
}
