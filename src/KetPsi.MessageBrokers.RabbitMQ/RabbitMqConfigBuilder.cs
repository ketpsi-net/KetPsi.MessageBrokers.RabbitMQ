using System.Diagnostics.CodeAnalysis;
using System.Globalization;
using System.Text.Json;
using System.Text.Json.Serialization;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.DependencyInjection.Extensions;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;

namespace KetPsi.MessageBrokers.RabbitMQ;

internal sealed class RabbitMqConfigBuilder(IServiceCollection services) : IRabbitMqConfigBuilder
{
    private readonly HashSet<Type> _checkedMessageTypes = [];

    public IRabbitMqConfigBuilder ConfigureConnection(Action<RabbitMqOptions> configure)
    {
        ArgumentNullException.ThrowIfNull(configure);
        services.Configure(configure);
        return this;
    }

    public IRabbitMqConfigBuilder UseJsonContext(JsonSerializerContext context)
    {
        ArgumentNullException.ThrowIfNull(context);
        services.Configure<RabbitMqJsonOptions>(options => options.Contexts.Add(context));
        return this;
    }

    public IRabbitMqConfigBuilder ConfigureJsonSerializer(Action<JsonSerializerOptions> configure)
    {
        ArgumentNullException.ThrowIfNull(configure);
        services.Configure<RabbitMqJsonOptions>(options => configure(options.SerializerOptions));
        return this;
    }

    // The source generator intercepts these calls and calls AddSubscriber<THandler, TMessage> with the inferred message type.
    public IRabbitMqConfigBuilder AddSubscriber<THandler>(Action<RabbitMqSubscriberOptions> configure)
        where THandler : class
        => throw new InvalidOperationException(NotInterceptedMessage<THandler>());

    public IRabbitMqConfigBuilder AddSubscriber<THandler>(string configSectionPath)
        where THandler : class
        => throw new InvalidOperationException(NotInterceptedMessage<THandler>());

    public IRabbitMqConfigBuilder AddSubscriber<[DynamicallyAccessedMembers(DynamicallyAccessedMemberTypes.PublicConstructors)] THandler, TMessage>(
        Action<RabbitMqSubscriberOptions> configure)
        where THandler : class, IRabbitMqHandler<TMessage>
    {
        ArgumentNullException.ThrowIfNull(configure);
        var name = NewOptionsName();
        AddSubscriberOptions<THandler>(name).Configure(configure);
        RegisterSubscriber<THandler, TMessage>(name);
        return this;
    }

    public IRabbitMqConfigBuilder AddSubscriber<[DynamicallyAccessedMembers(DynamicallyAccessedMemberTypes.PublicConstructors)] THandler, TMessage>(
        string configSectionPath)
        where THandler : class, IRabbitMqHandler<TMessage>
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(configSectionPath);
        var name = NewOptionsName();
        AddSubscriberOptions<THandler>(name)
            .Configure<IConfiguration>((options, configuration) => configuration.GetSection(configSectionPath).Bind(options));
        RegisterSubscriber<THandler, TMessage>(name);
        return this;
    }

    public IRabbitMqConfigBuilder AddPublisher<TMessage>(Action<RabbitMqPublisherOptions> configure)
    {
        ArgumentNullException.ThrowIfNull(configure);
        var name = NewOptionsName();
        AddPublisherOptions<TMessage>(name).Configure(configure);
        RegisterPublisher<TMessage>(name);
        return this;
    }

    public IRabbitMqConfigBuilder AddPublisher<TMessage>(string configSectionPath)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(configSectionPath);
        var name = NewOptionsName();
        AddPublisherOptions<TMessage>(name)
            .Configure<IConfiguration>((options, configuration) => configuration.GetSection(configSectionPath).Bind(options));
        RegisterPublisher<TMessage>(name);
        return this;
    }

    private static string NewOptionsName() => Guid.NewGuid().ToString("N", CultureInfo.InvariantCulture);

    private OptionsBuilder<RabbitMqSubscriberOptions> AddSubscriberOptions<THandler>(string name)
        => services.AddOptions<RabbitMqSubscriberOptions>(name)
            .Validate(options => !string.IsNullOrWhiteSpace(options.Queue), MissingQueueMessage<THandler>())
            .ValidateOnStart();

    private OptionsBuilder<RabbitMqPublisherOptions> AddPublisherOptions<TMessage>(string name)
        => services.AddOptions<RabbitMqPublisherOptions>(name)
            .Validate(options => !string.IsNullOrWhiteSpace(options.Exchange), MissingExchangeMessage<TMessage>())
            .ValidateOnStart();

    private void RegisterSubscriber<[DynamicallyAccessedMembers(DynamicallyAccessedMemberTypes.PublicConstructors)] THandler, TMessage>(
        string name)
        where THandler : class, IRabbitMqHandler<TMessage>
    {
        services.TryAddScoped<THandler>();
        AddJsonTypeCheck<TMessage>();

        services.AddSingleton<IRabbitMqConsumer>(serviceProvider =>
        {
            var options = serviceProvider.GetRequiredService<IOptionsMonitor<RabbitMqSubscriberOptions>>().Get(name);
            if (string.IsNullOrWhiteSpace(options.Queue))
            {
                throw new InvalidOperationException(MissingQueueMessage<THandler>());
            }

            var typeInfo = serviceProvider.GetRequiredService<IOptions<RabbitMqJsonOptions>>().Value.GetTypeInfo<TMessage>();
            var logger = serviceProvider.GetRequiredService<ILoggerFactory>().CreateLogger<RabbitMqConsumer<THandler, TMessage>>();

            return new RabbitMqConsumer<THandler, TMessage>(
                serviceProvider.GetRequiredService<RabbitMqConnectionProvider>(),
                serviceProvider.GetRequiredService<IServiceScopeFactory>(),
                options,
                typeInfo,
                logger);
        });
    }

    private void RegisterPublisher<TMessage>(string name)
    {
        if (services.Any(descriptor => descriptor.ServiceType == typeof(IRabbitMqPublisher<TMessage>)))
        {
            throw new InvalidOperationException(
                $"A RabbitMQ publisher for '{typeof(TMessage).FullName}' is already registered. Register one publisher per message type.");
        }

        AddJsonTypeCheck<TMessage>();

        services.AddSingleton<IRabbitMqPublisher<TMessage>>(serviceProvider =>
        {
            var options = serviceProvider.GetRequiredService<IOptionsMonitor<RabbitMqPublisherOptions>>().Get(name);
            if (string.IsNullOrWhiteSpace(options.Exchange))
            {
                throw new InvalidOperationException(MissingExchangeMessage<TMessage>());
            }

            var typeInfo = serviceProvider.GetRequiredService<IOptions<RabbitMqJsonOptions>>().Value.GetTypeInfo<TMessage>();
            var logger = serviceProvider.GetRequiredService<ILoggerFactory>().CreateLogger<RabbitMqPublisher<TMessage>>();

            return new RabbitMqPublisher<TMessage>(
                serviceProvider.GetRequiredService<RabbitMqConnectionProvider>(),
                options,
                typeInfo,
                logger);
        });
    }

    /// <summary>Fails host startup, rather than the first message, when no contract source knows <typeparamref name="TMessage"/>.</summary>
    private void AddJsonTypeCheck<TMessage>()
    {
        if (_checkedMessageTypes.Add(typeof(TMessage)))
        {
            services.AddOptions<RabbitMqJsonOptions>()
                .Validate(options => options.FindTypeInfo<TMessage>() is not null, RabbitMqJsonOptions.MissingTypeMessage<TMessage>());
        }
    }

    private static string NotInterceptedMessage<THandler>()
        => $"AddSubscriber<{typeof(THandler).Name}> was not rewritten by the KetPsi.MessageBrokers.RabbitMQ source generator, "
            + "which infers the message type from IRabbitMqHandler<T>. Reference the package so its generator runs, "
            + $"or call AddSubscriber<{typeof(THandler).Name}, TMessage> instead.";

    private static string MissingQueueMessage<THandler>()
        => $"The RabbitMQ subscriber for handler '{typeof(THandler).FullName}' has no Queue configured.";

    private static string MissingExchangeMessage<TMessage>()
        => $"The RabbitMQ publisher for '{typeof(TMessage).FullName}' has no Exchange configured.";
}
