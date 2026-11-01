using System.Diagnostics.CodeAnalysis;
using System.Text.Json;
using System.Text.Json.Serialization;

namespace KetPsi.MessageBrokers.RabbitMQ;

/// <summary>Configures RabbitMQ inside <see cref="RabbitMqServiceCollectionExtensions.AddRabbitMq"/>.</summary>
public interface IRabbitMqConfigBuilder
{
    /// <summary>
    /// Configures the connection in code. Runs after the connection string is read from <c>ConnectionStrings:RabbitMq</c>,
    /// so it overrides it.
    /// </summary>
    /// <param name="configure">Sets connection options.</param>
    IRabbitMqConfigBuilder ConfigureConnection(Action<RabbitMqOptions> configure);

    /// <summary>
    /// Registers a source-generated JSON context for message types. May be called several times; the first context that
    /// knows a type is used. Contexts registered in DI as <see cref="JsonSerializerContext"/> services are used too, after these.
    /// </summary>
    /// <remarks>
    /// A message type needs a source-generated contract only when reflection-based serialization is disabled, as under
    /// Native AOT; host startup then fails for any message type without one. Otherwise such types fall back to reflection.
    /// </remarks>
    /// <param name="context">A context such as <c>MyJsonContext.Default</c>; its <see cref="JsonSourceGenerationOptionsAttribute"/> settings apply.</param>
    IRabbitMqConfigBuilder UseJsonContext(JsonSerializerContext context);

    /// <summary>
    /// Configures the <see cref="JsonSerializerOptions"/> for message types no JSON context knows. Contracts added to its
    /// <see cref="JsonSerializerOptions.TypeInfoResolverChain"/> are used after the contexts, then those of every
    /// <see cref="JsonSerializerOptions"/> service; the reflection fallback uses its settings, such as the naming policy.
    /// </summary>
    /// <param name="configure">Changes the options.</param>
    IRabbitMqConfigBuilder ConfigureJsonSerializer(Action<JsonSerializerOptions> configure);

    /// <summary>
    /// Registers <typeparamref name="THandler"/> as a scoped handler for messages from a queue.
    /// <list type="bullet">
    /// <item>A handler implementing <see cref="IRabbitMqHandler{T}"/> gets each message deserialized to <c>T</c>. The
    /// package's source generator fills in <c>T</c> at compile time.</item>
    /// <item>A handler implementing <see cref="IRabbitMqHandler"/> gets the raw body, with no deserialization and no JSON
    /// contract needed.</item>
    /// </list>
    /// </summary>
    /// <typeparam name="THandler">
    /// The handler, created by DI per message. Implements <see cref="IRabbitMqHandler"/>, or exactly one
    /// <see cref="IRabbitMqHandler{T}"/>.
    /// </typeparam>
    /// <param name="configure">Sets the subscriber options; <see cref="RabbitMqSubscriberOptions.Queue"/> is required.</param>
    /// <exception cref="InvalidOperationException">
    /// <typeparamref name="THandler"/> implements only <see cref="IRabbitMqHandler{T}"/> and the source generator did not run.
    /// </exception>
    IRabbitMqConfigBuilder AddSubscriber<[DynamicallyAccessedMembers(DynamicallyAccessedMemberTypes.PublicConstructors)] THandler>(Action<RabbitMqSubscriberOptions> configure)
        where THandler : class;

    /// <summary>
    /// Registers <typeparamref name="THandler"/> with options bound from a configuration section. Raw and typed
    /// handlers work as in <see cref="AddSubscriber{THandler}(Action{RabbitMqSubscriberOptions})"/>.
    /// </summary>
    /// <typeparam name="THandler">
    /// The handler, created by DI per message. Implements <see cref="IRabbitMqHandler"/>, or exactly one
    /// <see cref="IRabbitMqHandler{T}"/>.
    /// </typeparam>
    /// <param name="configSectionPath">The section holding <see cref="RabbitMqSubscriberOptions"/>, e.g. <c>Messaging:Orders</c>.</param>
    /// <exception cref="InvalidOperationException">
    /// <typeparamref name="THandler"/> implements only <see cref="IRabbitMqHandler{T}"/> and the source generator did not run.
    /// </exception>
    IRabbitMqConfigBuilder AddSubscriber<[DynamicallyAccessedMembers(DynamicallyAccessedMemberTypes.PublicConstructors)] THandler>(string configSectionPath)
        where THandler : class;

    /// <summary>
    /// Registers <typeparamref name="THandler"/> as a scoped handler for <typeparamref name="TMessage"/>. Use it when the
    /// handler implements <see cref="IRabbitMqHandler{T}"/> more than once, or without the source generator.
    /// </summary>
    /// <typeparam name="THandler">The handler, created by DI per message.</typeparam>
    /// <typeparam name="TMessage">The message type.</typeparam>
    /// <param name="configure">Sets the subscriber options; <see cref="RabbitMqSubscriberOptions.Queue"/> is required.</param>
    IRabbitMqConfigBuilder AddSubscriber<[DynamicallyAccessedMembers(DynamicallyAccessedMemberTypes.PublicConstructors)] THandler, TMessage>(
        Action<RabbitMqSubscriberOptions> configure)
        where THandler : class, IRabbitMqHandler<TMessage>;

    /// <summary>Registers <typeparamref name="THandler"/> with options bound from a configuration section.</summary>
    /// <typeparam name="THandler">The handler, created by DI per message.</typeparam>
    /// <typeparam name="TMessage">The message type.</typeparam>
    /// <param name="configSectionPath">The section holding <see cref="RabbitMqSubscriberOptions"/>, e.g. <c>Messaging:Orders</c>.</param>
    IRabbitMqConfigBuilder AddSubscriber<[DynamicallyAccessedMembers(DynamicallyAccessedMemberTypes.PublicConstructors)] THandler, TMessage>(
        string configSectionPath)
        where THandler : class, IRabbitMqHandler<TMessage>;

    /// <summary>
    /// Registers the singleton <see cref="IRabbitMqPublisher"/>, which publishes bodies you have already serialized.
    /// There is one raw publisher.
    /// </summary>
    /// <param name="configure">Sets the publisher options; <see cref="RabbitMqPublisherOptions.Exchange"/> is required.</param>
    IRabbitMqConfigBuilder AddPublisher(Action<RabbitMqPublisherOptions> configure);

    /// <summary>Registers the singleton <see cref="IRabbitMqPublisher"/> with options bound from a configuration section.</summary>
    /// <param name="configSectionPath">The section holding <see cref="RabbitMqPublisherOptions"/>.</param>
    IRabbitMqConfigBuilder AddPublisher(string configSectionPath);

    /// <summary>Registers a singleton <see cref="IRabbitMqPublisher{TMessage}"/>. One publisher per message type.</summary>
    /// <typeparam name="TMessage">The message type.</typeparam>
    /// <param name="configure">Sets the publisher options; <see cref="RabbitMqPublisherOptions.Exchange"/> is required.</param>
    IRabbitMqConfigBuilder AddPublisher<TMessage>(Action<RabbitMqPublisherOptions> configure);

    /// <summary>Registers a singleton <see cref="IRabbitMqPublisher{TMessage}"/> with options bound from a configuration section.</summary>
    /// <typeparam name="TMessage">The message type.</typeparam>
    /// <param name="configSectionPath">The section holding <see cref="RabbitMqPublisherOptions"/>.</param>
    IRabbitMqConfigBuilder AddPublisher<TMessage>(string configSectionPath);
}
