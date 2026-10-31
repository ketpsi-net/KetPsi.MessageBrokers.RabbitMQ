using System.Diagnostics.CodeAnalysis;
using System.Text.Json;
using System.Text.Json.Serialization;
using System.Text.Json.Serialization.Metadata;

namespace KetPsi.MessageBrokers.RabbitMQ;

/// <summary>Where message types get their JSON contracts from.</summary>
internal sealed class RabbitMqJsonOptions
{
    private JsonSerializerOptions? _reflectionOptions;

    /// <summary>Contexts registered with <see cref="IRabbitMqConfigBuilder.UseJsonContext"/>, then every <see cref="JsonSerializerContext"/> service.</summary>
    public List<JsonSerializerContext> Contexts { get; } = [];

    /// <summary>
    /// The options set with <see cref="IRabbitMqConfigBuilder.ConfigureJsonSerializer"/>. Its resolver chain is consulted
    /// after the contexts, and the reflection fallback uses its settings.
    /// </summary>
    public JsonSerializerOptions SerializerOptions { get; } = new();

    /// <summary>Every <see cref="JsonSerializerOptions"/> service; their resolver chains are consulted after <see cref="SerializerOptions"/>.</summary>
    public List<JsonSerializerOptions> ServiceSerializerOptions { get; } = [];

    /// <summary>
    /// Whether to fall back to reflection for types no contract source knows. Always false under Native AOT, or when
    /// <c>JsonSerializerIsReflectionEnabledByDefault</c> is false; tests set it to false to act like Native AOT.
    /// </summary>
    public bool ReflectionEnabled { get; set; } = JsonSerializer.IsReflectionEnabledByDefault;

    /// <summary>
    /// Returns the type info for <typeparamref name="T"/> from, in order: the contexts (each with its own options, so its
    /// <see cref="JsonSourceGenerationOptionsAttribute"/> settings apply), <see cref="SerializerOptions"/>,
    /// <see cref="ServiceSerializerOptions"/>, and reflection when it is enabled.
    /// </summary>
    public JsonTypeInfo<T>? FindTypeInfo<T>()
    {
        foreach (var context in Contexts)
        {
            if (context.GetTypeInfo(typeof(T)) is JsonTypeInfo<T> typeInfo)
            {
                return typeInfo;
            }
        }

        if (FindTypeInfo<T>(SerializerOptions) is { } configured)
        {
            return configured;
        }

        foreach (var options in ServiceSerializerOptions)
        {
            if (FindTypeInfo<T>(options) is { } fromService)
            {
                return fromService;
            }
        }

        // IsReflectionEnabledByDefault is a feature switch the SDK sets to false for trimmed and Native AOT apps,
        // so there the trimmer removes this branch and the reflection resolver with it.
        if (JsonSerializer.IsReflectionEnabledByDefault && ReflectionEnabled)
        {
            _reflectionOptions ??= CreateReflectionOptions(SerializerOptions);
            return FindTypeInfo<T>(_reflectionOptions);
        }

        return null;
    }

    /// <exception cref="InvalidOperationException">No contract source knows <typeparamref name="T"/>.</exception>
    public JsonTypeInfo<T> GetTypeInfo<T>()
        => FindTypeInfo<T>() ?? throw new InvalidOperationException(MissingTypeMessage<T>());

    public static string MissingTypeMessage<T>()
        => $"No JSON type information for '{typeof(T).FullName}', and reflection-based serialization is disabled "
            + "(Native AOT, or JsonSerializerIsReflectionEnabledByDefault=false). "
            + $"Add [JsonSerializable(typeof({typeof(T).Name}))] to a JsonSerializerContext, and register that context with "
            + "UseJsonContext, as a JsonSerializerContext service, or in the TypeInfoResolverChain of ConfigureJsonSerializer.";

    private static JsonTypeInfo<T>? FindTypeInfo<T>(JsonSerializerOptions options)
    {
        if (options.TypeInfoResolver is null)
        {
            return null;
        }

#if NET9_0_OR_GREATER
        return options.TryGetTypeInfo(typeof(T), out var typeInfo) ? typeInfo as JsonTypeInfo<T> : null;
#else
        // .NET 8 has no TryGetTypeInfo; GetTypeInfo throws NotSupportedException when the resolver does not know the type.
        try
        {
            return options.GetTypeInfo(typeof(T)) as JsonTypeInfo<T>;
        }
        catch (NotSupportedException)
        {
            return null;
        }
#endif
    }

    /// <summary>A copy of <paramref name="settings"/> that resolves contracts through reflection.</summary>
    /// <remarks>
    /// The one suppression in the library, the same one ASP.NET Core's JsonOptions makes: the only caller is guarded by
    /// <see cref="JsonSerializer.IsReflectionEnabledByDefault"/>, which is false wherever trimming or Native AOT runs, so
    /// this method is removed from those apps. No feature guard can express that, because .NET has no built-in guard for
    /// <see cref="RequiresUnreferencedCodeAttribute"/>.
    /// </remarks>
    [UnconditionalSuppressMessage("Trimming", "IL2026:RequiresUnreferencedCode",
        Justification = "Only called when JsonSerializer.IsReflectionEnabledByDefault is true, which is never the case in trimmed apps.")]
    [UnconditionalSuppressMessage("AOT", "IL3050:RequiresDynamicCode",
        Justification = "Only called when JsonSerializer.IsReflectionEnabledByDefault is true, which is never the case in Native AOT apps.")]
    private static JsonSerializerOptions CreateReflectionOptions(JsonSerializerOptions settings)
    {
        var options = new JsonSerializerOptions(settings) { TypeInfoResolver = new DefaultJsonTypeInfoResolver() };
        options.MakeReadOnly();
        return options;
    }
}
