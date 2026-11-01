using Microsoft.CodeAnalysis;

namespace KetPsi.MessageBrokers.RabbitMQ.Generators;

internal static class SubscriberDiagnostics
{
    private const string Category = "KetPsi.RabbitMQ";

    public static readonly DiagnosticDescriptor NoHandlerInterface = new(
        "KPRMQ001",
        "Handler implements no handler interface",
        "'{0}' implements neither IRabbitMqHandler<T> nor IRabbitMqHandler, so AddSubscriber<{0}> cannot register it",
        Category,
        DiagnosticSeverity.Error,
        isEnabledByDefault: true);

    public static readonly DiagnosticDescriptor AmbiguousHandlerInterface = new(
        "KPRMQ002",
        "Handler implements IRabbitMqHandler<T> more than once",
        "'{0}' handles more than one message type ({1}); call AddSubscriber<{0}, TMessage> once per message type instead",
        Category,
        DiagnosticSeverity.Error,
        isEnabledByDefault: true);

    public static readonly DiagnosticDescriptor OpenHandlerType = new(
        "KPRMQ003",
        "Handler type is not known at compile time",
        "AddSubscriber<{0}> needs a concrete handler type to infer the message type; call AddSubscriber<{0}, TMessage> instead",
        Category,
        DiagnosticSeverity.Error,
        isEnabledByDefault: true);

    public static readonly DiagnosticDescriptor AbstractHandler = new(
        "KPRMQ004",
        "Handler is abstract or an interface",
        "'{0}' is abstract or an interface; register a concrete handler class that DI can create",
        Category,
        DiagnosticSeverity.Error,
        isEnabledByDefault: true);

    public static readonly DiagnosticDescriptor RawAndTypedHandler = new(
        "KPRMQ006",
        "Handler implements both IRabbitMqHandler and IRabbitMqHandler<T>",
        "'{0}' implements both IRabbitMqHandler and IRabbitMqHandler<{1}>; implement IRabbitMqHandler for raw messages or IRabbitMqHandler<T> for deserialized ones, not both",
        Category,
        DiagnosticSeverity.Error,
        isEnabledByDefault: true);

    public static readonly DiagnosticDescriptor InaccessibleType = new(
        "KPRMQ005",
        "Type is not accessible to generated code",
        "'{0}' must be public or internal, and not nested in a private type or declared 'file', so the generated registration can use it",
        Category,
        DiagnosticSeverity.Error,
        isEnabledByDefault: true);
}
