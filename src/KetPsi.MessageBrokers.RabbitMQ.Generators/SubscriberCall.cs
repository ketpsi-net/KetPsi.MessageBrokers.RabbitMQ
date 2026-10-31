using Microsoft.CodeAnalysis;
using Microsoft.CodeAnalysis.Text;

namespace KetPsi.MessageBrokers.RabbitMQ.Generators;

/// <summary>One <c>AddSubscriber&lt;THandler&gt;</c> call to intercept. Holds only strings, so the pipeline can cache it.</summary>
internal sealed record SubscriberCall(
    string HandlerType,
    string MessageType,
    bool BindsSection,
    int LocationVersion,
    string LocationData,
    string DisplayLocation);

/// <summary>A diagnostic, kept without symbols or syntax trees so the pipeline can cache it.</summary>
internal sealed record DiagnosticInfo(
    DiagnosticDescriptor Descriptor,
    string FilePath,
    TextSpan Span,
    LinePositionSpan LineSpan,
    string Argument0,
    string Argument1)
{
    public static DiagnosticInfo Create(DiagnosticDescriptor descriptor, SyntaxNode node, string argument0, string argument1 = "")
    {
        var location = node.GetLocation();
        return new DiagnosticInfo(descriptor, location.SourceTree?.FilePath ?? "", location.SourceSpan, location.GetLineSpan().Span, argument0, argument1);
    }

    public Diagnostic ToDiagnostic()
        => Diagnostic.Create(Descriptor, Location.Create(FilePath, Span, LineSpan), Argument0, Argument1);
}

/// <summary>The outcome for one candidate call: a call to intercept, or the reason it cannot be.</summary>
internal sealed record SubscriberCallResult(SubscriberCall? Call, DiagnosticInfo? Diagnostic);
