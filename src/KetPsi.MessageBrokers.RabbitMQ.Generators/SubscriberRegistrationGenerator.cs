using System.Collections.Immutable;
using System.Text;
using Microsoft.CodeAnalysis;
using Microsoft.CodeAnalysis.CSharp;
using Microsoft.CodeAnalysis.CSharp.Syntax;

namespace KetPsi.MessageBrokers.RabbitMQ.Generators;

/// <summary>
/// Intercepts every <c>IRabbitMqConfigBuilder.AddSubscriber&lt;THandler&gt;</c> call, typically inside
/// <c>AddRabbitMq(rabbit =&gt; ...)</c>, and replaces it with <c>AddSubscriber&lt;THandler, TMessage&gt;</c>, taking
/// <c>TMessage</c> from the <c>IRabbitMqHandler&lt;T&gt;</c> the handler implements. Registration then needs no reflection.
/// </summary>
[Generator(LanguageNames.CSharp)]
public sealed class SubscriberRegistrationGenerator : IIncrementalGenerator
{
    private const string LibraryNamespace = "KetPsi.MessageBrokers.RabbitMQ";
    private const string ConfigBuilderName = "IRabbitMqConfigBuilder";
    private const string HandlerInterfaceName = "IRabbitMqHandler";
    private const string AddSubscriberName = "AddSubscriber";

    /// <summary>The namespace of the generated interceptors; consuming projects list it in <c>InterceptorsNamespaces</c>.</summary>
    public const string InterceptorsNamespace = "KetPsi.MessageBrokers.RabbitMQ.Generated";

    private static readonly SymbolDisplayFormat s_typeFormat = SymbolDisplayFormat.FullyQualifiedFormat
        .AddMiscellaneousOptions(SymbolDisplayMiscellaneousOptions.IncludeNullableReferenceTypeModifier);

    /// <inheritdoc/>
    public void Initialize(IncrementalGeneratorInitializationContext context)
    {
        var results = context.SyntaxProvider
            .CreateSyntaxProvider(static (node, _) => IsCandidate(node), static (syntax, cancellationToken) => Analyze(syntax, cancellationToken))
            .Where(static result => result is not null)
            .Select(static (result, _) => result!);

        context.RegisterSourceOutput(
            results.Where(static result => result.Diagnostic is not null),
            static (output, result) => output.ReportDiagnostic(result.Diagnostic!.ToDiagnostic()));

        var calls = results
            .Where(static result => result.Call is not null)
            .Select(static (result, _) => result.Call!)
            .Collect();

        context.RegisterSourceOutput(calls, static (output, calls) =>
        {
            if (!calls.IsEmpty)
            {
                output.AddSource("RabbitMqSubscriberRegistrations.g.cs", Emit(calls));
            }
        });
    }

    private static bool IsCandidate(SyntaxNode node)
        => node is InvocationExpressionSyntax
        {
            ArgumentList.Arguments.Count: 1,
            Expression: MemberAccessExpressionSyntax
            {
                Name: GenericNameSyntax { Identifier.ValueText: AddSubscriberName, TypeArgumentList.Arguments.Count: 1 },
            },
        };

    private static SubscriberCallResult? Analyze(GeneratorSyntaxContext syntax, CancellationToken cancellationToken)
    {
        var invocation = (InvocationExpressionSyntax)syntax.Node;
        if (syntax.SemanticModel.GetSymbolInfo(invocation, cancellationToken).Symbol is not IMethodSymbol { TypeArguments.Length: 1, Parameters.Length: 1 } method
            || !IsLibraryType(method.ContainingType, ConfigBuilderName))
        {
            return null;
        }

        var name = ((MemberAccessExpressionSyntax)invocation.Expression).Name;
        var handler = method.TypeArguments[0];
        var handlerName = handler.ToDisplayString();

        // A raw handler (IRabbitMqHandler) has no message type to infer; the library registers it without the generator.
        var isRawHandler = ImplementsRawHandler(handler);

        if (handler is not INamedTypeSymbol namedHandler || ContainsTypeParameter(handler))
        {
            return isRawHandler ? null : Failure(SubscriberDiagnostics.OpenHandlerType, name, handlerName);
        }

        if (namedHandler.IsAbstract)
        {
            return Failure(SubscriberDiagnostics.AbstractHandler, name, handlerName);
        }

        var messageTypes = namedHandler.AllInterfaces
            .Where(static type => IsLibraryType(type, HandlerInterfaceName) && type.TypeArguments.Length == 1)
            .Select(static type => type.TypeArguments[0])
            .Distinct<ITypeSymbol>(SymbolEqualityComparer.Default)
            .ToList();

        if (isRawHandler)
        {
            return messageTypes.Count == 0
                ? null
                : Failure(SubscriberDiagnostics.RawAndTypedHandler, name, handlerName, string.Join(", ", messageTypes.Select(static type => type.ToDisplayString())));
        }

        if (messageTypes.Count == 0)
        {
            return Failure(SubscriberDiagnostics.NoHandlerInterface, name, handlerName);
        }

        if (messageTypes.Count > 1)
        {
            return Failure(SubscriberDiagnostics.AmbiguousHandlerInterface, name, handlerName, string.Join(", ", messageTypes.Select(static type => type.ToDisplayString())));
        }

        var messageType = messageTypes[0];
        var compilation = syntax.SemanticModel.Compilation;
        foreach (var type in new[] { handler, messageType })
        {
            if (!IsAccessible(compilation, type))
            {
                return Failure(SubscriberDiagnostics.InaccessibleType, name, type.ToDisplayString());
            }
        }

        if (syntax.SemanticModel.GetInterceptableLocation(invocation, cancellationToken) is not { } location)
        {
            return null;
        }

        var call = new SubscriberCall(
            handler.ToDisplayString(s_typeFormat),
            messageType.ToDisplayString(s_typeFormat),
            BindsSection: method.Parameters[0].Type.SpecialType == SpecialType.System_String,
            location.Version,
            location.Data,
            location.GetDisplayLocation());
        return new SubscriberCallResult(call, Diagnostic: null);
    }

    private static SubscriberCallResult Failure(DiagnosticDescriptor descriptor, SyntaxNode node, string argument0, string argument1 = "")
        => new(Call: null, DiagnosticInfo.Create(descriptor, node, argument0, argument1));

    private static bool ImplementsRawHandler(ITypeSymbol type)
        => type.AllInterfaces.Any(IsRawHandlerInterface)
            || (type is ITypeParameterSymbol parameter
                && parameter.ConstraintTypes.Any(constraint => IsRawHandlerInterface(constraint as INamedTypeSymbol) || constraint.AllInterfaces.Any(IsRawHandlerInterface)));

    private static bool IsRawHandlerInterface(INamedTypeSymbol? type)
        => IsLibraryType(type, HandlerInterfaceName) && type!.TypeArguments.Length == 0;

    private static bool IsLibraryType(INamedTypeSymbol? type, string name)
        => type is not null && type.Name == name && type.ContainingNamespace.ToDisplayString() == LibraryNamespace;

    private static bool ContainsTypeParameter(ITypeSymbol type) => type switch
    {
        ITypeParameterSymbol => true,
        IArrayTypeSymbol array => ContainsTypeParameter(array.ElementType),
        INamedTypeSymbol named => named.TypeArguments.Any(ContainsTypeParameter),
        _ => false,
    };

    private static bool IsAccessible(Compilation compilation, ITypeSymbol type) => type switch
    {
        IArrayTypeSymbol array => IsAccessible(compilation, array.ElementType),
        INamedTypeSymbol named => !named.IsFileLocal
            && compilation.IsSymbolAccessibleWithin(named, compilation.Assembly)
            && named.TypeArguments.All(argument => IsAccessible(compilation, argument)),
        _ => true,
    };

    private static string Emit(ImmutableArray<SubscriberCall> calls)
    {
        var source = new StringBuilder();
        source.AppendLine("""
            // <auto-generated/>
            #nullable enable

            namespace System.Runtime.CompilerServices
            {
                [global::System.AttributeUsage(global::System.AttributeTargets.Method, AllowMultiple = true)]
                file sealed class InterceptsLocationAttribute : global::System.Attribute
                {
                    public InterceptsLocationAttribute(int version, string data)
                    {
                    }
                }
            }

            """);
        source.AppendLine($"namespace {InterceptorsNamespace}");
        source.AppendLine("""
            {
                [global::System.CodeDom.Compiler.GeneratedCode("KetPsi.MessageBrokers.RabbitMQ.Generators", "1.0.0")]
                file static class RabbitMqSubscriberRegistrations
                {
            """);

        // Calls with the same handler, message type and overload share one interceptor.
        var index = 0;
        foreach (var group in calls.GroupBy(static call => (call.HandlerType, call.MessageType, call.BindsSection)))
        {
            if (index > 0)
            {
                source.AppendLine();
            }

            foreach (var call in group)
            {
                source.AppendLine($"        // {call.DisplayLocation}");
                source.AppendLine($"        [global::System.Runtime.CompilerServices.InterceptsLocation({call.LocationVersion}, \"{call.LocationData}\")]");
            }

            var (handlerType, messageType, bindsSection) = group.Key;
            var (parameterType, parameterName) = bindsSection
                ? ("string", "configSectionPath")
                : ("global::System.Action<global::KetPsi.MessageBrokers.RabbitMQ.RabbitMqSubscriberOptions>", "configure");

            source.AppendLine($"        public static global::KetPsi.MessageBrokers.RabbitMQ.IRabbitMqConfigBuilder AddSubscriber{index}(");
            source.AppendLine("            this global::KetPsi.MessageBrokers.RabbitMQ.IRabbitMqConfigBuilder builder,");
            source.AppendLine($"            {parameterType} {parameterName})");
            source.AppendLine($"            => builder.AddSubscriber<{handlerType}, {messageType}>({parameterName});");
            index++;
        }

        source.AppendLine("""
                }
            }
            """);
        return source.ToString();
    }
}
