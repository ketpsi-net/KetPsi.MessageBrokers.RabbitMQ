using System.Collections.Immutable;
using Microsoft.CodeAnalysis;
using Microsoft.CodeAnalysis.CSharp;

namespace KetPsi.MessageBrokers.RabbitMQ.Generators.Tests;

public sealed class SubscriberRegistrationGeneratorTests
{
    private const string Usings = """
        using KetPsi.MessageBrokers.RabbitMQ;
        using Microsoft.Extensions.DependencyInjection;

        """;

    private const string OrderHandler = """
        public sealed record Order(string Id);

        public sealed class OrderHandler : IRabbitMqHandler<Order>
        {
            public Task HandleAsync(MessageContext<Order> context, CancellationToken cancellationToken) => Task.CompletedTask;
        }

        """;

    [Fact]
    public void Both_overloads_are_rewritten_with_the_inferred_message_type()
    {
        var result = Run(Usings + OrderHandler + """
            public static class Registration
            {
                public static void Register(IServiceCollection services)
                    => services.AddRabbitMq(rabbit => rabbit
                        .AddSubscriber<OrderHandler>(options => options.Queue = "orders")
                        .AddSubscriber<OrderHandler>("Messaging:Orders")
                        .AddSubscriber<OrderHandler>("Messaging:MoreOrders"));
            }
            """);

        Assert.Empty(result.Diagnostics);
        Assert.Empty(result.CompilationErrors);
        var source = Assert.Single(result.GeneratedSources);
        Assert.Contains("builder.AddSubscriber<global::OrderHandler, global::Order>(configure);", source, StringComparison.Ordinal);
        Assert.Contains("builder.AddSubscriber<global::OrderHandler, global::Order>(configSectionPath);", source, StringComparison.Ordinal);
        // The two section calls share one interceptor.
        Assert.Equal(3, Count(source, "[global::System.Runtime.CompilerServices.InterceptsLocation("));
        Assert.Equal(2, Count(source, "public static global::KetPsi.MessageBrokers.RabbitMQ.IRabbitMqConfigBuilder AddSubscriber"));
    }

    [Fact]
    public void The_explicit_overload_is_left_alone()
    {
        var result = Run(Usings + OrderHandler + """
            public static class Registration
            {
                public static void Register(IServiceCollection services)
                    => services.AddRabbitMq(rabbit => rabbit.AddSubscriber<OrderHandler, Order>("Messaging:Orders"));
            }
            """);

        Assert.Empty(result.Diagnostics);
        Assert.Empty(result.CompilationErrors);
        Assert.Empty(result.GeneratedSources);
    }

    [Fact]
    public void A_handler_without_a_handler_interface_is_reported()
    {
        var result = Run(Usings + """
            public sealed class NotAHandler;

            public static class Registration
            {
                public static void Register(IServiceCollection services)
                    => services.AddRabbitMq(rabbit => rabbit.AddSubscriber<NotAHandler>("Messaging:Orders"));
            }
            """);

        AssertSingleDiagnostic(result, "KPRMQ001");
    }

    [Fact]
    public void A_handler_of_two_message_types_is_reported()
    {
        var result = Run(Usings + OrderHandler + """
            public sealed record Refund(string Id);

            public sealed class BillingHandler : IRabbitMqHandler<Order>, IRabbitMqHandler<Refund>
            {
                public Task HandleAsync(MessageContext<Order> context, CancellationToken cancellationToken) => Task.CompletedTask;

                public Task HandleAsync(MessageContext<Refund> context, CancellationToken cancellationToken) => Task.CompletedTask;
            }

            public static class Registration
            {
                public static void Register(IServiceCollection services)
                    => services.AddRabbitMq(rabbit => rabbit.AddSubscriber<BillingHandler>("Messaging:Billing"));
            }
            """);

        var diagnostic = AssertSingleDiagnostic(result, "KPRMQ002");
        Assert.Contains("Order, Refund", diagnostic.GetMessage(), StringComparison.Ordinal);
    }

    [Fact]
    public void A_handler_type_parameter_is_reported()
    {
        var result = Run(Usings + """
            public static class Registration
            {
                public static void Register<THandler>(IRabbitMqConfigBuilder rabbit)
                    where THandler : class
                    => rabbit.AddSubscriber<THandler>("Messaging:Orders");
            }
            """);

        AssertSingleDiagnostic(result, "KPRMQ003");
    }

    [Fact]
    public void An_abstract_handler_is_reported()
    {
        var result = Run(Usings + """
            public sealed record Order(string Id);

            public abstract class OrderHandlerBase : IRabbitMqHandler<Order>
            {
                public abstract Task HandleAsync(MessageContext<Order> context, CancellationToken cancellationToken);
            }

            public static class Registration
            {
                public static void Register(IRabbitMqConfigBuilder rabbit) => rabbit.AddSubscriber<OrderHandlerBase>("Messaging:Orders");
            }
            """);

        AssertSingleDiagnostic(result, "KPRMQ004");
    }

    [Fact]
    public void A_private_handler_is_reported()
    {
        var result = Run(Usings + """
            public sealed record Order(string Id);

            public static class Registration
            {
                public static void Register(IRabbitMqConfigBuilder rabbit) => rabbit.AddSubscriber<Hidden>("Messaging:Orders");

                private sealed class Hidden : IRabbitMqHandler<Order>
                {
                    public Task HandleAsync(MessageContext<Order> context, CancellationToken cancellationToken) => Task.CompletedTask;
                }
            }
            """);

        AssertSingleDiagnostic(result, "KPRMQ005");
    }

    private static Diagnostic AssertSingleDiagnostic(GeneratorResult result, string id)
    {
        var diagnostic = Assert.Single(result.Diagnostics);
        Assert.Equal(id, diagnostic.Id);
        Assert.Equal(DiagnosticSeverity.Error, diagnostic.Severity);
        Assert.Empty(result.GeneratedSources);
        return diagnostic;
    }

    private static int Count(string text, string value)
    {
        var count = 0;
        for (var index = text.IndexOf(value, StringComparison.Ordinal); index >= 0; index = text.IndexOf(value, index + value.Length, StringComparison.Ordinal))
        {
            count++;
        }

        return count;
    }

    private static GeneratorResult Run(string source)
    {
        var parseOptions = CSharpParseOptions.Default
            .WithLanguageVersion(LanguageVersion.Latest)
            .WithFeatures([new("InterceptorsNamespaces", SubscriberRegistrationGenerator.InterceptorsNamespace)]);
        var compilation = CSharpCompilation.Create(
            "GeneratorInput",
            [
                CSharpSyntaxTree.ParseText(source, parseOptions),
                CSharpSyntaxTree.ParseText("global using System; global using System.Threading; global using System.Threading.Tasks;", parseOptions),
            ],
            References.Value,
            new CSharpCompilationOptions(OutputKind.DynamicallyLinkedLibrary, nullableContextOptions: NullableContextOptions.Enable));

        var driver = CSharpGeneratorDriver.Create([new SubscriberRegistrationGenerator().AsSourceGenerator()], parseOptions: parseOptions)
            .RunGeneratorsAndUpdateCompilation(compilation, out var output, out var diagnostics, TestContext.Current.CancellationToken);

        var generated = driver.GetRunResult().GeneratedTrees.Select(static tree => tree.ToString()).ToImmutableArray();
        var errors = output.GetDiagnostics(TestContext.Current.CancellationToken)
            .Where(static diagnostic => diagnostic.Severity == DiagnosticSeverity.Error)
            .ToImmutableArray();
        return new GeneratorResult(diagnostics, generated, errors);
    }

    private static readonly Lazy<ImmutableArray<MetadataReference>> References = new(static () =>
        [
            .. ((string)AppContext.GetData("TRUSTED_PLATFORM_ASSEMBLIES")!)
                .Split(Path.PathSeparator)
                .Select(static path => MetadataReference.CreateFromFile(path)),
            MetadataReference.CreateFromFile(typeof(IRabbitMqConfigBuilder).Assembly.Location),
        ]);

    private sealed record GeneratorResult(
        ImmutableArray<Diagnostic> Diagnostics,
        ImmutableArray<string> GeneratedSources,
        ImmutableArray<Diagnostic> CompilationErrors);
}
