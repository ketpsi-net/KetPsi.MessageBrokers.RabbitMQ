; Unshipped analyzer release
; https://github.com/dotnet/roslyn-analyzers/blob/main/src/Microsoft.CodeAnalysis.Analyzers/ReleaseTrackingAnalyzers.Help.md

### New Rules

Rule ID | Category | Severity | Notes
--------|----------|----------|-------
KPRMQ001 | KetPsi.RabbitMQ | Error | The handler implements no IRabbitMqHandler<T>
KPRMQ002 | KetPsi.RabbitMQ | Error | The handler implements IRabbitMqHandler<T> more than once
KPRMQ003 | KetPsi.RabbitMQ | Error | The handler type is not known at compile time
KPRMQ004 | KetPsi.RabbitMQ | Error | The handler is abstract or an interface
KPRMQ005 | KetPsi.RabbitMQ | Error | The handler or message type is not accessible to generated code
