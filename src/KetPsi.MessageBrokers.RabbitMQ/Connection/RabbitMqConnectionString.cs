using System.Diagnostics.CodeAnalysis;
using System.Globalization;

namespace KetPsi.MessageBrokers.RabbitMQ;

/// <summary>
/// A parsed connection string of the form <c>amqp[s]://[user[:password]@]host[:port][/virtualHost]</c>. Environment
/// variables written as <c>%NAME%</c> are expanded first, and percent-encoded parts are decoded.
/// </summary>
/// <remarks>
/// Parsed by hand rather than with <see cref="Uri"/>, which works under Native AOT but would misread credentials:
/// splitting at the last <c>@</c> lets a password contain a raw <c>@</c>, <c>:</c>, <c>/</c>, <c>?</c> or <c>#</c>.
/// A literal <c>@</c> in the virtual host must therefore be encoded as <c>%40</c>. Errors never repeat the credentials.
/// This is a class rather than a record so that <see cref="object.ToString"/> cannot print the password.
/// </remarks>
internal sealed class RabbitMqConnectionString
{
    public const int DefaultPort = 5672;
    public const int DefaultTlsPort = 5671;
    public const string DefaultVirtualHost = "/";

    private const string AmqpScheme = "amqp";
    private const string AmqpsScheme = "amqps";
    private const string SchemeSeparator = "://";

    private RabbitMqConnectionString(bool useTls, string hostName, int port, string? userName, string? password, string virtualHost)
    {
        UseTls = useTls;
        HostName = hostName;
        Port = port;
        UserName = userName;
        Password = password;
        VirtualHost = virtualHost;
    }

    /// <summary>Whether the scheme is <c>amqps</c>.</summary>
    public bool UseTls { get; }

    public string HostName { get; }

    /// <summary>The port, or 5672 (5671 for <c>amqps</c>) when the connection string has none.</summary>
    public int Port { get; }

    /// <summary>The user, or null when the connection string has none.</summary>
    public string? UserName { get; }

    /// <summary>The password, or null when the connection string has none.</summary>
    public string? Password { get; }

    /// <summary>The virtual host, or <c>/</c> when the connection string has none.</summary>
    public string VirtualHost { get; }

    /// <exception cref="FormatException"><paramref name="connectionString"/> is not a valid connection string.</exception>
    public static RabbitMqConnectionString Parse(string connectionString)
        => TryParse(connectionString, out var result, out var error) ? result : throw new FormatException(error);

    public static bool TryParse(
        string connectionString,
        [NotNullWhen(true)] out RabbitMqConnectionString? result,
        [NotNullWhen(false)] out string? error)
    {
        ArgumentNullException.ThrowIfNull(connectionString);
        result = null;

        var value = Environment.ExpandEnvironmentVariables(connectionString).Trim();
        var schemeEnd = value.IndexOf(SchemeSeparator, StringComparison.Ordinal);
        if (schemeEnd < 0)
        {
            error = Error("must start with amqp:// or amqps://"
                + (value.Contains('%', StringComparison.Ordinal) ? "; check that the environment variables it uses are set" : string.Empty));
            return false;
        }

        var scheme = value.AsSpan(0, schemeEnd);
        bool useTls;
        if (scheme.Equals(AmqpScheme, StringComparison.OrdinalIgnoreCase))
        {
            useTls = false;
        }
        else if (scheme.Equals(AmqpsScheme, StringComparison.OrdinalIgnoreCase))
        {
            useTls = true;
        }
        else
        {
            error = Error($"has the scheme '{scheme}'; use amqp or amqps");
            return false;
        }

        var rest = value.AsSpan(schemeEnd + SchemeSeparator.Length);

        string? userName = null;
        string? password = null;
        var at = rest.LastIndexOf('@');
        if (at >= 0)
        {
            var userInfo = rest[..at];
            rest = rest[(at + 1)..];

            var colon = userInfo.IndexOf(':');
            var user = colon < 0 ? userInfo : userInfo[..colon];
            userName = user.IsEmpty ? null : Decode(user);
            password = colon < 0 ? null : Decode(userInfo[(colon + 1)..]);
        }

        if (rest.IndexOfAny('?', '#') >= 0)
        {
            error = Error("has a query string or fragment, which is not supported");
            return false;
        }

        var slash = rest.IndexOf('/');
        var authority = slash < 0 ? rest : rest[..slash];
        var path = slash < 0 ? [] : rest[(slash + 1)..];
        if (path.Contains('/'))
        {
            error = Error("has a virtual host with more than one path segment; encode a '/' in the virtual host as %2F");
            return false;
        }

        if (!TrySplitAuthority(authority, out var host, out var portText, out error))
        {
            return false;
        }

        if (host.IsEmpty)
        {
            error = Error("has no host");
            return false;
        }

        var port = useTls ? DefaultTlsPort : DefaultPort;
        if (portText is { } text
            && (!int.TryParse(text, NumberStyles.None, CultureInfo.InvariantCulture, out port) || port is < 1 or > 65535))
        {
            error = Error($"has the invalid port '{text}'");
            return false;
        }

        var virtualHost = path.IsEmpty ? DefaultVirtualHost : Decode(path);
        result = new RabbitMqConnectionString(useTls, Decode(host), port, userName, password, virtualHost);
        error = null;
        return true;
    }

    /// <summary>Splits <c>host[:port]</c>, where the host may be an IPv6 address in brackets.</summary>
    private static bool TrySplitAuthority(
        ReadOnlySpan<char> authority,
        out ReadOnlySpan<char> host,
        out string? port,
        [NotNullWhen(false)] out string? error)
    {
        ReadOnlySpan<char> afterHost;
        if (authority.StartsWith("[", StringComparison.Ordinal))
        {
            var close = authority.IndexOf(']');
            if (close < 0)
            {
                host = default;
                port = null;
                error = Error("has an IPv6 host without a closing ']'");
                return false;
            }

            host = authority[1..close];
            afterHost = authority[(close + 1)..];
        }
        else
        {
            var colon = authority.IndexOf(':');
            host = colon < 0 ? authority : authority[..colon];
            afterHost = colon < 0 ? [] : authority[colon..];
        }

        port = null;
        if (!afterHost.IsEmpty)
        {
            if (afterHost[0] != ':')
            {
                error = Error("has text after the host that is not a port");
                return false;
            }

            port = afterHost[1..].ToString();
        }

        error = null;
        return true;
    }

    private static string Decode(ReadOnlySpan<char> value) => Uri.UnescapeDataString(value.ToString());

    private static string Error(string reason) => $"The RabbitMQ connection string {reason}.";
}
