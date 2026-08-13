using Confluent.Kafka;

namespace RedePanda.Contracts;

/// <summary>
/// How every client in this repository authenticates and encrypts its connection to the broker.
/// <para>
/// This is what "the backing service is swappable through configuration" costs in practice. The
/// broker in the chart speaks plaintext on a cluster-internal Service; any broker outside it —
/// managed Redpanda, a shared Kafka, anything reachable over a network someone else owns — needs
/// TLS, credentials, or both. Without this, pointing <c>REDPANDA_BOOTSTRAP_SERVERS</c> elsewhere
/// would need a code change, which is precisely the claim it is meant to avoid.
/// </para>
/// <para>
/// It lives in Contracts, which otherwise holds no Kafka dependency at all, because
/// <see cref="ClientConfig"/> is the shared base of the producer, consumer and admin
/// configurations: one mapping here serves all seven client sites in the repository, and seven
/// copies of it would drift. Contracts already owns the other thing every client must agree on,
/// the wire format.
/// </para>
/// <para>
/// The count is worth keeping honest. It read "five" while there were seven, and the two it did
/// not account for were the two admin clients -- one of which had been shipped without this
/// applied at all, failing every readiness probe against a secured broker.
/// </para>
/// <para>
/// Nothing configured leaves the config untouched, so the bundled plaintext demo behaves exactly
/// as it did before any of this existed.
/// </para>
/// </summary>
public static class KafkaSecurity
{
    public const string ProtocolVariable = "REDPANDA_SECURITY_PROTOCOL";
    public const string MechanismVariable = "REDPANDA_SASL_MECHANISM";
    public const string UsernameVariable = "REDPANDA_SASL_USERNAME";
    public const string PasswordVariable = "REDPANDA_SASL_PASSWORD";
    public const string CaLocationVariable = "REDPANDA_SSL_CA_LOCATION";

    /// <summary>Reads the settings from the process environment (12-Factor).</summary>
    public static void ApplyTo(ClientConfig config) =>
        ApplyTo(config, Environment.GetEnvironmentVariable);

    /// <param name="read">
    /// Where a setting comes from. Injected rather than read directly so the behaviour can be
    /// tested without touching process-global state.
    /// </param>
    public static void ApplyTo(ClientConfig config, Func<string, string?> read)
    {
        var caLocation = Value(read, CaLocationVariable);
        var protocol = ParseProtocol(Value(read, ProtocolVariable));

        // Plaintext is librdkafka's own default, so it is left unset rather than written back:
        // an unconfigured client's config stays identical to what it was before this existed.
        if (protocol is null or SecurityProtocol.Plaintext)
        {
            // Not merely useless: a CA bundle here means someone believes this connection is
            // encrypted. It is not, and staying quiet about that is the worst of the options.
            if (caLocation is not null)
            {
                throw new InvalidOperationException(
                    $"{CaLocationVariable} is set, but {ProtocolVariable} does not use TLS. " +
                    $"Set {ProtocolVariable} to Ssl or SaslSsl, or remove {CaLocationVariable}.");
            }

            return;
        }

        // Non-nullable from here on; the early return above covers the other case.
        var secured = protocol.Value;
        config.SecurityProtocol = secured;

        var usesTls = secured is SecurityProtocol.Ssl or SecurityProtocol.SaslSsl;
        if (caLocation is not null)
        {
            if (!usesTls)
            {
                throw new InvalidOperationException(
                    $"{CaLocationVariable} is set, but {ProtocolVariable} is '{secured}', which " +
                    "does not use TLS. Set it to Ssl or SaslSsl, or remove " +
                    $"{CaLocationVariable}.");
            }

            // Left unset otherwise, which makes librdkafka use the system trust store — right for
            // a broker with a publicly trusted certificate, and wrong only for a private CA.
            config.SslCaLocation = caLocation;
        }

        if (secured is not (SecurityProtocol.SaslPlaintext or SecurityProtocol.SaslSsl))
        {
            return;
        }

        // Required rather than defaulted: a client that starts without credentials fails every
        // connection at runtime and reports it as a broker problem, which is a much longer walk
        // back to the actual cause than a message at startup.
        config.SaslMechanism = ParseMechanism(Required(read, MechanismVariable, secured));
        config.SaslUsername = Required(read, UsernameVariable, secured);
        config.SaslPassword = Required(read, PasswordVariable, secured);
    }

    private static string? Value(Func<string, string?> read, string key)
    {
        var value = read(key);
        return string.IsNullOrWhiteSpace(value) ? null : value.Trim();
    }

    private static string Required(Func<string, string?> read, string key, SecurityProtocol protocol)
        => Value(read, key)
           ?? throw new InvalidOperationException(
               $"{ProtocolVariable} is '{protocol}', which authenticates over SASL, but {key} is " +
               "not set.");

    private static SecurityProtocol? ParseProtocol(string? raw) =>
        raw is null ? null : Parse<SecurityProtocol>(ProtocolVariable, raw);

    private static SaslMechanism ParseMechanism(string raw) =>
        Parse<SaslMechanism>(MechanismVariable, raw);

    /// <summary>
    /// Accepts the spelling every broker's documentation uses (<c>SASL_SSL</c>,
    /// <c>SCRAM-SHA-512</c>) as well as the enum's own, by dropping the separators before parsing.
    /// Rejecting what the docs tell people to write would be a puzzle rather than a safeguard.
    /// </summary>
    private static T Parse<T>(string key, string raw) where T : struct, Enum
    {
        var candidate = raw.Replace("_", string.Empty).Replace("-", string.Empty);
        if (Enum.TryParse<T>(candidate, ignoreCase: true, out var parsed) &&
            Enum.IsDefined(parsed))
        {
            return parsed;
        }

        throw new InvalidOperationException(
            $"{key} is '{raw}', which is not a known value. Accepted: " +
            $"{string.Join(", ", Enum.GetNames<T>())}.");
    }
}
