using Confluent.Kafka;

namespace RedePanda.Contracts;

/// <summary>How every client in this repository authenticates and encrypts its connection to the broker.</summary>
public static class KafkaSecurity
{
    public const string ProtocolVariable = "REDPANDA_SECURITY_PROTOCOL";
    public const string MechanismVariable = "REDPANDA_SASL_MECHANISM";
    public const string UsernameVariable = "REDPANDA_SASL_USERNAME";
    public const string PasswordVariable = "REDPANDA_SASL_PASSWORD";
    public const string CaLocationVariable = "REDPANDA_SSL_CA_LOCATION";

    /// <summary>Reads the settings from the process environment.</summary>
    public static void ApplyTo(ClientConfig config) =>
        ApplyTo(config, Environment.GetEnvironmentVariable);

    /// <summary>Applies the settings read through <paramref name="read"/> to <paramref name="config"/>.</summary>
    public static void ApplyTo(ClientConfig config, Func<string, string?> read)
    {
        var caLocation = Value(read, CaLocationVariable);
        var protocol = ParseProtocol(Value(read, ProtocolVariable));

        if (protocol is null or SecurityProtocol.Plaintext)
        {
            if (caLocation is not null)
            {
                throw new InvalidOperationException(
                    $"{CaLocationVariable} is set, but {ProtocolVariable} does not use TLS. " +
                    $"Set {ProtocolVariable} to Ssl or SaslSsl, or remove {CaLocationVariable}.");
            }

            return;
        }

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

            config.SslCaLocation = caLocation;
        }

        if (secured is not (SecurityProtocol.SaslPlaintext or SecurityProtocol.SaslSsl))
        {
            return;
        }

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
