using Confluent.Kafka;
using RedePanda.Contracts;

namespace RedePanda.Backend.Tests;

/// <summary>
/// What makes "the broker is swappable without a code change" true rather than merely plausible:
/// any broker that is not the unauthenticated one in this chart needs TLS, credentials, or both.
/// <para>
/// Every case reads through an explicit lookup rather than the process environment — these tests
/// run in parallel with everything else, and environment variables are process-global.
/// </para>
/// </summary>
public class KafkaSecurityTests
{
    private static Func<string, string?> Env(params (string Key, string Value)[] entries) =>
        key => entries.FirstOrDefault(e => e.Key == key).Value;

    /// <summary>
    /// Nothing configured must leave the config byte-for-byte what it was before any of this
    /// existed: the bundled single-broker demo speaks plaintext and has to keep working untouched.
    /// </summary>
    [Fact]
    public void AnUnconfiguredClientIsLeftAlone()
    {
        var config = new ClientConfig();

        KafkaSecurity.ApplyTo(config, Env());

        Assert.Null(config.SecurityProtocol);
        Assert.Null(config.SaslMechanism);
        Assert.Null(config.SaslUsername);
    }

    [Fact]
    public void AnExplicitPlaintextIsAlsoLeftAlone()
    {
        var config = new ClientConfig();

        KafkaSecurity.ApplyTo(config, Env(("REDPANDA_SECURITY_PROTOCOL", "Plaintext")));

        Assert.Null(config.SecurityProtocol);
    }

    [Fact]
    public void SaslOverTlsCarriesProtocolMechanismAndCredentials()
    {
        var config = new ClientConfig();

        KafkaSecurity.ApplyTo(config, Env(
            ("REDPANDA_SECURITY_PROTOCOL", "SaslSsl"),
            ("REDPANDA_SASL_MECHANISM", "ScramSha512"),
            ("REDPANDA_SASL_USERNAME", "chat"),
            ("REDPANDA_SASL_PASSWORD", "s3cret")));

        Assert.Equal(SecurityProtocol.SaslSsl, config.SecurityProtocol);
        Assert.Equal(SaslMechanism.ScramSha512, config.SaslMechanism);
        Assert.Equal("chat", config.SaslUsername);
        Assert.Equal("s3cret", config.SaslPassword);
    }

    /// <summary>
    /// Everyone who has configured a Kafka client has typed <c>SASL_SSL</c> and
    /// <c>SCRAM-SHA-512</c>, because that is what every broker's documentation writes. Rejecting
    /// those spellings would be a puzzle, not a safeguard.
    /// </summary>
    [Fact]
    public void TheSpellingsFromBrokerDocumentationAreAccepted()
    {
        var config = new ClientConfig();

        KafkaSecurity.ApplyTo(config, Env(
            ("REDPANDA_SECURITY_PROTOCOL", "SASL_SSL"),
            ("REDPANDA_SASL_MECHANISM", "SCRAM-SHA-512"),
            ("REDPANDA_SASL_USERNAME", "chat"),
            ("REDPANDA_SASL_PASSWORD", "s3cret")));

        Assert.Equal(SecurityProtocol.SaslSsl, config.SecurityProtocol);
        Assert.Equal(SaslMechanism.ScramSha512, config.SaslMechanism);
    }

    [Fact]
    public void TlsWithoutSaslNeedsNoCredentials()
    {
        var config = new ClientConfig();

        KafkaSecurity.ApplyTo(config, Env(
            ("REDPANDA_SECURITY_PROTOCOL", "Ssl"),
            ("REDPANDA_SSL_CA_LOCATION", "/etc/redepanda/ca/ca.crt")));

        Assert.Equal(SecurityProtocol.Ssl, config.SecurityProtocol);
        Assert.Equal("/etc/redepanda/ca/ca.crt", config.SslCaLocation);
        Assert.Null(config.SaslUsername);
    }

    /// <summary>
    /// Each of these is a pod that would otherwise start, fail every connection at runtime, and
    /// report it as a broker problem. The message has to name the variable that is missing.
    /// </summary>
    [Theory]
    [InlineData("REDPANDA_SASL_MECHANISM")]
    [InlineData("REDPANDA_SASL_USERNAME")]
    [InlineData("REDPANDA_SASL_PASSWORD")]
    public void SaslWithAMissingSettingFailsAtStartupAndSaysWhich(string missing)
    {
        var complete = new[]
        {
            ("REDPANDA_SECURITY_PROTOCOL", "SaslSsl"),
            ("REDPANDA_SASL_MECHANISM", "Plain"),
            ("REDPANDA_SASL_USERNAME", "chat"),
            ("REDPANDA_SASL_PASSWORD", "s3cret"),
        };

        var failure = Assert.Throws<InvalidOperationException>(() =>
            KafkaSecurity.ApplyTo(
                new ClientConfig(), Env(complete.Where(e => e.Item1 != missing).ToArray())));

        Assert.Contains(missing, failure.Message, StringComparison.Ordinal);
    }

    /// <summary>
    /// A CA bundle alongside a protocol that never opens a TLS connection means someone believes
    /// the traffic is encrypted while it is not — the one misconfiguration here worth refusing
    /// outright rather than ignoring.
    /// </summary>
    [Fact]
    public void ACaBundleWithoutTlsIsRefused()
    {
        var failure = Assert.Throws<InvalidOperationException>(() =>
            KafkaSecurity.ApplyTo(new ClientConfig(), Env(
                ("REDPANDA_SECURITY_PROTOCOL", "SaslPlaintext"),
                ("REDPANDA_SASL_MECHANISM", "Plain"),
                ("REDPANDA_SASL_USERNAME", "chat"),
                ("REDPANDA_SASL_PASSWORD", "s3cret"),
                ("REDPANDA_SSL_CA_LOCATION", "/etc/redepanda/ca/ca.crt"))));

        Assert.Contains("REDPANDA_SSL_CA_LOCATION", failure.Message, StringComparison.Ordinal);
    }

    [Theory]
    [InlineData("REDPANDA_SECURITY_PROTOCOL", "Tls")]
    [InlineData("REDPANDA_SASL_MECHANISM", "Scram")]
    public void AnUnknownValueIsRejectedWithTheAcceptedOnes(string key, string value)
    {
        // The overridden entry comes first: Env resolves a key to its first match, so putting it
        // last would leave the valid value in place and test nothing.
        var failure = Assert.Throws<InvalidOperationException>(() =>
            KafkaSecurity.ApplyTo(new ClientConfig(), Env(
                (key, value),
                ("REDPANDA_SECURITY_PROTOCOL", "SaslSsl"),
                ("REDPANDA_SASL_MECHANISM", "Plain"),
                ("REDPANDA_SASL_USERNAME", "chat"),
                ("REDPANDA_SASL_PASSWORD", "s3cret"))));

        Assert.Contains(key, failure.Message, StringComparison.Ordinal);
        Assert.Contains(value, failure.Message, StringComparison.Ordinal);
    }

    /// <summary>The password must not travel into a log, an exception message included.</summary>
    [Fact]
    public void AFailureNeverRepeatsThePassword()
    {
        var failure = Assert.Throws<InvalidOperationException>(() =>
            KafkaSecurity.ApplyTo(new ClientConfig(), Env(
                ("REDPANDA_SECURITY_PROTOCOL", "SaslSsl"),
                ("REDPANDA_SASL_MECHANISM", "Nonsense"),
                ("REDPANDA_SASL_USERNAME", "chat"),
                ("REDPANDA_SASL_PASSWORD", "s3cret"))));

        Assert.DoesNotContain("s3cret", failure.Message, StringComparison.Ordinal);
    }
}
