using Confluent.Kafka;
using RedeTim.Contracts;

namespace RedeTim.Backend.Tests;

public class KafkaSecurityTests
{
    private static Func<string, string?> Env(params (string Key, string Value)[] entries) =>
        key => entries.FirstOrDefault(e => e.Key == key).Value;

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
            ("REDPANDA_SSL_CA_LOCATION", "/etc/redetim/ca/ca.crt")));

        Assert.Equal(SecurityProtocol.Ssl, config.SecurityProtocol);
        Assert.Equal("/etc/redetim/ca/ca.crt", config.SslCaLocation);
        Assert.Null(config.SaslUsername);
    }

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

    [Fact]
    public void ACaBundleWithoutTlsIsRefused()
    {
        var failure = Assert.Throws<InvalidOperationException>(() =>
            KafkaSecurity.ApplyTo(new ClientConfig(), Env(
                ("REDPANDA_SECURITY_PROTOCOL", "SaslPlaintext"),
                ("REDPANDA_SASL_MECHANISM", "Plain"),
                ("REDPANDA_SASL_USERNAME", "chat"),
                ("REDPANDA_SASL_PASSWORD", "s3cret"),
                ("REDPANDA_SSL_CA_LOCATION", "/etc/redetim/ca/ca.crt"))));

        Assert.Contains("REDPANDA_SSL_CA_LOCATION", failure.Message, StringComparison.Ordinal);
    }

    [Theory]
    [InlineData("REDPANDA_SECURITY_PROTOCOL", "Tls")]
    [InlineData("REDPANDA_SASL_MECHANISM", "Scram")]
    public void AnUnknownValueIsRejectedWithTheAcceptedOnes(string key, string value)
    {
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
