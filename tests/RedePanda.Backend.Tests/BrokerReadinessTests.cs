using Confluent.Kafka;
using RedePanda.Contracts;

namespace RedePanda.Backend.Tests;

/// <summary>
/// The backend speaks to the broker through three separate clients, and every one of them has to
/// carry the same security settings. <see cref="BrokerReadiness"/> shipped without them.
/// <para>
/// The consequence was not a loud failure. The producer and the consumer connected normally, while
/// the readiness probe's admin client spoke plaintext to a secured listener and threw on every
/// <c>GetMetadata</c>; the exception became <c>ready = false</c> and a log line nobody saw, so
/// <c>/health/ready</c> answered 503 for the whole life of the pod. With
/// <c>maxUnavailable: 0</c> in the chart, the rollout then never completed — and it read exactly
/// like a broker outage.
/// </para>
/// <para>
/// These tests pin the invariant that made that possible: *every* client config the backend builds
/// goes through <see cref="KafkaSecurity"/>. Adding a fourth client without one of these is meant
/// to be uncomfortable.
/// </para>
/// </summary>
public class BrokerReadinessTests
{
    /// <summary>
    /// A fake environment. The real settings are process-global and these tests run in parallel
    /// with everything else, so nothing here touches <see cref="Environment"/>.
    /// <para>
    /// First match wins, so an entry meant to override another has to be listed before it — the
    /// same gotcha noted in <c>KafkaSecurityTests</c>.
    /// </para>
    /// </summary>
    private static Func<string, string?> Env(params (string Key, string Value)[] entries) =>
        key => entries.FirstOrDefault(e => e.Key == key).Value;

    private static Func<string, string?> SaslSslEnv() => Env(
        (KafkaSecurity.ProtocolVariable, "SaslSsl"),
        (KafkaSecurity.MechanismVariable, "ScramSha512"),
        (KafkaSecurity.UsernameVariable, "chat"),
        (KafkaSecurity.PasswordVariable, "hunter2"),
        (KafkaSecurity.CaLocationVariable, "/etc/redepanda/kafka-ca/ca.crt"));

    /// <summary>
    /// Every client config the backend builds, by the name that identifies it in a failure message.
    /// The producer and consumer are here to keep the readiness client honest: the bug was not that
    /// one client was wrong, it was that one client was <em>different</em>.
    /// </summary>
    public static TheoryData<string, Func<BackendOptions, Func<string, string?>, ClientConfig>>
        AllClients => new()
    {
        { "readiness admin client", (o, read) => BrokerReadiness.BuildConfig(o, read) },
        { "producer", (o, read) => ChatProducer.BuildConfig(o, read) },
        { "consumer", (o, read) => ChatConsumerService.BuildConfig(o, read) },
    };

    /// <summary>The test that would have caught BR-1.</summary>
    [Theory]
    [MemberData(nameof(AllClients))]
    public void EveryBackendClientCarriesTheBrokerSecuritySettings(
        string name, Func<BackendOptions, Func<string, string?>, ClientConfig> build)
    {
        var config = build(TestOptions.Create(), SaslSslEnv());

        // Assert.True rather than Assert.Equal so the failure names the client. Which one differs
        // is the entire diagnosis here; "expected SaslSsl, got null" on its own is not.
        Assert.True(
            config.SecurityProtocol == SecurityProtocol.SaslSsl,
            $"The {name} does not apply REDPANDA_SECURITY_PROTOCOL.");
        Assert.True(
            config.SaslMechanism == SaslMechanism.ScramSha512,
            $"The {name} does not apply REDPANDA_SASL_MECHANISM.");
        Assert.True(config.SaslUsername == "chat", $"The {name} does not apply the SASL username.");
        Assert.True(config.SaslPassword == "hunter2", $"The {name} does not apply the SASL password.");
        Assert.True(
            config.SslCaLocation == "/etc/redepanda/kafka-ca/ca.crt",
            $"The {name} does not apply REDPANDA_SSL_CA_LOCATION.");
    }

    /// <summary>
    /// The other half of the invariant: a missing credential has to stop the pod at startup rather
    /// than surface later as an unexplained connection failure. Every client must refuse alike.
    /// </summary>
    [Theory]
    [MemberData(nameof(AllClients))]
    public void EveryBackendClientRefusesSaslWithoutCredentials(
        string name, Func<BackendOptions, Func<string, string?>, ClientConfig> build)
    {
        var incomplete = Env((KafkaSecurity.ProtocolVariable, "SaslSsl"));

        var failure = Assert.Throws<InvalidOperationException>(
            () => build(TestOptions.Create(), incomplete));

        Assert.True(
            failure.Message.Contains(KafkaSecurity.MechanismVariable, StringComparison.Ordinal),
            $"The {name} refused SASL without naming the missing setting: {failure.Message}");
    }

    /// <summary>
    /// The bundled broker speaks plaintext, and an unconfigured client's config has to stay exactly
    /// as it was before any of this existed — otherwise the demo path pays for the secured one.
    /// </summary>
    [Theory]
    [MemberData(nameof(AllClients))]
    public void AnUnconfiguredClientIsLeftAlone(
        string name, Func<BackendOptions, Func<string, string?>, ClientConfig> build)
    {
        var config = build(TestOptions.Create(), Env());

        Assert.True(config.SecurityProtocol is null, $"The {name} set a SecurityProtocol unasked.");
        Assert.True(config.SaslMechanism is null, $"The {name} set a SaslMechanism unasked.");
        Assert.True(config.SaslUsername is null, $"The {name} set a SASL username unasked.");
        Assert.True(config.SaslPassword is null, $"The {name} set a SASL password unasked.");
        Assert.True(config.SslCaLocation is null, $"The {name} set an SslCaLocation unasked.");
    }

    /// <summary>
    /// The bootstrap list still has to survive the security mapping — a config that authenticates
    /// perfectly against no broker at all is not an improvement.
    /// </summary>
    [Theory]
    [MemberData(nameof(AllClients))]
    public void EveryBackendClientKeepsItsBootstrapServers(
        string name, Func<BackendOptions, Func<string, string?>, ClientConfig> build)
    {
        var options = TestOptions.Create() with { BootstrapServers = "broker.example:9093" };

        Assert.True(
            build(options, SaslSslEnv()).BootstrapServers == "broker.example:9093",
            $"The {name} lost its bootstrap list to the security mapping.");
    }
}
