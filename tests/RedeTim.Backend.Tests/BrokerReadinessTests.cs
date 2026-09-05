using System.Reflection;
using Confluent.Kafka;
using RedeTim.Contracts;

namespace RedeTim.Backend.Tests;

public class BrokerReadinessTests
{
    private static Func<string, string?> Env(params (string Key, string Value)[] entries) =>
        key => entries.FirstOrDefault(e => e.Key == key).Value;

    private static Func<string, string?> SaslSslEnv() => Env(
        (KafkaSecurity.ProtocolVariable, "SaslSsl"),
        (KafkaSecurity.MechanismVariable, "ScramSha512"),
        (KafkaSecurity.UsernameVariable, "chat"),
        (KafkaSecurity.PasswordVariable, "hunter2"),
        (KafkaSecurity.CaLocationVariable, "/etc/redetim/kafka-ca/ca.crt"));

    public static TheoryData<string, Func<BackendOptions, Func<string, string?>, ClientConfig>>
        AllClients => new()
    {
        { "readiness admin client", (o, read) => BrokerReadiness.BuildConfig(o, read) },
        { "producer", (o, read) => ChatProducer.BuildConfig(o, read) },
        { "consumer", (o, read) => ChatConsumerService.BuildConfig(o, read) },
        { "presence producer", (o, read) => PresenceProducer.BuildConfig(o, read) },
        { "presence consumer", (o, read) => PresenceConsumerService.BuildConfig(o, read) },
    };

    // AllClients is hand-maintained, so it can only prove things about the clients somebody
    // remembered to list. This is the part that notices a *new* one: any static method in the
    // backend that hands out a ClientConfig is a Kafka client being configured, and it has to
    // be represented above, or the KafkaSecurity.ApplyTo invariant is enforced by nothing.
    [Fact]
    public void EveryClientConfigBuilderInTheBackendIsListedInAllClients()
    {
        var builders = typeof(ChatProducer).Assembly
            .GetTypes()
            .SelectMany(type => type.GetMethods(
                BindingFlags.Static | BindingFlags.Public | BindingFlags.NonPublic |
                BindingFlags.DeclaredOnly))
            .Where(method =>
                typeof(ClientConfig).IsAssignableFrom(method.ReturnType) &&
                method.GetParameters().Length == 2)
            .Select(method => $"{method.DeclaringType!.Name}.{method.Name}")
            .Distinct()
            .Order(StringComparer.Ordinal)
            .ToArray();

        // Listed explicitly so a new builder cannot appear without somebody deciding, here,
        // whether it also needs a row in AllClients above. KafkaJsonProducer is the shared
        // configuration the two producers delegate to, not a sixth client.
        string[] expected =
        [
            "BrokerReadiness.BuildConfig",
            "ChatConsumerService.BuildConfig",
            "ChatProducer.BuildConfig",
            "KafkaJsonProducer.BuildConfig",
            "PresenceConsumerService.BuildConfig",
            "PresenceProducer.BuildConfig",
        ];

        Assert.Equal(expected, builders);
    }

    [Theory]
    [MemberData(nameof(AllClients))]
    public void EveryBackendClientCarriesTheBrokerSecuritySettings(
        string name, Func<BackendOptions, Func<string, string?>, ClientConfig> build)
    {
        var config = build(TestOptions.Create(), SaslSslEnv());

        Assert.True(
            config.SecurityProtocol == SecurityProtocol.SaslSsl,
            $"The {name} does not apply REDPANDA_SECURITY_PROTOCOL.");
        Assert.True(
            config.SaslMechanism == SaslMechanism.ScramSha512,
            $"The {name} does not apply REDPANDA_SASL_MECHANISM.");
        Assert.True(config.SaslUsername == "chat", $"The {name} does not apply the SASL username.");
        Assert.True(config.SaslPassword == "hunter2", $"The {name} does not apply the SASL password.");
        Assert.True(
            config.SslCaLocation == "/etc/redetim/kafka-ca/ca.crt",
            $"The {name} does not apply REDPANDA_SSL_CA_LOCATION.");
    }

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
