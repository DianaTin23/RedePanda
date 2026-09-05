using Confluent.Kafka;
using RedeTim.Contracts;

namespace RedeTim.Backend;

public sealed class PresenceConsumerService : KafkaConsumerService
{
    private readonly PresenceStore _store;
    private readonly BrokerReadiness _readiness;

    public PresenceConsumerService(
        BackendOptions options,
        PresenceStore store,
        BrokerReadiness readiness,
        ILogger<PresenceConsumerService> logger)
        : base(
            "presence consumer", options.PresenceTopic, options.PresenceConsumerGroupId,
            BuildConfig(options), logger)
    {
        _store = store;
        _readiness = readiness;
    }

    internal static ConsumerConfig BuildConfig(BackendOptions options) =>
        BuildConfig(options, Environment.GetEnvironmentVariable);

    internal static ConsumerConfig BuildConfig(BackendOptions options, Func<string, string?> read)
    {
        var config = new ConsumerConfig
        {
            BootstrapServers = options.BootstrapServers,
            GroupId = options.PresenceConsumerGroupId,
            AutoOffsetReset = AutoOffsetReset.Earliest,
            EnablePartitionEof = true,
            EnableAutoCommit = false,
        };

        KafkaSecurity.ApplyTo(config, read);
        return config;
    }

    // No replay window: the topic is log-compacted, so reading it whole *is* the current state.
    // The base class already starts at Offset.Beginning.

    protected override void Handle(ConsumeResult<string, string> result)
    {
        var message = result.Message;

        if (message.Value is null)
        {
            if (PresenceKey.TryDecode(message.Key, out var room, out var nickname))
            {
                _store.Remove(room, nickname);
            }

            return;
        }

        var record = WireFormat.Deserialize<PresenceRecord>(message.Value);
        if (record is null)
        {
            Logger.LogWarning("Skipped an unreadable presence record");
            return;
        }

        _store.Apply(record.Room, record.Nickname, record.RenewedAt);
    }

    protected override void OnReplayed()
    {
        _readiness.MarkPresenceLoaded();
        Logger.LogInformation(
            "Replayed presence topic {Topic} to the end; the pod can enforce the nickname lock",
            Topic);
    }

    // Deliberately the opposite of the chat consumer: presence is a soft UX gate, not the
    // service. Losing it costs the nickname lock, not the chat, so readiness degrades open
    // instead of restarting the pod. See docs/kafka.md and README section 14.
    protected override void OnFatal(Exception exception)
    {
        Logger.LogCritical(
            exception,
            "The presence consumer stopped fatally; this pod can no longer enforce the " +
            "nickname lock, but chat is unaffected. Degrading readiness open instead of " +
            "restarting the pod");
        _readiness.MarkPresenceLoaded();
    }
}
