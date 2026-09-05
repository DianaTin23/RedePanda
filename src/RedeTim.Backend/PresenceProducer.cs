using Confluent.Kafka;
using RedeTim.Contracts;

namespace RedeTim.Backend;

internal interface IPresenceProducer
{
    Task RenewAsync(string room, string nickname, CancellationToken cancellationToken);

    Task ReleaseAsync(string room, string nickname, CancellationToken cancellationToken);
}

public sealed class PresenceProducer : IPresenceProducer, IDisposable
{
    private readonly KafkaJsonProducer _producer;
    private readonly BackendOptions _options;

    public PresenceProducer(
        BackendOptions options, ChatMetrics metrics, ILogger<PresenceProducer> logger)
    {
        _options = options;
        _producer = new KafkaJsonProducer(
            BuildConfig(options), metrics, logger, "Presence producer");
    }

    internal static ProducerConfig BuildConfig(BackendOptions options) =>
        BuildConfig(options, Environment.GetEnvironmentVariable);

    internal static ProducerConfig BuildConfig(BackendOptions options, Func<string, string?> read) =>
        KafkaJsonProducer.BuildConfig(options, read);

    // The key is (room, nickname), not the room: the topic is log-compacted and holds the
    // current state per reservation. See docs/kafka.md#presence-topic.
    public Task RenewAsync(string room, string nickname, CancellationToken cancellationToken) =>
        Produce(
            room,
            nickname,
            WireFormat.Serialize(
                new PresenceRecord(room, nickname, _options.PodName, DateTimeOffset.UtcNow)),
            cancellationToken);

    // A null value is the tombstone compaction collapses the key to.
    public Task ReleaseAsync(string room, string nickname, CancellationToken cancellationToken) =>
        Produce(room, nickname, null!, cancellationToken);

    private Task Produce(
        string room, string nickname, string value, CancellationToken cancellationToken)
    {
        var record = new Message<string, string>
        {
            Key = PresenceKey.Encode(room, nickname),
            Value = value,
        };

        return _producer.ProduceAsync(_options.PresenceTopic, record, cancellationToken);
    }

    public void Dispose() => _producer.Dispose();
}
