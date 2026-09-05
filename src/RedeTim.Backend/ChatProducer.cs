using Confluent.Kafka;
using RedeTim.Contracts;

namespace RedeTim.Backend;

public sealed class ChatProducer : IDisposable
{
    private readonly KafkaJsonProducer _producer;
    private readonly BackendOptions _options;
    private readonly ChatMetrics _metrics;

    public ChatProducer(BackendOptions options, ChatMetrics metrics, ILogger<ChatProducer> logger)
    {
        _options = options;
        _metrics = metrics;
        _producer = new KafkaJsonProducer(BuildConfig(options), metrics, logger, "Kafka producer");
    }

    internal static ProducerConfig BuildConfig(BackendOptions options) =>
        BuildConfig(options, Environment.GetEnvironmentVariable);

    internal static ProducerConfig BuildConfig(BackendOptions options, Func<string, string?> read) =>
        KafkaJsonProducer.BuildConfig(options, read);

    // The key is the room, so every message of a room lands on one partition and its offsets
    // stay strictly increasing. See docs/kafka.md and CLAUDE.md's invariants.
    public async Task ProduceAsync(ChatMessage message, CancellationToken cancellationToken)
    {
        var record = new Message<string, string>
        {
            Key = message.Room,
            Value = WireFormat.Serialize(message),
        };

        await _producer.ProduceAsync(_options.Topic, record, cancellationToken);

        _metrics.RecordMessageSent();
    }

    public void Dispose() => _producer.Dispose();
}
