using Confluent.Kafka;
using RedePanda.Contracts;

namespace RedePanda.Backend;

/// <summary>Publishes accepted messages to the Kafka topic.</summary>
public sealed class ChatProducer : IDisposable
{
    private readonly IProducer<string, string> _producer;
    private readonly BackendOptions _options;
    private readonly ChatMetrics _metrics;
    private readonly ILogger<ChatProducer> _logger;

    public ChatProducer(BackendOptions options, ChatMetrics metrics, ILogger<ChatProducer> logger)
    {
        _options = options;
        _metrics = metrics;
        _logger = logger;

        var config = new ProducerConfig
        {
            BootstrapServers = options.BootstrapServers,
            Acks = Acks.Leader,
        };

        _producer = new ProducerBuilder<string, string>(config)
            .SetErrorHandler((_, error) =>
            {
                _metrics.RecordKafkaError();
                _logger.LogWarning("Kafka producer error: {Reason}", error.Reason);
            })
            .Build();
    }

    /// <summary>Produces one message, keyed by room so per-room ordering survives repartitioning.</summary>
    public async Task ProduceAsync(ChatMessage message, CancellationToken cancellationToken)
    {
        var record = new Message<string, string>
        {
            Key = message.Room,
            Value = ChatMessageSerializer.Serialize(message),
        };

        await _producer.ProduceAsync(_options.Topic, record, cancellationToken);
        _metrics.RecordMessageSent();
    }

    public void Dispose()
    {
        // Flush before the process exits so a message accepted with 202 is not lost on SIGTERM.
        _producer.Flush(TimeSpan.FromSeconds(5));
        _producer.Dispose();
    }
}
