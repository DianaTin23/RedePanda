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

        _producer = new ProducerBuilder<string, string>(BuildConfig(options))
            .SetErrorHandler((_, error) =>
            {
                _metrics.RecordKafkaError();
                _logger.LogWarning("Kafka producer error: {Reason}", error.Reason);
            })
            .Build();
    }

    /// <summary>
    /// The producer's configuration, separate from the constructor so it can be asserted on
    /// without a broker in the picture.
    /// </summary>
    internal static ProducerConfig BuildConfig(BackendOptions options)
    {
        var config = new ProducerConfig
        {
            BootstrapServers = options.BootstrapServers,
            Acks = Acks.Leader,

            // Without this, librdkafka's five-minute default applies and the POST behind it holds
            // the browser's composer open for the whole of it. See BackendOptions.ProduceTimeoutMs.
            MessageTimeoutMs = options.ProduceTimeoutMs,

            // Strictly below the message timeout: at or above it, one in-flight request would
            // spend the entire budget and the retry the message timeout allows would never happen.
            RequestTimeoutMs = options.ProduceTimeoutMs / 2,
        };

        // A no-op against the plaintext broker in the chart; the whole of TLS and SASL against
        // anything else. See RedePanda.Contracts.KafkaSecurity.
        KafkaSecurity.ApplyTo(config);
        return config;
    }

    /// <summary>
    /// How a failed produce is reported to the browser. A timeout means the request was never
    /// answered either way, which is a gateway *timeout*; anything the broker actively refused is
    /// a bad gateway.
    /// </summary>
    internal static int StatusCodeFor(Error error) => error.Code switch
    {
        ErrorCode.Local_MsgTimedOut or ErrorCode.Local_TimedOut =>
            StatusCodes.Status504GatewayTimeout,
        _ => StatusCodes.Status502BadGateway,
    };

    /// <summary>Produces one message, keyed by room so per-room ordering survives repartitioning.</summary>
    public async Task ProduceAsync(ChatMessage message, CancellationToken cancellationToken)
    {
        var record = new Message<string, string>
        {
            Key = message.Room,
            Value = ChatMessageSerializer.Serialize(message),
        };

        // Delivery failures never reach the error handler above — librdkafka's error_cb only
        // reports client-level events — so the counter has to be fed from here.
        try
        {
            await _producer.ProduceAsync(_options.Topic, record, cancellationToken);
        }
        catch (ProduceException<string, string>)
        {
            _metrics.RecordKafkaError();
            throw;
        }

        _metrics.RecordMessageSent();
    }

    public void Dispose()
    {
        // Flush before the process exits so a message accepted with 202 is not lost on SIGTERM.
        _producer.Flush(TimeSpan.FromSeconds(5));
        _producer.Dispose();
    }
}
