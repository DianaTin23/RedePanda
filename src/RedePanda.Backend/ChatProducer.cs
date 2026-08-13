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
    private readonly LogThrottle _errorLog = new(KafkaLogging.ErrorInterval);

    public ChatProducer(BackendOptions options, ChatMetrics metrics, ILogger<ChatProducer> logger)
    {
        _options = options;
        _metrics = metrics;
        _logger = logger;

        _producer = new ProducerBuilder<string, string>(BuildConfig(options))
            .SetErrorHandler((_, error) =>
            {
                // The metric counts every error; the log reports at most one per interval. See
                // LogThrottle for why those two are treated differently.
                _metrics.RecordKafkaError();
                if (_errorLog.ShouldLog(out var suppressed))
                {
                    _logger.LogWarning(
                        "Kafka producer error: {Reason}{Suppressed}",
                        error.Reason,
                        LogThrottle.Describe(suppressed));
                }
            })

            // librdkafka writes its own diagnostics to stderr unless they are claimed here, which
            // put them outside LOG_LEVEL and outside the JSON the platform collects.
            .SetLogHandler((_, message) =>
                _logger.Log(
                    KafkaLogging.ToLogLevel(message.Level),
                    "librdkafka {Facility}: {Message}",
                    message.Facility,
                    message.Message))
            .Build();
    }

    /// <summary>
    /// The producer's configuration, separate from the constructor so it can be asserted on
    /// without a broker in the picture.
    /// </summary>
    internal static ProducerConfig BuildConfig(BackendOptions options) =>
        BuildConfig(options, Environment.GetEnvironmentVariable);

    /// <param name="read">
    /// Where a security setting comes from, injected for the same reason
    /// <see cref="KafkaSecurity.ApplyTo(ClientConfig, Func{string, string?})"/> injects it.
    /// </param>
    internal static ProducerConfig BuildConfig(BackendOptions options, Func<string, string?> read)
    {
        var config = new ProducerConfig
        {
            BootstrapServers = options.BootstrapServers,

            // Idempotence is what makes a retry safe *and ordered*. Without it librdkafka may
            // deliver a retried record after one produced later, and the frontend's resume filter
            // drops anything whose offset is not greater than the last it saw -- so a reordered
            // retry was not a duplicate to be tolerated, it was a message lost for good, silently.
            //
            // It implies acks=all, bounds in-flight requests and enables retries, so Acks is set
            // to match rather than left at Leader: librdkafka rejects the combination outright,
            // and an explicit value is easier to read than an implied one.
            EnableIdempotence = true,
            Acks = Acks.All,

            // Without this, librdkafka's five-minute default applies and the POST behind it holds
            // the browser's composer open for the whole of it. See BackendOptions.ProduceTimeoutMs.
            MessageTimeoutMs = options.ProduceTimeoutMs,

            // Strictly below the message timeout: at or above it, one in-flight request would
            // spend the entire budget and the retry the message timeout allows would never happen.
            RequestTimeoutMs = options.ProduceTimeoutMs / 2,
        };

        // A no-op against the plaintext broker in the chart; the whole of TLS and SASL against
        // anything else. See RedePanda.Contracts.KafkaSecurity.
        KafkaSecurity.ApplyTo(config, read);
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
