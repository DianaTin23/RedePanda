using Confluent.Kafka;
using RedeTim.Contracts;

namespace RedeTim.Backend;

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
                _metrics.RecordKafkaError();
                if (_errorLog.ShouldLog(out var suppressed))
                {
                    _logger.LogWarning(
                        "Kafka producer error: {Reason}{Suppressed}",
                        error.Reason,
                        LogThrottle.Describe(suppressed));
                }
            })
            .SetLogHandler((_, message) =>
                _logger.Log(
                    KafkaLogging.ToLogLevel(message.Level),
                    "librdkafka {Facility}: {Message}",
                    message.Facility,
                    message.Message))
            .Build();
    }

    internal static ProducerConfig BuildConfig(BackendOptions options) =>
        BuildConfig(options, Environment.GetEnvironmentVariable);

    internal static ProducerConfig BuildConfig(BackendOptions options, Func<string, string?> read)
    {
        var config = new ProducerConfig
        {
            BootstrapServers = options.BootstrapServers,
            EnableIdempotence = true,
            Acks = Acks.All,
            MessageTimeoutMs = options.ProduceTimeoutMs,
            RequestTimeoutMs = options.ProduceTimeoutMs / 2,
        };

        KafkaSecurity.ApplyTo(config, read);
        return config;
    }

    internal static int StatusCodeFor(Error error) => error.Code switch
    {
        ErrorCode.Local_MsgTimedOut or ErrorCode.Local_TimedOut =>
            StatusCodes.Status504GatewayTimeout,
        _ => StatusCodes.Status502BadGateway,
    };

    public async Task ProduceAsync(ChatMessage message, CancellationToken cancellationToken)
    {
        var record = new Message<string, string>
        {
            Key = message.Room,
            Value = ChatMessageSerializer.Serialize(message),
        };

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
        _producer.Flush(TimeSpan.FromSeconds(5));
        _producer.Dispose();
    }
}
