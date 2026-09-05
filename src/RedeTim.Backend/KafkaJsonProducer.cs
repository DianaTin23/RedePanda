using Confluent.Kafka;
using RedeTim.Contracts;

namespace RedeTim.Backend;

// The half of a producer that does not depend on what is being published: the librdkafka handle,
// the throttled error log, and the flush-then-dispose. What the two producers genuinely differ
// in -- the record key, and whether a null value is meaningful -- stays with them.
// See docs/kafka.md#producer.
internal sealed class KafkaJsonProducer : IDisposable
{
    private readonly IProducer<string, string> _producer;
    private readonly ChatMetrics _metrics;
    private readonly LogThrottle _errorLog = new(KafkaLogging.ErrorInterval);

    public KafkaJsonProducer(
        ProducerConfig config, ChatMetrics metrics, ILogger logger, string role)
    {
        _metrics = metrics;

        _producer = new ProducerBuilder<string, string>(config)
            .SetErrorHandler((_, error) =>
            {
                _metrics.RecordKafkaError();
                if (_errorLog.ShouldLog(out var suppressed))
                {
                    logger.LogWarning(
                        "{Role} error: {Reason}{Suppressed}",
                        role,
                        error.Reason,
                        LogThrottle.Describe(suppressed));
                }
            })
            .SetLogHandler((_, message) =>
                logger.Log(
                    KafkaLogging.ToLogLevel(message.Level),
                    "librdkafka {Facility}: {Message}",
                    message.Facility,
                    message.Message))
            .Build();
    }

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

    public async Task ProduceAsync(
        string topic, Message<string, string> message, CancellationToken cancellationToken)
    {
        try
        {
            await _producer.ProduceAsync(topic, message, cancellationToken);
        }
        catch (ProduceException<string, string>)
        {
            _metrics.RecordKafkaError();
            throw;
        }
    }

    public void Dispose()
    {
        _producer.Flush(TimeSpan.FromSeconds(5));
        _producer.Dispose();
    }
}
