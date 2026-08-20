using Confluent.Kafka;
using RedeTim.Contracts;

namespace RedeTim.Backend;

/// <summary>Renews or releases one (room, nickname) reservation on the presence topic.</summary>
internal interface IPresenceProducer
{
    /// <summary>Marks the reservation as still alive, resetting its TTL clock on every pod.</summary>
    Task RenewAsync(string room, string nickname, CancellationToken cancellationToken);

    /// <summary>Frees the reservation immediately via a Kafka tombstone, instead of waiting out the TTL.</summary>
    Task ReleaseAsync(string room, string nickname, CancellationToken cancellationToken);
}

/// <summary>Publishes presence heartbeats and tombstones to the presence topic.</summary>
public sealed class PresenceProducer : IPresenceProducer, IDisposable
{
    private readonly IProducer<string, string> _producer;
    private readonly BackendOptions _options;
    private readonly ChatMetrics _metrics;
    private readonly ILogger<PresenceProducer> _logger;
    private readonly LogThrottle _errorLog = new(KafkaLogging.ErrorInterval);

    public PresenceProducer(BackendOptions options, ChatMetrics metrics, ILogger<PresenceProducer> logger)
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
                        "Presence producer error: {Reason}{Suppressed}",
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

    /// <summary>The producer's configuration, separate so it can be asserted on without a broker.</summary>
    internal static ProducerConfig BuildConfig(BackendOptions options) =>
        BuildConfig(options, Environment.GetEnvironmentVariable);

    /// <summary>The producer's configuration, reading security settings through <paramref name="read"/>.</summary>
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

    /// <summary>How a failed produce is reported to the browser.</summary>
    internal static int StatusCodeFor(Error error) => error.Code switch
    {
        ErrorCode.Local_MsgTimedOut or ErrorCode.Local_TimedOut =>
            StatusCodes.Status504GatewayTimeout,
        _ => StatusCodes.Status502BadGateway,
    };

    public async Task RenewAsync(string room, string nickname, CancellationToken cancellationToken)
    {
        var record = new PresenceRecord(room, nickname, _options.PodName, DateTimeOffset.UtcNow);
        var message = new Message<string, string>
        {
            Key = PresenceKey.Encode(room, nickname),
            Value = PresenceEventSerializer.Serialize(record),
        };

        await ProduceAsync(message, cancellationToken);
    }

    public async Task ReleaseAsync(string room, string nickname, CancellationToken cancellationToken)
    {
        var message = new Message<string, string>
        {
            Key = PresenceKey.Encode(room, nickname),
            Value = null!,
        };

        await ProduceAsync(message, cancellationToken);
    }

    private async Task ProduceAsync(Message<string, string> message, CancellationToken cancellationToken)
    {
        try
        {
            await _producer.ProduceAsync(_options.PresenceTopic, message, cancellationToken);
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
