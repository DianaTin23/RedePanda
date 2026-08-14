using Confluent.Kafka;
using RedePanda.Contracts;

namespace RedePanda.Backend;

/// <summary>
/// Backs <c>/health/ready</c> by asking the broker for metadata, and by waiting for the chat
/// history to be read off the topic.
/// </summary>
public sealed class BrokerReadiness : IDisposable
{
    private static readonly TimeSpan CacheDuration = TimeSpan.FromSeconds(5);
    private static readonly TimeSpan MetadataTimeout = TimeSpan.FromSeconds(3);

    private readonly IAdminClient _adminClient;
    private readonly BackendOptions _options;
    private readonly ILogger<BrokerReadiness> _logger;
    private readonly SemaphoreSlim _gate = new(1, 1);

    private bool _lastResult;
    private DateTimeOffset _lastCheck = DateTimeOffset.MinValue;

    private volatile bool _historyLoaded;

    public BrokerReadiness(BackendOptions options, ILogger<BrokerReadiness> logger)
    {
        _options = options;
        _logger = logger;

        _adminClient = new AdminClientBuilder(BuildConfig(options))
            .SetLogHandler((_, message) =>
                _logger.Log(
                    KafkaLogging.ToLogLevel(message.Level),
                    "librdkafka {Facility}: {Message}",
                    message.Facility,
                    message.Message))
            .Build();
    }

    /// <summary>The admin client's configuration, separate so it can be asserted on without a broker.</summary>
    internal static AdminClientConfig BuildConfig(BackendOptions options) =>
        BuildConfig(options, Environment.GetEnvironmentVariable);

    /// <summary>The admin client's configuration, reading security settings through <paramref name="read"/>.</summary>
    internal static AdminClientConfig BuildConfig(BackendOptions options, Func<string, string?> read)
    {
        var config = new AdminClientConfig
        {
            BootstrapServers = options.BootstrapServers,
        };

        KafkaSecurity.ApplyTo(config, read);
        return config;
    }

    /// <summary>Called by <see cref="ChatConsumerService"/> once it has caught up with the topic.</summary>
    public void MarkHistoryLoaded() => _historyLoaded = true;

    public async Task<bool> IsReadyAsync(CancellationToken cancellationToken)
    {
        if (!_historyLoaded)
        {
            return false;
        }

        if (DateTimeOffset.UtcNow - _lastCheck < CacheDuration)
        {
            return _lastResult;
        }

        await _gate.WaitAsync(cancellationToken);
        try
        {
            if (DateTimeOffset.UtcNow - _lastCheck < CacheDuration)
            {
                return _lastResult;
            }

            bool ready;
            try
            {
                var metadata = _adminClient.GetMetadata(_options.Topic, MetadataTimeout);
                ready = metadata.Brokers.Count > 0;
            }
            catch (KafkaException e)
            {
                _logger.LogWarning("Broker not ready: {Reason}", e.Error.Reason);
                ready = false;
            }

            _lastResult = ready;
            _lastCheck = DateTimeOffset.UtcNow;
            return ready;
        }
        finally
        {
            _gate.Release();
        }
    }

    public void Dispose()
    {
        _adminClient.Dispose();
        _gate.Dispose();
    }
}
