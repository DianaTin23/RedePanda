using Confluent.Kafka;

namespace RedePanda.Backend;

/// <summary>
/// Backs <c>/health/ready</c> by asking the broker for metadata.
/// <para>
/// The result is cached briefly because the readiness probe runs every few seconds and a
/// metadata round trip per probe would be pointless load on the broker.
/// </para>
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

    public BrokerReadiness(BackendOptions options, ILogger<BrokerReadiness> logger)
    {
        _options = options;
        _logger = logger;
        _adminClient = new AdminClientBuilder(new AdminClientConfig
        {
            BootstrapServers = options.BootstrapServers,
        }).Build();
    }

    public async Task<bool> IsReadyAsync(CancellationToken cancellationToken)
    {
        if (DateTimeOffset.UtcNow - _lastCheck < CacheDuration)
        {
            return _lastResult;
        }

        await _gate.WaitAsync(cancellationToken);
        try
        {
            // Re-check: another request may have refreshed the cache while we waited.
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
                _logger.LogDebug("Broker not ready: {Reason}", e.Error.Reason);
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
