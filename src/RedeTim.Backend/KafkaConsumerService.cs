using Confluent.Kafka;

namespace RedeTim.Backend;

// The consume loop both consumers share: the long-running thread, the error and log handlers,
// tracking when every assigned partition has reached its end, and the bounded close on shutdown.
//
// What the two do *not* share is deliberate and documented, so it stays with the subclasses:
// where they start reading (docs/kafka.md#wiederaufnahme), and what a fatal error means. The
// chat consumer takes the pod down with it; the presence consumer degrades open and keeps
// serving. See docs/kafka.md and README section 13.
public abstract class KafkaConsumerService : BackgroundService
{
    private readonly string _role;
    private readonly string _topic;
    private readonly string _groupId;
    private readonly ConsumerConfig _config;
    private readonly ILogger _logger;

    private readonly LogThrottle _errorLog = new(KafkaLogging.ErrorInterval);
    private readonly HashSet<TopicPartition> _caughtUp = [];

    private IConsumer<string, string>? _consumer;

    protected KafkaConsumerService(
        string role,
        string topic,
        string groupId,
        ConsumerConfig config,
        ILogger logger)
    {
        _role = role;
        _topic = topic;
        _groupId = groupId;
        _config = config;
        _logger = logger;
    }

    // True once every assigned partition has been read to its end at least once.
    protected bool Replayed { get; private set; }

    protected ILogger Logger => _logger;

    protected string Topic => _topic;

    // Reading the whole topic from the start is the safe default; only the chat consumer has a
    // reason to skip ahead, and it overrides this.
    protected virtual IEnumerable<TopicPartitionOffset> StartOffsets(
        IConsumer<string, string> consumer, List<TopicPartition> partitions) =>
        partitions.Select(p => new TopicPartitionOffset(p, Offset.Beginning));

    protected abstract void Handle(ConsumeResult<string, string> result);

    // Called once, when the last assigned partition reaches its end.
    protected abstract void OnReplayed();

    // The asymmetry that matters: see the class comment.
    protected abstract void OnFatal(Exception exception);

    protected override Task ExecuteAsync(CancellationToken stoppingToken)
    {
        return Task.Factory.StartNew(
            () => ConsumeLoop(stoppingToken),
            stoppingToken,
            TaskCreationOptions.LongRunning,
            TaskScheduler.Default);
    }

    private void ConsumeLoop(CancellationToken stoppingToken)
    {
        try
        {
            _consumer = new ConsumerBuilder<string, string>(_config)
                .SetPartitionsAssignedHandler(StartOffsets)
                .SetErrorHandler((_, error) =>
                {
                    if (_errorLog.ShouldLog(out var suppressed))
                    {
                        _logger.LogWarning(
                            "{Role} error: {Reason}{Suppressed}",
                            _role,
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

            _consumer.Subscribe(_topic);
            _logger.LogInformation(
                "Consuming topic {Topic} as group {GroupId}", _topic, _groupId);

            while (!stoppingToken.IsCancellationRequested)
            {
                try
                {
                    var result = _consumer.Consume(stoppingToken);

                    if (result is not null && result.IsPartitionEOF)
                    {
                        NoteCaughtUp(result.TopicPartition);
                        continue;
                    }

                    if (result?.Message is null)
                    {
                        continue;
                    }

                    Handle(result);
                }
                catch (ConsumeException e)
                {
                    _logger.LogWarning("{Role} consume failed: {Reason}", _role, e.Error.Reason);
                }
            }
        }
        catch (OperationCanceledException)
        {
        }
        catch (Exception e)
        {
            OnFatal(e);
        }
    }

    private void NoteCaughtUp(TopicPartition partition)
    {
        if (Replayed)
        {
            return;
        }

        _caughtUp.Add(partition);

        var assignment = _consumer!.Assignment;
        if (assignment.Count == 0 || !assignment.All(_caughtUp.Contains))
        {
            return;
        }

        Replayed = true;
        OnReplayed();
    }

    internal static readonly TimeSpan CloseBudget = TimeSpan.FromSeconds(5);

    // Abandonable: the close runs on a background thread and the pod stops waiting after budget.
    internal static bool TryCloseWithin(Action close, TimeSpan budget, out Exception? failure)
    {
        Exception? caught = null;

        var closer = new Thread(() =>
        {
            try
            {
                close();
            }
            catch (Exception e)
            {
                caught = e;
            }
        })
        {
            IsBackground = true,
            Name = "kafka-consumer-close",
        };

        closer.Start();
        var finished = closer.Join(budget);

        failure = finished ? caught : null;
        return finished;
    }

    public override async Task StopAsync(CancellationToken cancellationToken)
    {
        await base.StopAsync(cancellationToken);

        var consumer = _consumer;
        _consumer = null;

        if (consumer is null)
        {
            return;
        }

        if (!TryCloseWithin(
                () =>
                {
                    consumer.Close();
                    consumer.Dispose();
                },
                CloseBudget,
                out var failure))
        {
            _logger.LogWarning(
                "The {Role} did not finish leaving its group within {Budget}; shutting down " +
                "without it. The group coordinator will time the member out after its session " +
                "timeout instead",
                _role,
                CloseBudget);
            return;
        }

        if (failure is not null)
        {
            _logger.LogWarning(
                failure, "The {Role} reported an error while leaving its group", _role);
        }
    }
}
