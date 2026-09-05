using Confluent.Kafka;
using RedeTim.Contracts;

namespace RedeTim.Backend;

public sealed class PresenceConsumerService(
    BackendOptions options,
    PresenceStore store,
    BrokerReadiness readiness,
    ILogger<PresenceConsumerService> logger) : BackgroundService
{
    private IConsumer<string, string>? _consumer;

    private readonly LogThrottle _errorLog = new(KafkaLogging.ErrorInterval);

    private readonly HashSet<TopicPartition> _caughtUp = [];

    private bool _presenceLoaded;

    protected override Task ExecuteAsync(CancellationToken stoppingToken)
    {
        return Task.Factory.StartNew(
            () => ConsumeLoop(stoppingToken),
            stoppingToken,
            TaskCreationOptions.LongRunning,
            TaskScheduler.Default);
    }

    internal static ConsumerConfig BuildConfig(BackendOptions options) =>
        BuildConfig(options, Environment.GetEnvironmentVariable);

    internal static ConsumerConfig BuildConfig(BackendOptions options, Func<string, string?> read)
    {
        var config = new ConsumerConfig
        {
            BootstrapServers = options.BootstrapServers,
            GroupId = options.PresenceConsumerGroupId,
            AutoOffsetReset = AutoOffsetReset.Earliest,
            EnablePartitionEof = true,
            EnableAutoCommit = false,
        };

        KafkaSecurity.ApplyTo(config, read);
        return config;
    }

    private void ConsumeLoop(CancellationToken stoppingToken)
    {
        try
        {
            _consumer = new ConsumerBuilder<string, string>(BuildConfig(options))
                .SetPartitionsAssignedHandler(
                    (_, partitions) => partitions.Select(p => new TopicPartitionOffset(p, Offset.Beginning)))
                .SetErrorHandler((_, error) =>
                {
                    if (_errorLog.ShouldLog(out var suppressed))
                    {
                        logger.LogWarning(
                            "Presence consumer error: {Reason}{Suppressed}",
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

            _consumer.Subscribe(options.PresenceTopic);
            logger.LogInformation(
                "Consuming presence topic {Topic} as group {GroupId}",
                options.PresenceTopic,
                options.PresenceConsumerGroupId);

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

                    if (result?.Message is not { } message)
                    {
                        continue;
                    }

                    Apply(message);
                }
                catch (ConsumeException e)
                {
                    logger.LogWarning("Presence consume failed: {Reason}", e.Error.Reason);
                }
            }
        }
        catch (OperationCanceledException)
        {
        }
        catch (Exception e)
        {
            logger.LogCritical(
                e,
                "The presence consumer stopped fatally; this pod can no longer enforce the " +
                "nickname lock, but chat is unaffected. Degrading readiness open instead of " +
                "restarting the pod");
            readiness.MarkPresenceLoaded();
        }
    }

    private void Apply(Message<string, string> message)
    {
        if (message.Value is null)
        {
            if (PresenceKey.TryDecode(message.Key, out var room, out var nickname))
            {
                store.Remove(room, nickname);
            }

            return;
        }

        var record = PresenceEventSerializer.Deserialize(message.Value);
        if (record is null)
        {
            logger.LogWarning("Skipped an unreadable presence record");
            return;
        }

        store.Apply(record.Room, record.Nickname, record.RenewedAt);
    }

    private void NoteCaughtUp(TopicPartition partition)
    {
        if (_presenceLoaded)
        {
            return;
        }

        _caughtUp.Add(partition);

        var assignment = _consumer!.Assignment;
        if (assignment.Count == 0 || !assignment.All(_caughtUp.Contains))
        {
            return;
        }

        _presenceLoaded = true;
        readiness.MarkPresenceLoaded();
        logger.LogInformation(
            "Replayed presence topic {Topic} to the end; the pod can enforce the nickname lock",
            options.PresenceTopic);
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

        if (!ChatConsumerService.TryCloseWithin(
                () =>
                {
                    consumer.Close();
                    consumer.Dispose();
                },
                ChatConsumerService.CloseBudget,
                out var failure))
        {
            logger.LogWarning(
                "The presence consumer did not finish leaving its group within {Budget}; " +
                "shutting down without it",
                ChatConsumerService.CloseBudget);
            return;
        }

        if (failure is not null)
        {
            logger.LogWarning(failure, "The presence consumer reported an error while leaving its group");
        }
    }
}
