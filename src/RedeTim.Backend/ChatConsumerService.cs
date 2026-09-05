using Confluent.Kafka;
using RedeTim.Contracts;

namespace RedeTim.Backend;

public sealed class ChatConsumerService(
    BackendOptions options,
    ChatBroadcaster broadcaster,
    ChatMetrics metrics,
    BrokerReadiness readiness,
    IHostApplicationLifetime lifetime,
    ILogger<ChatConsumerService> logger) : BackgroundService
{
    private IConsumer<string, string>? _consumer;

    private readonly LogThrottle _errorLog = new(KafkaLogging.ErrorInterval);

    private readonly HashSet<TopicPartition> _caughtUp = [];

    private bool _historyLoaded;

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
            GroupId = options.ConsumerGroupId,
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
                .SetPartitionsAssignedHandler(StartOffsets)
                .SetErrorHandler((_, error) =>
                {
                    metrics.RecordKafkaError();
                    if (_errorLog.ShouldLog(out var suppressed))
                    {
                        logger.LogWarning(
                            "Kafka consumer error: {Reason}{Suppressed}",
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

            _consumer.Subscribe(options.Topic);
            logger.LogInformation(
                "Consuming topic {Topic} as group {GroupId}", options.Topic, options.ConsumerGroupId);

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

                    if (result?.Message?.Value is not { } payload)
                    {
                        continue;
                    }

                    var message = ChatMessageSerializer.Deserialize(payload);
                    if (message is null)
                    {
                        logger.LogWarning("Skipped an unreadable record at offset {Offset}", result.Offset);
                        continue;
                    }

                    broadcaster.Publish(message, result.Offset.Value);

                    if (_historyLoaded)
                    {
                        metrics.RecordMessageReceived();
                    }
                }
                catch (ConsumeException e)
                {
                    metrics.RecordKafkaError();
                    logger.LogWarning("Consume failed: {Reason}", e.Error.Reason);
                }
            }
        }
        catch (OperationCanceledException)
        {
        }
        catch (Exception e)
        {
            logger.LogCritical(
                e, "The chat consumer stopped fatally; this pod cannot serve the chat");
            Environment.ExitCode = 1;
            lifetime.StopApplication();
        }
    }

    private static readonly TimeSpan WatermarkTimeout = TimeSpan.FromSeconds(10);

    internal static long StartOffsetFor(long low, long high, int replayRecords) =>
        replayRecords <= 0 ? low : Math.Max(low, high - replayRecords);

    private IEnumerable<TopicPartitionOffset> StartOffsets(
        IConsumer<string, string> consumer, List<TopicPartition> partitions)
    {
        if (options.ReplayRecords <= 0)
        {
            logger.LogInformation(
                "CHAT_REPLAY_RECORDS is 0: replaying every partition of {Topic} in full",
                options.Topic);
            return partitions.Select(p => new TopicPartitionOffset(p, Offset.Beginning));
        }

        var assignments = new List<TopicPartitionOffset>(partitions.Count);
        foreach (var partition in partitions)
        {
            try
            {
                var watermarks = consumer.QueryWatermarkOffsets(partition, WatermarkTimeout);
                var start = StartOffsetFor(
                    watermarks.Low.Value, watermarks.High.Value, options.ReplayRecords);

                logger.LogInformation(
                    "Starting {Partition} at offset {Offset} (watermarks {Low}..{High})",
                    partition, start, watermarks.Low.Value, watermarks.High.Value);
                assignments.Add(new TopicPartitionOffset(partition, new Offset(start)));
            }
            catch (KafkaException e)
            {
                logger.LogWarning(
                    "Could not read the watermarks for {Partition} ({Reason}); replaying it in full",
                    partition, e.Error.Reason);
                assignments.Add(new TopicPartitionOffset(partition, Offset.Beginning));
            }
        }

        return assignments;
    }

    private void NoteCaughtUp(TopicPartition partition)
    {
        if (_historyLoaded)
        {
            return;
        }

        _caughtUp.Add(partition);

        var assignment = _consumer!.Assignment;
        if (assignment.Count == 0 || !assignment.All(_caughtUp.Contains))
        {
            return;
        }

        _historyLoaded = true;
        readiness.MarkHistoryLoaded();
        logger.LogInformation(
            "Replayed topic {Topic} to the end; the pod is ready to serve history", options.Topic);
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
            logger.LogWarning(
                "The Kafka consumer did not finish leaving its group within {Budget}; shutting " +
                "down without it. The group coordinator will time the member out after its " +
                "session timeout instead",
                CloseBudget);
            return;
        }

        if (failure is not null)
        {
            logger.LogWarning(
                failure, "The Kafka consumer reported an error while leaving its group");
        }
    }
}
