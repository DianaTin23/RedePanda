using Confluent.Kafka;
using RedePanda.Contracts;

namespace RedePanda.Backend;

/// <summary>
/// Reads the chat topic and hands every message to the <see cref="ChatBroadcaster"/>.
/// <para>
/// The pod consumes under its own group id, so it sees every message rather than a share of the
/// partitions. That is what makes more than one backend replica possible at all.
/// </para>
/// <para>
/// The first thing it does is rebuild the chat history from the topic, starting
/// <c>CHAT_REPLAY_RECORDS</c> records back rather than at the very beginning — see
/// <see cref="BackendOptions.ReplayRecords"/> for why that is a separate number from the history
/// size. Only once every assigned partition has reported EOF is the pod considered ready; see
/// <see cref="BrokerReadiness.MarkHistoryLoaded"/>.
/// </para>
/// </summary>
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

    /// <summary>Partitions that have been read to the end at least once.</summary>
    private readonly HashSet<TopicPartition> _caughtUp = [];

    /// <summary>
    /// False while the topic is being replayed. Only ever touched from the consume loop.
    /// </summary>
    private bool _historyLoaded;

    protected override Task ExecuteAsync(CancellationToken stoppingToken)
    {
        // Consume(CancellationToken) blocks, so it must not run on a thread-pool thread: the host
        // would not start any further service until this method yielded.
        return Task.Factory.StartNew(
            () => ConsumeLoop(stoppingToken),
            stoppingToken,
            TaskCreationOptions.LongRunning,
            TaskScheduler.Default);
    }

    /// <summary>
    /// The consumer's configuration, separate from the loop so it can be asserted on without a
    /// broker in the picture.
    /// </summary>
    internal static ConsumerConfig BuildConfig(BackendOptions options) =>
        BuildConfig(options, Environment.GetEnvironmentVariable);

    /// <param name="read">
    /// Where a security setting comes from, injected for the same reason
    /// <see cref="KafkaSecurity.ApplyTo(ClientConfig, Func{string, string?})"/> injects it.
    /// </param>
    internal static ConsumerConfig BuildConfig(BackendOptions options, Func<string, string?> read)
    {
        var config = new ConsumerConfig
        {
            BootstrapServers = options.BootstrapServers,
            GroupId = options.ConsumerGroupId,

            // The topic *is* the chat history: every pod incarnation replays part of it and
            // rebuilds the per-room buffer that browsers are served on join. How far back it
            // starts is decided per partition in StartOffsets; this is only the fallback for when
            // no offset applies, and Earliest keeps that fallback on the safe side of the trade.
            AutoOffsetReset = AutoOffsetReset.Earliest,

            // Turns "no more records for now" into an actual Consume result. Without it there is no
            // way to tell the replay from the live tail, and the pod could never report itself
            // ready at the right moment.
            EnablePartitionEof = true,

            // Each pod incarnation invents a new group id, so committing would leave offset
            // records behind in the broker for the whole retention period for no benefit.
            EnableAutoCommit = false,
        };

        // A no-op against the plaintext broker in the chart; the whole of TLS and SASL against
        // anything else. See RedePanda.Contracts.KafkaSecurity.
        KafkaSecurity.ApplyTo(config, read);
        return config;
    }

    private void ConsumeLoop(CancellationToken stoppingToken)
    {
        try
        {
            // Inside the try, not before it. Building the consumer reads the broker security
            // settings, and a missing one throws; so does Subscribe against a broker that refuses
            // the group. Outside, that fault reached BackgroundService, whose default StopHost
            // behaviour asks the host to stop *gracefully* -- so the process exited 0, Kubernetes
            // reported "Completed" instead of a crash, no restart followed, and /health/live went
            // on answering from a pod that would never deliver a message.
            _consumer = new ConsumerBuilder<string, string>(BuildConfig(options))

                // Where this pod starts reading. Without it every replica replayed the entire
                // topic before it could report ready, so startup time and broker read load both
                // grew with the topic *and* with the replica count.
                .SetPartitionsAssignedHandler(StartOffsets)
                .SetErrorHandler((_, error) =>
                {
                    // The metric counts every error; the log reports at most one per interval.
                    metrics.RecordKafkaError();
                    if (_errorLog.ShouldLog(out var suppressed))
                    {
                        logger.LogWarning(
                            "Kafka consumer error: {Reason}{Suppressed}",
                            error.Reason,
                            LogThrottle.Describe(suppressed));
                    }
                })

                // Otherwise librdkafka's own diagnostics go straight to stderr, outside LOG_LEVEL
                // and outside the JSON the platform collects.
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

                    // Replayed messages are deliberately not counted. They were counted when they
                    // were first consumed, and counting them again would make
                    // redepanda_messages_received_total jump by the whole retention on every pod
                    // restart — the counter is meant to track the live chat, not our own backfill.
                    if (_historyLoaded)
                    {
                        metrics.RecordMessageReceived();
                    }
                }
                catch (ConsumeException e)
                {
                    // A single bad record must not end the loop.
                    metrics.RecordKafkaError();
                    logger.LogWarning("Consume failed: {Reason}", e.Error.Reason);
                }
            }
        }
        catch (OperationCanceledException)
        {
            // Normal shutdown.
        }
        catch (Exception e)
        {
            // A pod whose consumer will not start is not degraded, it is useless: it can accept a
            // POST and will never show anyone a message. Say so at Critical, set a non-zero exit
            // code so the restart actually looks like a failure, and stop rather than idle.
            logger.LogCritical(
                e, "The chat consumer stopped fatally; this pod cannot serve the chat");
            Environment.ExitCode = 1;
            lifetime.StopApplication();
        }
    }

    /// <summary>How long to wait for one partition's watermarks before giving up on them.</summary>
    private static readonly TimeSpan WatermarkTimeout = TimeSpan.FromSeconds(10);

    /// <summary>
    /// Where to start reading one partition, given the offsets the broker still holds for it.
    /// <para>
    /// Separated from the assignment handler because it is the half that can be quietly wrong: an
    /// offset below <paramref name="low"/> is not rejected, it makes librdkafka fall back to
    /// <c>auto.offset.reset</c>, and the pod then replays the whole partition while appearing to
    /// have asked for a window. That failure looks like nothing at all from the outside.
    /// </para>
    /// </summary>
    /// <param name="low">First offset the broker still holds; above 0 once retention has bitten.</param>
    /// <param name="high">One past the last offset — the next record written gets this one.</param>
    internal static long StartOffsetFor(long low, long high, int replayRecords) =>
        replayRecords <= 0 ? low : Math.Max(low, high - replayRecords);

    /// <summary>
    /// The offset each newly assigned partition should start from: far enough back to fill the
    /// room buffers, not so far back that a pod reads the whole topic to serve a screenful.
    /// <para>
    /// The low watermark is the floor. Asking for an offset the broker has already deleted is not
    /// an error librdkafka reports usefully — it silently applies <c>auto.offset.reset</c>
    /// instead, which would put the behaviour back exactly where it started.
    /// </para>
    /// </summary>
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
                // Not worth failing a pod over. Replaying the partition in full is the older,
                // slower behaviour rather than a wrong one, so it is the right thing to fall back
                // to -- but it is said out loud, because a pod that quietly takes the slow path is
                // how a startup-time regression goes unexplained.
                logger.LogWarning(
                    "Could not read the watermarks for {Partition} ({Reason}); replaying it in full",
                    partition, e.Error.Reason);
                assignments.Add(new TopicPartitionOffset(partition, Offset.Beginning));
            }
        }

        return assignments;
    }

    /// <summary>
    /// Records that one partition has been read to its end, and releases the readiness gate once
    /// every assigned partition has.
    /// </summary>
    private void NoteCaughtUp(TopicPartition partition)
    {
        if (_historyLoaded)
        {
            return;
        }

        _caughtUp.Add(partition);

        // Read from the consumer rather than remembered from an assignment handler: this is the
        // set librdkafka is actually feeding us, and an empty one means nothing is assigned yet.
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

    public override async Task StopAsync(CancellationToken cancellationToken)
    {
        await base.StopAsync(cancellationToken);

        // Close() tells the group coordinator we are leaving instead of waiting for the session
        // timeout. It must run before Dispose().
        _consumer?.Close();
        _consumer?.Dispose();
        _consumer = null;
    }
}
