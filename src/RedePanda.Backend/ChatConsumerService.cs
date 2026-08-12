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
/// It starts at the earliest retained offset, so the first thing it does is rebuild the chat
/// history from the topic. Only once every assigned partition has reported EOF is the pod
/// considered ready; see <see cref="BrokerReadiness.MarkHistoryLoaded"/>.
/// </para>
/// </summary>
public sealed class ChatConsumerService(
    BackendOptions options,
    ChatBroadcaster broadcaster,
    ChatMetrics metrics,
    BrokerReadiness readiness,
    ILogger<ChatConsumerService> logger) : BackgroundService
{
    private IConsumer<string, string>? _consumer;

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

    private void ConsumeLoop(CancellationToken stoppingToken)
    {
        var config = new ConsumerConfig
        {
            BootstrapServers = options.BootstrapServers,
            GroupId = options.ConsumerGroupId,

            // The topic *is* the chat history: every pod incarnation replays it from the start and
            // rebuilds the per-room buffer that browsers are served on join. What a room shows is
            // therefore whatever the broker still retains.
            AutoOffsetReset = AutoOffsetReset.Earliest,

            // Turns "no more records for now" into an actual Consume result. Without it there is no
            // way to tell the replay from the live tail, and the pod could never report itself
            // ready at the right moment.
            EnablePartitionEof = true,

            // Each pod incarnation invents a new group id, so committing would leave offset
            // records behind in the broker for the whole retention period for no benefit.
            EnableAutoCommit = false,
        };

        _consumer = new ConsumerBuilder<string, string>(config)
            .SetErrorHandler((_, error) =>
            {
                metrics.RecordKafkaError();
                logger.LogWarning("Kafka consumer error: {Reason}", error.Reason);
            })
            .Build();

        _consumer.Subscribe(options.Topic);
        logger.LogInformation(
            "Consuming topic {Topic} as group {GroupId}", options.Topic, options.ConsumerGroupId);

        try
        {
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
