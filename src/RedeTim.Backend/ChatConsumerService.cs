using Confluent.Kafka;
using RedeTim.Contracts;

namespace RedeTim.Backend;

public sealed class ChatConsumerService : KafkaConsumerService
{
    private readonly BackendOptions _options;
    private readonly ChatBroadcaster _broadcaster;
    private readonly ChatMetrics _metrics;
    private readonly BrokerReadiness _readiness;
    private readonly IHostApplicationLifetime _lifetime;

    public ChatConsumerService(
        BackendOptions options,
        ChatBroadcaster broadcaster,
        ChatMetrics metrics,
        BrokerReadiness readiness,
        IHostApplicationLifetime lifetime,
        ILogger<ChatConsumerService> logger)
        : base(
            "chat consumer", options.Topic, options.ConsumerGroupId,
            BuildConfig(options), metrics, logger)
    {
        _options = options;
        _broadcaster = broadcaster;
        _metrics = metrics;
        _readiness = readiness;
        _lifetime = lifetime;
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

    protected override void Handle(ConsumeResult<string, string> result)
    {
        if (result.Message.Value is not { } payload)
        {
            return;
        }

        var message = WireFormat.Deserialize<ChatMessage>(payload);
        if (message is null)
        {
            Logger.LogWarning("Skipped an unreadable record at offset {Offset}", result.Offset);
            return;
        }

        _broadcaster.Publish(message, result.Offset.Value);

        // Only after the replay: the backlog is history, not traffic.
        if (Replayed)
        {
            _metrics.RecordMessageReceived();
        }
    }

    protected override void OnReplayed()
    {
        _readiness.MarkHistoryLoaded();
        Logger.LogInformation(
            "Replayed topic {Topic} to the end; the pod is ready to serve history", Topic);
    }

    // The chat is what this pod exists for: without a consumer it cannot serve, so it goes down
    // and lets Kubernetes replace it. The presence consumer deliberately does the opposite.
    protected override void OnFatal(Exception exception)
    {
        Logger.LogCritical(
            exception, "The chat consumer stopped fatally; this pod cannot serve the chat");
        Environment.ExitCode = 1;
        _lifetime.StopApplication();
    }

    private static readonly TimeSpan WatermarkTimeout = TimeSpan.FromSeconds(10);

    internal static long StartOffsetFor(long low, long high, int replayRecords) =>
        replayRecords <= 0 ? low : Math.Max(low, high - replayRecords);

    protected override IEnumerable<TopicPartitionOffset> StartOffsets(
        IConsumer<string, string> consumer, List<TopicPartition> partitions)
    {
        if (_options.ReplayRecords <= 0)
        {
            Logger.LogInformation(
                "CHAT_REPLAY_RECORDS is 0: replaying every partition of {Topic} in full", Topic);
            return base.StartOffsets(consumer, partitions);
        }

        var assignments = new List<TopicPartitionOffset>(partitions.Count);
        foreach (var partition in partitions)
        {
            try
            {
                var watermarks = consumer.QueryWatermarkOffsets(partition, WatermarkTimeout);
                var start = StartOffsetFor(
                    watermarks.Low.Value, watermarks.High.Value, _options.ReplayRecords);

                Logger.LogInformation(
                    "Starting {Partition} at offset {Offset} (watermarks {Low}..{High})",
                    partition, start, watermarks.Low.Value, watermarks.High.Value);
                assignments.Add(new TopicPartitionOffset(partition, new Offset(start)));
            }
            catch (KafkaException e)
            {
                Logger.LogWarning(
                    "Could not read the watermarks for {Partition} ({Reason}); replaying it in full",
                    partition, e.Error.Reason);
                assignments.Add(new TopicPartitionOffset(partition, Offset.Beginning));
            }
        }

        return assignments;
    }
}
