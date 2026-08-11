using Confluent.Kafka;
using RedePanda.Contracts;

namespace RedePanda.Backend;

/// <summary>
/// Reads the chat topic and hands every message to the <see cref="ChatBroadcaster"/>.
/// <para>
/// The pod consumes under its own group id, so it sees every message rather than a share of the
/// partitions. That is what makes more than one backend replica possible at all.
/// </para>
/// </summary>
public sealed class ChatConsumerService(
    BackendOptions options,
    ChatBroadcaster broadcaster,
    ChatMetrics metrics,
    ILogger<ChatConsumerService> logger) : BackgroundService
{
    private IConsumer<string, string>? _consumer;

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

            // Only messages produced from now on; the topic is not a backlog to replay at startup.
            AutoOffsetReset = AutoOffsetReset.Latest,

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

                    broadcaster.Publish(message);
                    metrics.RecordMessageReceived();
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
