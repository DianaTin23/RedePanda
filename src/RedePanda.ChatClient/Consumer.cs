using Confluent.Kafka;
using RedePanda.Contracts;

namespace RedePanda.ChatClient;

public sealed class Consumer : IDisposable
{
    private readonly IConsumer<string, string> _consumer;

    public Consumer(string bootstrap, string topic, bool showHistory)
    {
        var config = new ConsumerConfig
        {
            BootstrapServers = bootstrap,

            // A unique group per client means every client receives every message instead of the
            // group splitting the partitions between them.
            GroupId = "kchat-" + Guid.NewGuid().ToString("N")[..6],
            AutoOffsetReset = showHistory ? AutoOffsetReset.Earliest : AutoOffsetReset.Latest,

            // Without this, every throwaway group id above would leave offsets behind in the
            // broker for the full retention period.
            EnableAutoCommit = false,
        };

        KafkaSecurity.ApplyTo(config);

        _consumer = new ConsumerBuilder<string, string>(config).Build();
        _consumer.Subscribe(topic);
    }

    /// <summary>Consumes until cancelled. Runs on a dedicated thread because
    /// <see cref="IConsumer{TKey,TValue}.Consume(CancellationToken)"/> blocks.</summary>
    public Task RunAsync(CancellationToken token) =>
        Task.Factory.StartNew(
            () => ConsumeLoop(token),
            token,
            TaskCreationOptions.LongRunning,
            TaskScheduler.Default);

    private void ConsumeLoop(CancellationToken token)
    {
        try
        {
            while (!token.IsCancellationRequested)
            {
                var result = _consumer.Consume(token);
                if (result?.Message?.Value is not { } payload)
                {
                    continue;
                }

                var message = ChatMessageSerializer.Deserialize(payload);
                if (message is null)
                {
                    continue;
                }

                Console.WriteLine(
                    $"[{message.Timestamp.ToLocalTime():HH:mm:ss}] ({message.Room}) {message.Nickname}: {message.Text}");
            }
        }
        catch (OperationCanceledException)
        {
            // Normal shutdown.
        }
        catch (ConsumeException e)
        {
            Console.Error.WriteLine($"Message could not be read: {e.Error.Reason}");
        }
    }

    public void Dispose()
    {
        // Close() leaves the consumer group cleanly; it must happen before Dispose().
        _consumer.Close();
        _consumer.Dispose();
    }
}
