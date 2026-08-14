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

            GroupId = "kchat-" + Guid.NewGuid().ToString("N")[..6],
            AutoOffsetReset = showHistory ? AutoOffsetReset.Earliest : AutoOffsetReset.Latest,

            EnableAutoCommit = false,
        };

        KafkaSecurity.ApplyTo(config);

        _consumer = new ConsumerBuilder<string, string>(config).Build();
        _consumer.Subscribe(topic);
    }

    /// <summary>Consumes until cancelled, on a dedicated thread because <c>Consume</c> blocks.</summary>
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
        _consumer.Close();
        _consumer.Dispose();
    }
}
