using System.Text.Json;
using Confluent.Kafka;

namespace RedePanda_chat_client;

public class Consumer
{
    private readonly IConsumer<Null, string> _consumer;

    public Consumer(string bootstrap, string topic, bool showHist)
    {
        AutoOffsetReset autoOffsetReset;
        if (showHist)
        {
            autoOffsetReset = AutoOffsetReset.Earliest;
        }
        else
        {
            autoOffsetReset = AutoOffsetReset.Latest;
        }

        var config = new ConsumerConfig
        {
            BootstrapServers = bootstrap,
            GroupId = "kchat-" + Guid.NewGuid().ToString("N")[..6],
            AutoOffsetReset = autoOffsetReset,
            EnablePartitionEof = true
        };

        _consumer = new ConsumerBuilder<Null, string>(config).Build();
        _consumer.Subscribe(topic);
    }

    public void ConsumeMessages(CancellationToken token)
    {
        Task.Run(() =>
        {
            try
            {
                while (!token.IsCancellationRequested)
                {
                    var cr = _consumer.Consume(token);
                    if (cr?.Message?.Value is not null)
                    {
                        var msg = JsonSerializer.Deserialize<ChatMsg>(cr.Message.Value);
                        if (msg is not null) Console.WriteLine($"[{msg.Ts}] {msg.Nick}: {msg.Text}");
                    }
                }
                Console.WriteLine("Chat was closed, shutting down...");
            }
            catch (Exception e)
            {
                Console.WriteLine("Message can not be loaded: " + e.Message);
            }
            finally
            {
                _consumer.Close();
            }
        });
    }
}