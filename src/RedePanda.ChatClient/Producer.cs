using Confluent.Kafka;
using RedePanda.Contracts;

namespace RedePanda.ChatClient;

public sealed class Producer : IDisposable
{
    private readonly IProducer<string, string> _producer;
    private readonly string _topic;

    public Producer(string bootstrap, string topic)
    {
        var config = new ProducerConfig
        {
            BootstrapServers = bootstrap,
            Acks = Acks.Leader,
        };

        // The console client talks to the same broker as the backend and therefore needs the same
        // credentials; see RedePanda.Contracts.KafkaSecurity.
        KafkaSecurity.ApplyTo(config);

        _producer = new ProducerBuilder<string, string>(config).Build();
        _topic = topic;
    }

    public async Task SendAsync(ChatMessage message, CancellationToken token)
    {
        var record = new Message<string, string>
        {
            // Keying by room keeps per-room ordering if the topic ever gains partitions.
            Key = message.Room,
            Value = ChatMessageSerializer.Serialize(message),
        };

        try
        {
            await _producer.ProduceAsync(_topic, record, token);
        }
        catch (ProduceException<string, string> e)
        {
            Console.Error.WriteLine($"Message could not be sent: {e.Error.Reason}");
        }
        catch (OperationCanceledException)
        {
            // Shutting down.
        }
    }

    public void Dispose()
    {
        _producer.Flush(TimeSpan.FromSeconds(5));
        _producer.Dispose();
    }
}
