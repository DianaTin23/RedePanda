using Confluent.Kafka;
using RedeTim.Contracts;

namespace RedeTim.ChatClient;

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

        KafkaSecurity.ApplyTo(config);

        _producer = new ProducerBuilder<string, string>(config).Build();
        _topic = topic;
    }

    public async Task SendAsync(ChatMessage message, CancellationToken token)
    {
        var record = new Message<string, string>
        {
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
        }
    }

    public void Dispose()
    {
        _producer.Flush(TimeSpan.FromSeconds(5));
        _producer.Dispose();
    }
}
