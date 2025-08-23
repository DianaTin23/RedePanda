using System.Text.Json;
using Confluent.Kafka;

namespace RedePanda_chat_client;

public class Producer
{
    private readonly IProducer<Null, string> _producer;
    private readonly string _topic;

    public Producer(string bootstrap, string topic)
    {
        var config = new ProducerConfig
        {
            BootstrapServers = bootstrap,
            Acks = Acks.Leader
        };

        _producer = new ProducerBuilder<Null, string>(config).Build();

        _topic = topic;
    }

    public async Task SendMessages(ChatMsg msg)
    {
        var json = JsonSerializer.Serialize(msg.Text);
        await _producer.ProduceAsync(_topic, new Message<Null, string> { Value = json });
    }
}