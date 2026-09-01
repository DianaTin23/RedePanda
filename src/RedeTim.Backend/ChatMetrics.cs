using System.Diagnostics.Metrics;

namespace RedeTim.Backend;

/// <summary>The application's own metrics.</summary>
public sealed class ChatMetrics
{
    public const string MeterName = "RedeTim";

    private readonly Counter<long> _messagesSent;
    private readonly Counter<long> _messagesReceived;
    private readonly Counter<long> _kafkaErrors;
    private readonly Counter<long> _streamsCut;

    public ChatMetrics(IMeterFactory meterFactory)
    {
        var meter = meterFactory.Create(MeterName);

        // Naming is bound to the collector's Prometheus exporter: dotted lowercase, no _total
        // suffix, no unit. See docs/observability.md.
        _messagesSent = meter.CreateCounter<long>("redetim.messages.sent");
        _messagesReceived = meter.CreateCounter<long>("redetim.messages.received");
        _kafkaErrors = meter.CreateCounter<long>("redetim.kafka.errors");
        _streamsCut = meter.CreateCounter<long>("redetim.streams.cut");
    }

    /// <summary>A message was accepted and handed to the producer.</summary>
    public void RecordMessageSent() => _messagesSent.Add(1);

    /// <summary>A message came back from the topic through the consumer.</summary>
    public void RecordMessageReceived() => _messagesReceived.Add(1);

    /// <summary>A produce or consume operation failed.</summary>
    public void RecordKafkaError() => _kafkaErrors.Add(1);

    /// <summary>An SSE subscriber fell far enough behind that its stream was ended.</summary>
    public void RecordStreamCut() => _streamsCut.Add(1);
}
