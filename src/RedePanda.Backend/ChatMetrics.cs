using System.Diagnostics.Metrics;

namespace RedePanda.Backend;

/// <summary>
/// The application's own metrics.
/// <para>
/// Naming rules, because the translation to Prometheus happens in the collector's Prometheus
/// exporter and not here: dotted lowercase names, <b>no</b> <c>_total</c> suffix (the exporter
/// appends it to monotonic counters) and <b>no</b> unit (a unit of <c>"1"</c> on a gauge would
/// produce a <c>_ratio</c> suffix). <c>redepanda.messages.sent</c> arrives in Prometheus as
/// <c>redepanda_messages_sent_total</c>.
/// </para>
/// </summary>
public sealed class ChatMetrics
{
    public const string MeterName = "RedePanda";

    private readonly Counter<long> _messagesSent;
    private readonly Counter<long> _messagesReceived;
    private readonly Counter<long> _kafkaErrors;

    public ChatMetrics(IMeterFactory meterFactory)
    {
        var meter = meterFactory.Create(MeterName);

        _messagesSent = meter.CreateCounter<long>("redepanda.messages.sent");
        _messagesReceived = meter.CreateCounter<long>("redepanda.messages.received");
        _kafkaErrors = meter.CreateCounter<long>("redepanda.kafka.errors");
    }

    /// <summary>A message was accepted and handed to the producer.</summary>
    public void RecordMessageSent() => _messagesSent.Add(1);

    /// <summary>A message came back from the topic through the consumer.</summary>
    public void RecordMessageReceived() => _messagesReceived.Add(1);

    /// <summary>A produce or consume operation failed.</summary>
    public void RecordKafkaError() => _kafkaErrors.Add(1);
}
