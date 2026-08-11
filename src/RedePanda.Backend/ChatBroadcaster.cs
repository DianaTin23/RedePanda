using System.Collections.Concurrent;
using System.Diagnostics.Metrics;
using System.Threading.Channels;
using RedePanda.Contracts;

namespace RedePanda.Backend;

/// <summary>
/// Fans messages coming off the Kafka topic out to the SSE connections held by this pod.
/// <para>
/// State here is deliberately process-local: an SSE connection belongs to exactly one pod and
/// cannot be moved. The durable chat history lives in the Kafka topic, not in this class.
/// </para>
/// </summary>
public sealed class ChatBroadcaster
{
    private readonly ConcurrentDictionary<Guid, Subscriber> _subscribers = new();

    public ChatBroadcaster(IMeterFactory meterFactory)
    {
        var meter = meterFactory.Create(ChatMetrics.MeterName);

        // Observable rather than a counter that is incremented and decremented by hand: a manual
        // counter drifts permanently whenever a client disconnect slips past the finally block.
        // The callback reads the real size every export and reports 0 from process start rather
        // than "no data".
        //
        // This instrument is an instance field of a DI singleton on purpose. A static observable
        // instrument that nothing ever references is never constructed, and the metric would then
        // silently never appear.
        meter.CreateObservableUpDownCounter("redepanda.active_connections", () => _subscribers.Count);
    }

    /// <summary>Number of open SSE connections on this pod.</summary>
    public int Count => _subscribers.Count;

    /// <summary>Registers an SSE connection for one room.</summary>
    public Subscription Subscribe(string room)
    {
        // Bounded so one stalled browser cannot grow the heap without limit; the oldest message
        // is dropped instead, which is the right trade-off for a live chat.
        var channel = Channel.CreateBounded<ChatMessage>(new BoundedChannelOptions(capacity: 256)
        {
            FullMode = BoundedChannelFullMode.DropOldest,
            SingleReader = true,
            SingleWriter = false,
        });

        var id = Guid.NewGuid();
        _subscribers[id] = new Subscriber(room, channel);
        return new Subscription(this, id, channel.Reader);
    }

    /// <summary>Delivers a message to every connection watching its room.</summary>
    public void Publish(ChatMessage message)
    {
        foreach (var (_, subscriber) in _subscribers)
        {
            if (string.Equals(subscriber.Room, message.Room, StringComparison.Ordinal))
            {
                // Bounded + DropOldest, so this never blocks and never fails for a live channel.
                subscriber.Channel.Writer.TryWrite(message);
            }
        }
    }

    private void Remove(Guid id)
    {
        if (_subscribers.TryRemove(id, out var subscriber))
        {
            subscriber.Channel.Writer.TryComplete();
        }
    }

    private sealed record Subscriber(string Room, Channel<ChatMessage> Channel);

    /// <summary>Hands out the reader and removes the subscriber again on dispose.</summary>
    public sealed class Subscription(ChatBroadcaster broadcaster, Guid id, ChannelReader<ChatMessage> reader)
        : IDisposable
    {
        public ChannelReader<ChatMessage> Reader { get; } = reader;

        public void Dispose() => broadcaster.Remove(id);
    }
}
