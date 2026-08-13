using System.Collections.Concurrent;
using System.Diagnostics.Metrics;
using System.Threading.Channels;
using RedePanda.Contracts;

namespace RedePanda.Backend;

/// <summary>
/// Fans messages coming off the Kafka topic out to the SSE connections held by this pod, and keeps
/// what it has seen so a browser joining a room gets the conversation so far.
/// <para>
/// State here is deliberately process-local: an SSE connection belongs to exactly one pod and
/// cannot be moved. The durable history still lives in the Kafka topic — what this class holds is a
/// projection of it, rebuilt from the topic by <see cref="ChatConsumerService"/> on every start.
/// </para>
/// </summary>
public sealed class ChatBroadcaster
{
    private readonly ConcurrentDictionary<Guid, Subscriber> _subscribers = new();
    private readonly ChatHistory _history;
    private readonly ChatMetrics _metrics;
    private readonly ILogger<ChatBroadcaster> _logger;

    /// <summary>
    /// Serialises recording-and-fanning-out against snapshotting-and-subscribing. Without it there
    /// is a window between a new connection taking its snapshot and appearing in
    /// <see cref="_subscribers"/>, and a message arriving inside that window is either delivered
    /// twice or not at all.
    /// </summary>
    private readonly Lock _gate = new();

    public ChatBroadcaster(
        BackendOptions options,
        IMeterFactory meterFactory,
        ChatMetrics metrics,
        ILogger<ChatBroadcaster> logger)
    {
        _history = new ChatHistory(options.HistorySize, options.MaxRooms);
        _metrics = metrics;
        _logger = logger;

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

    /// <summary>
    /// Messages one SSE connection may fall behind before its stream is ended and it is made to
    /// reconnect. Large enough that an ordinary slow render never reaches it, small enough that a
    /// browser that has genuinely stopped reading cannot hold much memory hostage.
    /// </summary>
    internal const int SubscriberBufferSize = 256;

    /// <summary>Number of open SSE connections on this pod.</summary>
    public int Count => _subscribers.Count;

    /// <summary>
    /// Registers an SSE connection for one room and hands back everything already said in it.
    /// </summary>
    /// <param name="afterOffset">
    /// The offset the client last saw, taken from its <c>Last-Event-ID</c> header, or <c>-1</c> to
    /// replay the whole room.
    /// </param>
    public Subscription Subscribe(string room, long afterOffset = -1)
    {
        // Bounded so one stalled browser cannot grow the heap without limit.
        //
        // FullMode is Wait rather than DropOldest, and nothing ever waits on it: TryWrite returns
        // false instead of blocking, and Publish treats that as the end of this subscription.
        // DropOldest was worse than it looked -- it silently discarded a message for a reader that
        // was merely slow, with no log, no metric, and no way for the browser to learn it had a
        // hole. Ending the stream instead is recoverable: ChatStream stops on a completed channel,
        // EventSource reconnects with Last-Event-ID, and the replay fills the gap exactly.
        var channel = Channel.CreateBounded<ChatRecord>(new BoundedChannelOptions(SubscriberBufferSize)
        {
            FullMode = BoundedChannelFullMode.Wait,
            SingleReader = true,
            SingleWriter = false,
        });

        var id = Guid.NewGuid();

        IReadOnlyList<ChatRecord> backlog;
        lock (_gate)
        {
            // Snapshot and registration have to happen together — see the comment on _gate.
            backlog = _history.Snapshot(room, afterOffset);
            _subscribers[id] = new Subscriber(room, channel);
        }

        return new Subscription(this, id, channel.Reader, backlog);
    }

    /// <summary>Records a message and delivers it to every connection watching its room.</summary>
    public void Publish(ChatMessage message, long offset)
    {
        var record = new ChatRecord(offset, message);

        // Holding the lock across the fan-out is safe because nothing in it blocks: TryWrite
        // returns false on a full buffer rather than waiting, which is true of FullMode.Wait as
        // much as it was of DropOldest. Only the answer to a full buffer changed.
        lock (_gate)
        {
            _history.Append(record);

            foreach (var (_, subscriber) in _subscribers)
            {
                if (!string.Equals(subscriber.Room, message.Room, StringComparison.Ordinal))
                {
                    continue;
                }

                // TryComplete is inside the condition on purpose: it returns true only the first
                // time, so a subscriber that has already been cut and not yet disposed cannot log
                // and count once per message for the rest of its life.
                if (!subscriber.Channel.Writer.TryWrite(record) &&
                    subscriber.Channel.Writer.TryComplete())
                {
                    _metrics.RecordStreamCut();
                    _logger.LogWarning(
                        "A subscriber in room {Room} fell {Capacity} messages behind; its stream " +
                        "was ended so the browser reconnects and replays the gap instead of " +
                        "silently missing it",
                        message.Room,
                        SubscriberBufferSize);
                }
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

    private sealed record Subscriber(string Room, Channel<ChatRecord> Channel);

    /// <summary>Hands out the backlog and the reader, and removes the subscriber again on dispose.</summary>
    public sealed class Subscription(
        ChatBroadcaster broadcaster,
        Guid id,
        ChannelReader<ChatRecord> reader,
        IReadOnlyList<ChatRecord> backlog)
        : IDisposable
    {
        /// <summary>What was already said in the room, oldest first. Replay this before reading.</summary>
        public IReadOnlyList<ChatRecord> Backlog { get; } = backlog;

        public ChannelReader<ChatRecord> Reader { get; } = reader;

        public void Dispose() => broadcaster.Remove(id);
    }
}
