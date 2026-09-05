using System.Collections.Concurrent;
using System.Threading.Channels;
using RedeTim.Contracts;

namespace RedeTim.Backend;

public sealed class ChatBroadcaster
{
    private readonly ConcurrentDictionary<Guid, Subscriber> _subscribers = new();
    private readonly ChatHistory _history;
    private readonly ILogger<ChatBroadcaster> _logger;

    private readonly Lock _gate = new();

    public ChatBroadcaster(BackendOptions options, ILogger<ChatBroadcaster> logger)
    {
        _history = new ChatHistory(options.HistorySize, options.MaxRooms);
        _logger = logger;
    }

    internal const int SubscriberBufferSize = 256;

    public int Count => _subscribers.Count;

    public Subscription Subscribe(string room, long afterOffset = -1)
    {
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
            backlog = _history.Snapshot(room, afterOffset);
            _subscribers[id] = new Subscriber(room, channel);
        }

        return new Subscription(this, id, channel.Reader, backlog);
    }

    public void Publish(ChatMessage message, long offset)
    {
        var record = new ChatRecord(offset, message);

        lock (_gate)
        {
            _history.Append(record);

            foreach (var (_, subscriber) in _subscribers)
            {
                if (!string.Equals(subscriber.Room, message.Room, StringComparison.Ordinal))
                {
                    continue;
                }

                if (!subscriber.Channel.Writer.TryWrite(record) &&
                    subscriber.Channel.Writer.TryComplete())
                {
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

    public sealed class Subscription(
        ChatBroadcaster broadcaster,
        Guid id,
        ChannelReader<ChatRecord> reader,
        IReadOnlyList<ChatRecord> backlog)
        : IDisposable
    {
        public IReadOnlyList<ChatRecord> Backlog { get; } = backlog;

        public ChannelReader<ChatRecord> Reader { get; } = reader;

        public void Dispose() => broadcaster.Remove(id);
    }
}
