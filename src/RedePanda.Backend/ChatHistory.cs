namespace RedePanda.Backend;

/// <summary>
/// The messages this pod has read off the topic, kept per room so a browser joining a room can be
/// handed everything that was said in it before it connected.
/// <para>
/// Filled by <see cref="ChatConsumerService"/>, which replays the topic from the earliest retained
/// offset at startup. The buffer is therefore a projection of the topic, and it is bounded in both
/// directions it can grow: <c>CHAT_HISTORY_SIZE</c> caps the messages held for one room, and
/// <c>CHAT_MAX_ROOMS</c> caps how many rooms are held at once. Both ship switched on; either at
/// <c>0</c> means "no limit", which is what the whole buffer used to be in the second direction.
/// </para>
/// <para>
/// Deliberately <b>not</b> thread-safe. <see cref="ChatBroadcaster"/> owns the only instance and
/// guards every call with its own lock, because taking a snapshot and registering a subscriber has
/// to be one atomic step; a second lock in here would suggest a safety that individual calls cannot
/// provide anyway.
/// </para>
/// </summary>
/// <param name="limit">Messages kept per room, or <c>0</c> for everything the topic still holds.</param>
/// <param name="roomLimit">
/// Rooms kept at once, or <c>0</c> for as many as arrive. The second bound exists because the
/// first one is not enough: <paramref name="limit"/> trims the queue <em>inside</em> a room, and
/// nothing trimmed the number of rooms. A room name is not a fixed set — it arrives from a query
/// string or from a message — so an unbounded dictionary of them is a memory bound set by whoever
/// is talking to the pod, on every replica at once, since each one consumes the whole topic.
/// </param>
internal sealed class ChatHistory(int limit, int roomLimit)
{
    // Ordinal, matching ChatBroadcaster: rooms are opaque identifiers, not display names.
    private readonly Dictionary<string, Room> _rooms = new(StringComparer.Ordinal);

    /// <summary>
    /// Counts appends, so the room to drop is decidable without a clock. A timestamp would make
    /// the eviction order depend on the wall clock of the pod rather than on the order the topic
    /// was read in, and a replay reads a whole topic within one tick of a coarse one.
    /// </summary>
    private long _appends;

    public void Append(ChatRecord record)
    {
        if (!_rooms.TryGetValue(record.Message.Room, out var room))
        {
            EvictOldestRoomIfFull();
            room = new Room();
            _rooms[record.Message.Room] = room;
        }

        room.LastAppend = ++_appends;
        room.Records.Enqueue(record);

        while (limit > 0 && room.Records.Count > limit)
        {
            room.Records.Dequeue();
        }
    }

    /// <summary>
    /// Drops the room whose last message is oldest, to make space for one that is about to be
    /// created.
    /// <para>
    /// Least recently <em>written</em>, not least recently read: a snapshot is a read, and making
    /// reads mutate the eviction order would mean a browser joining a room could evict another
    /// room's backlog — the same class of surprise this bound exists to remove. What a pod forgets
    /// here is what it can serve on a join; the messages themselves are still in the topic, which
    /// is the same trade <c>CHAT_REPLAY_RECORDS</c> already makes at startup.
    /// </para>
    /// <para>
    /// The scan is linear, and deliberately so: it runs only when a <em>new</em> room appears while
    /// the buffer is full, over at most <c>roomLimit</c> entries. A structure that made this O(1)
    /// would have to be maintained on every append instead.
    /// </para>
    /// </summary>
    private void EvictOldestRoomIfFull()
    {
        if (roomLimit <= 0 || _rooms.Count < roomLimit)
        {
            return;
        }

        var oldestName = string.Empty;
        var oldestAppend = long.MaxValue;

        foreach (var (name, room) in _rooms)
        {
            if (room.LastAppend < oldestAppend)
            {
                oldestAppend = room.LastAppend;
                oldestName = name;
            }
        }

        _rooms.Remove(oldestName);
    }

    /// <summary>
    /// Everything remembered for <paramref name="room"/> that came after
    /// <paramref name="afterOffset"/>, oldest first.
    /// </summary>
    /// <param name="afterOffset">
    /// The offset a reconnecting browser last saw, or <c>-1</c> for a fresh connection. Kafka
    /// offsets start at 0, so -1 is below every real record and lets everything through.
    /// </param>
    public IReadOnlyList<ChatRecord> Snapshot(string room, long afterOffset)
    {
        if (!_rooms.TryGetValue(room, out var entry))
        {
            return [];
        }

        var backlog = new List<ChatRecord>(entry.Records.Count);
        foreach (var record in entry.Records)
        {
            if (record.Offset > afterOffset)
            {
                backlog.Add(record);
            }
        }

        return backlog;
    }

    /// <summary>One room's messages, plus what is needed to decide which room to drop.</summary>
    private sealed class Room
    {
        public Queue<ChatRecord> Records { get; } = new();

        /// <summary>Value of <see cref="_appends"/> when this room last received a message.</summary>
        public long LastAppend { get; set; }
    }
}
