namespace RedeTim.Backend;

/// <summary>
/// The messages this pod has read off the topic, kept per room. Not thread-safe;
/// <see cref="ChatBroadcaster"/> owns the only instance and guards every call.
/// </summary>
internal sealed class ChatHistory(int limit, int roomLimit)
{
    private readonly Dictionary<string, Room> _rooms = new(StringComparer.Ordinal);

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
    /// Everything remembered for <paramref name="room"/> after <paramref name="afterOffset"/>,
    /// oldest first. Pass <c>-1</c> for a fresh connection.
    /// </summary>
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

    private sealed class Room
    {
        public Queue<ChatRecord> Records { get; } = new();

        public long LastAppend { get; set; }
    }
}
