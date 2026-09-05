namespace RedeTim.Backend;

// Not thread-safe: ChatBroadcaster owns the only instance and guards every call.
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

    // afterOffset is exclusive; pass -1 for a fresh connection.
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
