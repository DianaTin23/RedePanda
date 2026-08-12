namespace RedePanda.Backend;

/// <summary>
/// The messages this pod has read off the topic, kept per room so a browser joining a room can be
/// handed everything that was said in it before it connected.
/// <para>
/// Filled by <see cref="ChatConsumerService"/>, which replays the topic from the earliest retained
/// offset at startup. The buffer is therefore a projection of the topic, and its size is bounded by
/// the broker's retention — plus <c>CHAT_HISTORY_SIZE</c>, which is off by default.
/// </para>
/// <para>
/// Deliberately <b>not</b> thread-safe. <see cref="ChatBroadcaster"/> owns the only instance and
/// guards every call with its own lock, because taking a snapshot and registering a subscriber has
/// to be one atomic step; a second lock in here would suggest a safety that individual calls cannot
/// provide anyway.
/// </para>
/// </summary>
/// <param name="limit">Messages kept per room, or <c>0</c> for everything the topic still holds.</param>
internal sealed class ChatHistory(int limit)
{
    // Ordinal, matching ChatBroadcaster: rooms are opaque identifiers, not display names.
    private readonly Dictionary<string, Queue<ChatRecord>> _rooms = new(StringComparer.Ordinal);

    public void Append(ChatRecord record)
    {
        if (!_rooms.TryGetValue(record.Message.Room, out var room))
        {
            room = new Queue<ChatRecord>();
            _rooms[record.Message.Room] = room;
        }

        room.Enqueue(record);

        while (limit > 0 && room.Count > limit)
        {
            room.Dequeue();
        }
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
        if (!_rooms.TryGetValue(room, out var records))
        {
            return [];
        }

        var backlog = new List<ChatRecord>(records.Count);
        foreach (var record in records)
        {
            if (record.Offset > afterOffset)
            {
                backlog.Add(record);
            }
        }

        return backlog;
    }
}
