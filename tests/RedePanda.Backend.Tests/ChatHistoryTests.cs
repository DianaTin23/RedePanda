using RedePanda.Contracts;

namespace RedePanda.Backend.Tests;

/// <summary>
/// The buffer a joining browser is served from. Room separation is the same promise the
/// broadcaster makes, and the offset filter is what keeps a reconnect from repeating a room the
/// reader has already seen.
/// </summary>
public class ChatHistoryTests
{
    private static ChatRecord Record(long offset, string room, string text) =>
        new(offset, new ChatMessage(room, "alice", text, DateTimeOffset.UtcNow));

    [Fact]
    public void SnapshotReturnsWhatWasAppendedInOrder()
    {
        var history = new ChatHistory(limit: 0, roomLimit: 0);
        history.Append(Record(0, "general", "erste"));
        history.Append(Record(1, "general", "zweite"));

        var backlog = history.Snapshot("general", afterOffset: -1);

        Assert.Equal(["erste", "zweite"], backlog.Select(r => r.Message.Text));
    }

    [Fact]
    public void SnapshotOfAnUnknownRoomIsEmpty()
    {
        var history = new ChatHistory(limit: 0, roomLimit: 0);
        history.Append(Record(0, "general", "hallo"));

        Assert.Empty(history.Snapshot("nie-betreten", afterOffset: -1));
    }

    [Fact]
    public void RoomsAreKeptApart()
    {
        var history = new ChatHistory(limit: 0, roomLimit: 0);
        history.Append(Record(0, "general", "hallo"));
        history.Append(Record(1, "andererraum", "geheim"));

        var backlog = history.Snapshot("general", afterOffset: -1);

        Assert.Equal("hallo", Assert.Single(backlog).Message.Text);
    }

    [Fact]
    public void RoomMatchingIsCaseSensitive()
    {
        // Same rule as the broadcaster: a room is an opaque identifier, not a display name.
        var history = new ChatHistory(limit: 0, roomLimit: 0);
        history.Append(Record(0, "General", "hallo"));

        Assert.Empty(history.Snapshot("general", afterOffset: -1));
    }

    /// <summary>
    /// The <c>Last-Event-ID</c> path: a browser that reconnects must be told what it missed, not
    /// handed the room a second time.
    /// </summary>
    [Fact]
    public void SnapshotSkipsEverythingUpToTheGivenOffset()
    {
        var history = new ChatHistory(limit: 0, roomLimit: 0);
        history.Append(Record(4, "general", "vorher"));
        history.Append(Record(5, "general", "gesehen"));
        history.Append(Record(6, "general", "verpasst"));

        var backlog = history.Snapshot("general", afterOffset: 5);

        Assert.Equal("verpasst", Assert.Single(backlog).Message.Text);
    }

    [Fact]
    public void ZeroMeansEverythingIsKept()
    {
        var history = new ChatHistory(limit: 0, roomLimit: 0);
        for (var offset = 0; offset < 500; offset++)
        {
            history.Append(Record(offset, "general", $"nachricht {offset}"));
        }

        Assert.Equal(500, history.Snapshot("general", afterOffset: -1).Count);
    }

    [Fact]
    public void ALimitDropsTheOldestMessagesFirst()
    {
        var history = new ChatHistory(limit: 2, roomLimit: 0);
        history.Append(Record(0, "general", "älteste"));
        history.Append(Record(1, "general", "mittlere"));
        history.Append(Record(2, "general", "neueste"));

        var backlog = history.Snapshot("general", afterOffset: -1);

        Assert.Equal(["mittlere", "neueste"], backlog.Select(r => r.Message.Text));
    }

    /// <summary>
    /// Not a tautology: this runs the shipped default through the trim in <c>Append</c>. Every
    /// replica holds this buffer, so putting it back to 0 breaks a test instead of quietly
    /// multiplying the memory cost by the replica count.
    /// </summary>
    [Fact]
    public void TheShippedDefaultKeepsTheBufferBounded()
    {
        var history = new ChatHistory(
            BackendOptions.DefaultHistorySize, BackendOptions.DefaultMaxRooms);
        for (var offset = 0; offset < BackendOptions.DefaultHistorySize + 50; offset++)
        {
            history.Append(Record(offset, "general", $"nachricht {offset}"));
        }

        var backlog = history.Snapshot("general", afterOffset: -1);

        Assert.Equal(BackendOptions.DefaultHistorySize, backlog.Count);
        Assert.Equal($"nachricht {BackendOptions.DefaultHistorySize + 49}", backlog[^1].Message.Text);
    }

    /// <summary>
    /// The bound that was missing. <c>CHAT_HISTORY_SIZE</c> trims the queue inside a room; nothing
    /// ever trimmed the dictionary of rooms, and a room name arrives from a query string. Every
    /// replica holds every room it has ever seen, so the growth happens on all of them at once.
    /// </summary>
    [Fact]
    public void TheNumberOfRoomsIsBoundedByTheShippedDefault()
    {
        var history = new ChatHistory(
            BackendOptions.DefaultHistorySize, BackendOptions.DefaultMaxRooms);
        var rooms = BackendOptions.DefaultMaxRooms + 50;

        for (var offset = 0; offset < rooms; offset++)
        {
            history.Append(Record(offset, $"raum-{offset}", "hallo"));
        }

        Assert.Empty(history.Snapshot("raum-0", afterOffset: -1));
        Assert.NotEmpty(history.Snapshot($"raum-{rooms - 1}", afterOffset: -1));
    }

    /// <summary>
    /// Which room goes when a new one arrives: the one whose last message is oldest, not the one
    /// created first. A room that is still busy therefore survives rooms created after it.
    /// </summary>
    [Fact]
    public void TheRoomWithTheOldestLastMessageIsDroppedFirst()
    {
        var history = new ChatHistory(limit: 0, roomLimit: 2);
        history.Append(Record(0, "zuerst", "hallo"));
        history.Append(Record(1, "danach", "hallo"));

        // "zuerst" speaks again, so "danach" now holds the oldest message of the two.
        history.Append(Record(2, "zuerst", "immer noch da"));
        history.Append(Record(3, "neu", "hallo"));

        Assert.Empty(history.Snapshot("danach", afterOffset: -1));
        Assert.NotEmpty(history.Snapshot("zuerst", afterOffset: -1));
        Assert.NotEmpty(history.Snapshot("neu", afterOffset: -1));
    }

    /// <summary>
    /// A message in a room that is already held is not a new room, so it must not evict anything —
    /// otherwise a busy two-room chat would keep dropping the room it is not currently in.
    /// </summary>
    [Fact]
    public void AMessageInAKnownRoomEvictsNothing()
    {
        var history = new ChatHistory(limit: 0, roomLimit: 2);
        history.Append(Record(0, "eins", "hallo"));
        history.Append(Record(1, "zwei", "hallo"));

        history.Append(Record(2, "eins", "noch eine"));
        history.Append(Record(3, "eins", "und noch eine"));

        Assert.NotEmpty(history.Snapshot("zwei", afterOffset: -1));
    }

    /// <summary>Zero keeps the old behaviour available, as it does for the per-room limit.</summary>
    [Fact]
    public void ZeroMeansAnUnlimitedNumberOfRooms()
    {
        var history = new ChatHistory(limit: 0, roomLimit: 0);
        for (var offset = 0; offset < 1_000; offset++)
        {
            history.Append(Record(offset, $"raum-{offset}", "hallo"));
        }

        Assert.NotEmpty(history.Snapshot("raum-0", afterOffset: -1));
    }

    [Fact]
    public void TheLimitAppliesPerRoomAndNotAcrossThem()
    {
        var history = new ChatHistory(limit: 1, roomLimit: 0);
        history.Append(Record(0, "general", "hallo"));
        history.Append(Record(1, "andererraum", "geheim"));

        Assert.Equal("hallo", Assert.Single(history.Snapshot("general", -1)).Message.Text);
        Assert.Equal("geheim", Assert.Single(history.Snapshot("andererraum", -1)).Message.Text);
    }
}
