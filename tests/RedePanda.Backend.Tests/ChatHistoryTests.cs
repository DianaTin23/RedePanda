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
        var history = new ChatHistory(limit: 0);
        history.Append(Record(0, "general", "erste"));
        history.Append(Record(1, "general", "zweite"));

        var backlog = history.Snapshot("general", afterOffset: -1);

        Assert.Equal(["erste", "zweite"], backlog.Select(r => r.Message.Text));
    }

    [Fact]
    public void SnapshotOfAnUnknownRoomIsEmpty()
    {
        var history = new ChatHistory(limit: 0);
        history.Append(Record(0, "general", "hallo"));

        Assert.Empty(history.Snapshot("nie-betreten", afterOffset: -1));
    }

    [Fact]
    public void RoomsAreKeptApart()
    {
        var history = new ChatHistory(limit: 0);
        history.Append(Record(0, "general", "hallo"));
        history.Append(Record(1, "andererraum", "geheim"));

        var backlog = history.Snapshot("general", afterOffset: -1);

        Assert.Equal("hallo", Assert.Single(backlog).Message.Text);
    }

    [Fact]
    public void RoomMatchingIsCaseSensitive()
    {
        // Same rule as the broadcaster: a room is an opaque identifier, not a display name.
        var history = new ChatHistory(limit: 0);
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
        var history = new ChatHistory(limit: 0);
        history.Append(Record(4, "general", "vorher"));
        history.Append(Record(5, "general", "gesehen"));
        history.Append(Record(6, "general", "verpasst"));

        var backlog = history.Snapshot("general", afterOffset: 5);

        Assert.Equal("verpasst", Assert.Single(backlog).Message.Text);
    }

    [Fact]
    public void ZeroMeansEverythingIsKept()
    {
        var history = new ChatHistory(limit: 0);
        for (var offset = 0; offset < 500; offset++)
        {
            history.Append(Record(offset, "general", $"nachricht {offset}"));
        }

        Assert.Equal(500, history.Snapshot("general", afterOffset: -1).Count);
    }

    [Fact]
    public void ALimitDropsTheOldestMessagesFirst()
    {
        var history = new ChatHistory(limit: 2);
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
        var history = new ChatHistory(BackendOptions.DefaultHistorySize);
        for (var offset = 0; offset < BackendOptions.DefaultHistorySize + 50; offset++)
        {
            history.Append(Record(offset, "general", $"nachricht {offset}"));
        }

        var backlog = history.Snapshot("general", afterOffset: -1);

        Assert.Equal(BackendOptions.DefaultHistorySize, backlog.Count);
        Assert.Equal($"nachricht {BackendOptions.DefaultHistorySize + 49}", backlog[^1].Message.Text);
    }

    [Fact]
    public void TheLimitAppliesPerRoomAndNotAcrossThem()
    {
        var history = new ChatHistory(limit: 1);
        history.Append(Record(0, "general", "hallo"));
        history.Append(Record(1, "andererraum", "geheim"));

        Assert.Equal("hallo", Assert.Single(history.Snapshot("general", -1)).Message.Text);
        Assert.Equal("geheim", Assert.Single(history.Snapshot("andererraum", -1)).Message.Text);
    }
}
