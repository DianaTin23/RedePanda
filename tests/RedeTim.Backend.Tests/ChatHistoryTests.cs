using RedeTim.Contracts;

namespace RedeTim.Backend.Tests;

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
        var history = new ChatHistory(limit: 0, roomLimit: 0);
        history.Append(Record(0, "General", "hallo"));

        Assert.Empty(history.Snapshot("general", afterOffset: -1));
    }

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

    [Fact]
    public void TheRoomWithTheOldestLastMessageIsDroppedFirst()
    {
        var history = new ChatHistory(limit: 0, roomLimit: 2);
        history.Append(Record(0, "zuerst", "hallo"));
        history.Append(Record(1, "danach", "hallo"));

        history.Append(Record(2, "zuerst", "immer noch da"));
        history.Append(Record(3, "neu", "hallo"));

        Assert.Empty(history.Snapshot("danach", afterOffset: -1));
        Assert.NotEmpty(history.Snapshot("zuerst", afterOffset: -1));
        Assert.NotEmpty(history.Snapshot("neu", afterOffset: -1));
    }

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
