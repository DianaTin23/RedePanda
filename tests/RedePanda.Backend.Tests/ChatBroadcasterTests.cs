using RedePanda.Contracts;

namespace RedePanda.Backend.Tests;

/// <summary>
/// Room isolation is the behaviour the whole demo turns on, and the connection count is what the
/// active_connections metric reports, so both are pinned here. The backlog handed to a joining
/// subscriber is pinned alongside them: it travels the same room filter and must obey it.
/// </summary>
public class ChatBroadcasterTests
{
    private static ChatBroadcaster CreateBroadcaster(int historySize = 0) =>
        new(TestOptions.Create(historySize), new TestMeterFactory());

    private static ChatMessage Message(string room, string text) =>
        new(room, "alice", text, DateTimeOffset.UtcNow);

    [Fact]
    public void SubscriberReceivesMessagesForItsOwnRoom()
    {
        var broadcaster = CreateBroadcaster();
        using var subscription = broadcaster.Subscribe("general");

        broadcaster.Publish(Message("general", "hallo"), offset: 0);

        Assert.True(subscription.Reader.TryRead(out var received));
        Assert.Equal("hallo", received.Message.Text);
        Assert.Equal(0, received.Offset);
    }

    [Fact]
    public void SubscriberDoesNotReceiveOtherRooms()
    {
        var broadcaster = CreateBroadcaster();
        using var subscription = broadcaster.Subscribe("general");

        broadcaster.Publish(Message("andererraum", "geheim"), offset: 0);

        Assert.False(subscription.Reader.TryRead(out _));
    }

    [Fact]
    public void RoomMatchingIsCaseSensitive()
    {
        // Rooms are opaque identifiers, not display names; "General" is a different room.
        var broadcaster = CreateBroadcaster();
        using var subscription = broadcaster.Subscribe("general");

        broadcaster.Publish(Message("General", "hallo"), offset: 0);

        Assert.False(subscription.Reader.TryRead(out _));
    }

    [Fact]
    public void EverySubscriberInARoomGetsTheMessage()
    {
        var broadcaster = CreateBroadcaster();
        using var first = broadcaster.Subscribe("general");
        using var second = broadcaster.Subscribe("general");

        broadcaster.Publish(Message("general", "an alle"), offset: 0);

        Assert.True(first.Reader.TryRead(out _));
        Assert.True(second.Reader.TryRead(out _));
    }

    [Fact]
    public void JoiningASubscriberIsHandedWhatWasSaidBefore()
    {
        var broadcaster = CreateBroadcaster();
        broadcaster.Publish(Message("general", "erste"), offset: 0);
        broadcaster.Publish(Message("general", "zweite"), offset: 1);

        using var subscription = broadcaster.Subscribe("general");

        Assert.Equal(["erste", "zweite"], subscription.Backlog.Select(r => r.Message.Text));
    }

    [Fact]
    public void TheBacklogCarriesOnlyTheSubscribersOwnRoom()
    {
        var broadcaster = CreateBroadcaster();
        broadcaster.Publish(Message("general", "hallo"), offset: 0);
        broadcaster.Publish(Message("andererraum", "geheim"), offset: 1);

        using var subscription = broadcaster.Subscribe("general");

        Assert.Equal("hallo", Assert.Single(subscription.Backlog).Message.Text);
    }

    /// <summary>
    /// What a browser gets when EventSource reconnects with a <c>Last-Event-ID</c>: the part it
    /// missed, and not the part it already rendered.
    /// </summary>
    [Fact]
    public void ResumingFromAnOffsetLeavesOutWhatTheClientHasSeen()
    {
        var broadcaster = CreateBroadcaster();
        broadcaster.Publish(Message("general", "gesehen"), offset: 7);
        broadcaster.Publish(Message("general", "verpasst"), offset: 8);

        using var subscription = broadcaster.Subscribe("general", afterOffset: 7);

        Assert.Equal("verpasst", Assert.Single(subscription.Backlog).Message.Text);
    }

    [Fact]
    public void AHistorySizeCapsWhatAJoiningSubscriberSees()
    {
        var broadcaster = CreateBroadcaster(historySize: 1);
        broadcaster.Publish(Message("general", "alt"), offset: 0);
        broadcaster.Publish(Message("general", "neu"), offset: 1);

        using var subscription = broadcaster.Subscribe("general");

        Assert.Equal("neu", Assert.Single(subscription.Backlog).Message.Text);
    }

    [Fact]
    public void CountTracksOpenSubscriptionsAndReturnsToZero()
    {
        var broadcaster = CreateBroadcaster();
        Assert.Equal(0, broadcaster.Count);

        var first = broadcaster.Subscribe("general");
        var second = broadcaster.Subscribe("other");
        Assert.Equal(2, broadcaster.Count);

        first.Dispose();
        Assert.Equal(1, broadcaster.Count);

        second.Dispose();
        // Back to zero: this is why the metric is an observable instrument reading this
        // property rather than a counter incremented and decremented by hand.
        Assert.Equal(0, broadcaster.Count);
    }

    [Fact]
    public void PublishingToARoomWithNoSubscribersIsHarmless()
    {
        var broadcaster = CreateBroadcaster();

        broadcaster.Publish(Message("leer", "niemand da"), offset: 0);

        Assert.Equal(0, broadcaster.Count);
    }
}
