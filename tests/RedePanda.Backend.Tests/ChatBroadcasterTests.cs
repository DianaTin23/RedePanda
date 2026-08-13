using System.Threading.Channels;
using Microsoft.Extensions.Logging.Abstractions;
using RedePanda.Contracts;

namespace RedePanda.Backend.Tests;

/// <summary>
/// Room isolation is the behaviour the whole demo turns on, and the connection count is what the
/// active_connections metric reports, so both are pinned here. The backlog handed to a joining
/// subscriber is pinned alongside them: it travels the same room filter and must obey it.
/// </summary>
public class ChatBroadcasterTests
{
    private static ChatBroadcaster CreateBroadcaster(int historySize = 0)
    {
        // One factory for both: the broadcaster's observable instrument and the counters in
        // ChatMetrics belong to the same meter in the running application too.
        var meterFactory = new TestMeterFactory();
        return new ChatBroadcaster(
            TestOptions.Create(historySize),
            meterFactory,
            new ChatMetrics(meterFactory),
            NullLogger<ChatBroadcaster>.Instance);
    }

    private static ChatMessage Message(string room, string text) =>
        new(room, "alice", text, DateTimeOffset.UtcNow);

    /// <summary>
    /// A browser that stops reading used to lose messages silently. The buffer dropped its oldest
    /// entry to make room, nothing was logged, no metric moved, and the connection stayed open —
    /// so the client had a hole in its history and no way to find out, because the offsets it did
    /// receive still increased and its resume filter had nothing to catch.
    /// <para>
    /// Ending the stream is what makes the same overflow recoverable: <c>ChatStream</c> stops on a
    /// completed channel, <c>EventSource</c> reconnects with <c>Last-Event-ID</c>, and the replay
    /// covers exactly the gap. The assertion that matters is the first one in the loop — under the
    /// old behaviour the oldest record was the one thrown away, so reading would have started at
    /// offset 1.
    /// </para>
    /// </summary>
    [Fact]
    public async Task ASubscriberThatFallsBehindIsCutOffRatherThanQuietlyLosingMessages()
    {
        var broadcaster = CreateBroadcaster();
        using var subscription = broadcaster.Subscribe("general");

        // One past what the buffer holds: every message up to the last one fits, and that last
        // one has nowhere to go.
        for (var offset = 0; offset <= ChatBroadcaster.SubscriberBufferSize; offset++)
        {
            broadcaster.Publish(Message("general", $"nachricht-{offset}"), offset);
        }

        for (var offset = 0; offset < ChatBroadcaster.SubscriberBufferSize; offset++)
        {
            var received = await subscription.Reader.ReadAsync(TestContext.Current.CancellationToken);
            Assert.Equal(offset, received.Offset);
        }

        // Not "no more messages for now" — the stream is over, which is the signal the browser
        // needs in order to reconnect and ask for the rest.
        await Assert.ThrowsAsync<ChannelClosedException>(
            async () => await subscription.Reader.ReadAsync(TestContext.Current.CancellationToken));
    }

    /// <summary>
    /// The cut must not become its own problem: a subscriber that has been ended and not yet
    /// disposed still sits in the fan-out, and re-completing it once per message would log and
    /// count on every publish for the rest of the connection's life.
    /// </summary>
    [Fact]
    public void CuttingASubscriberOffHappensOnceAndNotPerMessage()
    {
        var broadcaster = CreateBroadcaster();
        using var subscription = broadcaster.Subscribe("general");

        for (var offset = 0; offset <= ChatBroadcaster.SubscriberBufferSize + 50; offset++)
        {
            broadcaster.Publish(Message("general", $"nachricht-{offset}"), offset);
        }

        // Still exactly one subscriber, still one completed channel: publishing past the cut is a
        // no-op rather than a repeated event.
        Assert.Equal(1, broadcaster.Count);
        Assert.False(subscription.Reader.Completion.IsFaulted);
    }

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
