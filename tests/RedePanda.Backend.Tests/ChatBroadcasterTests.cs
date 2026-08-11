using System.Diagnostics.Metrics;
using RedePanda.Contracts;

namespace RedePanda.Backend.Tests;

/// <summary>
/// Room isolation is the behaviour the whole demo turns on, and the connection count is what the
/// active_connections metric reports, so both are pinned here.
/// </summary>
public class ChatBroadcasterTests
{
    /// <summary>Minimal stand-in so the tests need no DI container.</summary>
    private sealed class TestMeterFactory : IMeterFactory
    {
        private readonly List<Meter> _meters = [];

        public Meter Create(MeterOptions options)
        {
            var meter = new Meter(options.Name, options.Version, options.Tags, scope: this);
            _meters.Add(meter);
            return meter;
        }

        public void Dispose()
        {
            foreach (var meter in _meters)
            {
                meter.Dispose();
            }
        }
    }

    private static ChatBroadcaster CreateBroadcaster() => new(new TestMeterFactory());

    private static ChatMessage Message(string room, string text) =>
        new(room, "alice", text, DateTimeOffset.UtcNow);

    [Fact]
    public void SubscriberReceivesMessagesForItsOwnRoom()
    {
        var broadcaster = CreateBroadcaster();
        using var subscription = broadcaster.Subscribe("general");

        broadcaster.Publish(Message("general", "hallo"));

        Assert.True(subscription.Reader.TryRead(out var received));
        Assert.Equal("hallo", received.Text);
    }

    [Fact]
    public void SubscriberDoesNotReceiveOtherRooms()
    {
        var broadcaster = CreateBroadcaster();
        using var subscription = broadcaster.Subscribe("general");

        broadcaster.Publish(Message("andererraum", "geheim"));

        Assert.False(subscription.Reader.TryRead(out _));
    }

    [Fact]
    public void RoomMatchingIsCaseSensitive()
    {
        // Rooms are opaque identifiers, not display names; "General" is a different room.
        var broadcaster = CreateBroadcaster();
        using var subscription = broadcaster.Subscribe("general");

        broadcaster.Publish(Message("General", "hallo"));

        Assert.False(subscription.Reader.TryRead(out _));
    }

    [Fact]
    public void EverySubscriberInARoomGetsTheMessage()
    {
        var broadcaster = CreateBroadcaster();
        using var first = broadcaster.Subscribe("general");
        using var second = broadcaster.Subscribe("general");

        broadcaster.Publish(Message("general", "an alle"));

        Assert.True(first.Reader.TryRead(out _));
        Assert.True(second.Reader.TryRead(out _));
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

        broadcaster.Publish(Message("leer", "niemand da"));

        Assert.Equal(0, broadcaster.Count);
    }
}
