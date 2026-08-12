using System.Net.ServerSentEvents;
using RedePanda.Contracts;

namespace RedePanda.Backend.Tests;

/// <summary>
/// The SSE stream is what every browser hangs off, and none of it is reachable through the
/// broadcaster tests, so its wire-level contract is pinned here: when the first item appears,
/// what a heartbeat looks like, what a message looks like, and that a closed connection stops
/// counting towards <c>redepanda.active_connections</c>.
/// <para>
/// Every wait in here is bounded. An unbounded <c>await MoveNextAsync()</c> would turn a broken
/// stream into a test run that hangs for a whole heartbeat interval instead of failing.
/// </para>
/// </summary>
public class ChatStreamTests
{
    /// <summary>
    /// Long enough that a test reaching it has certainly gone wrong, short enough that such a test
    /// still fails in reasonable time.
    /// </summary>
    private static readonly TimeSpan NoHeartbeatDuringThisTest = TimeSpan.FromSeconds(30);

    /// <summary>How long a genuinely prompt item may take before the test calls it a failure.</summary>
    private static readonly TimeSpan Promptly = TimeSpan.FromSeconds(5);

    private static ChatBroadcaster CreateBroadcaster() =>
        new(TestOptions.Create(), new TestMeterFactory());

    private static ChatMessage Message(string room, string text) =>
        new(room, "alice", text, DateTimeOffset.UtcNow);

    /// <summary>Advances the stream, failing with <paramref name="because"/> if it stalls.</summary>
    private static async Task<bool> MoveNextWithin(
        IAsyncEnumerator<SseItem<string>> stream, TimeSpan timeout, string because)
    {
        var moved = stream.MoveNextAsync().AsTask();
        var winner = await Task.WhenAny(moved, Task.Delay(timeout, CancellationToken.None));

        Assert.True(ReferenceEquals(winner, moved), because);
        return await moved;
    }

    /// <summary>
    /// The regression that the move to <c>TypedResults.ServerSentEvents</c> introduced: the result
    /// writes nothing at all — not even the status line — until the first item is yielded, so a
    /// quiet room left the browser in EventSource.CONNECTING for a whole heartbeat interval. The
    /// hand-written predecessor flushed the headers before its loop; the stream primes itself now.
    /// </summary>
    [Fact]
    public async Task FirstItemArrivesWithoutWaitingForTheHeartbeat()
    {
        var broadcaster = CreateBroadcaster();
        using var cts = new CancellationTokenSource();

        await using var stream = ChatStream
            .Create(broadcaster, "general", NoHeartbeatDuringThisTest, ct: cts.Token)
            .GetAsyncEnumerator(cts.Token);

        Assert.True(
            await MoveNextWithin(
                stream,
                Promptly,
                $"Nothing was yielded within {Promptly.TotalSeconds}s. The stream is waiting for "
                + "the heartbeat before it writes anything, so the response headers never reach "
                + "the browser and EventSource stays in CONNECTING."));

        Assert.Equal(ChatStream.HeartbeatEventType, stream.Current.EventType);

        await cts.CancelAsync();
    }

    [Fact]
    public async Task HeartbeatsKeepComingWhileTheRoomStaysQuiet()
    {
        var broadcaster = CreateBroadcaster();
        using var cts = new CancellationTokenSource(TimeSpan.FromSeconds(30));

        var items = new List<SseItem<string>>();
        await foreach (var item in ChatStream.Create(
                           broadcaster, "general", TimeSpan.FromMilliseconds(50), ct: cts.Token))
        {
            items.Add(item);
            if (items.Count == 3)
            {
                break;
            }
        }

        // Nothing was ever published, so every item is a heartbeat: the priming one and two more.
        Assert.Equal(3, items.Count);
        Assert.All(items, item => Assert.Equal(ChatStream.HeartbeatEventType, item.EventType));
        Assert.All(items, item => Assert.Equal(string.Empty, item.Data));
    }

    [Fact]
    public async Task PublishedMessageIsCarriedAsADefaultEvent()
    {
        var broadcaster = CreateBroadcaster();
        using var cts = new CancellationTokenSource();

        await using var stream = ChatStream
            .Create(broadcaster, "general", NoHeartbeatDuringThisTest, ct: cts.Token)
            .GetAsyncEnumerator(cts.Token);

        // Consuming the priming item first is what guarantees the subscription exists; the stream
        // only subscribes once it is enumerated, so publishing before this would race.
        Assert.True(await MoveNextWithin(stream, Promptly, "The priming item never arrived."));

        broadcaster.Publish(Message("general", "hallo"), offset: 0);
        Assert.True(await MoveNextWithin(stream, Promptly, "The published message never arrived."));

        // "message" is the default SSE event type and the only one EventSource.onmessage fires
        // for. A heartbeat is deliberately *not* that type, which is what keeps it invisible to
        // the frontend.
        Assert.Equal("message", stream.Current.EventType);

        var received = ChatMessageSerializer.Deserialize(stream.Current.Data);
        Assert.NotNull(received);
        Assert.Equal("hallo", received.Text);
        Assert.Equal("general", received.Room);

        await cts.CancelAsync();
    }

    /// <summary>
    /// What a browser sees on join: the room as it stands, before anything live. The frontend
    /// renders append-only, so the order these arrive in is the order they appear on screen.
    /// </summary>
    [Fact]
    public async Task TheRoomsHistoryIsReplayedBeforeAnythingLive()
    {
        var broadcaster = CreateBroadcaster();
        using var cts = new CancellationTokenSource();

        broadcaster.Publish(Message("general", "erste"), offset: 0);
        broadcaster.Publish(Message("general", "zweite"), offset: 1);

        await using var stream = ChatStream
            .Create(broadcaster, "general", NoHeartbeatDuringThisTest, ct: cts.Token)
            .GetAsyncEnumerator(cts.Token);

        // The priming heartbeat still comes first — it is what flushes the response headers.
        Assert.True(await MoveNextWithin(stream, Promptly, "The priming item never arrived."));
        Assert.Equal(ChatStream.HeartbeatEventType, stream.Current.EventType);

        Assert.True(await MoveNextWithin(stream, Promptly, "The first history item never arrived."));
        Assert.Equal("erste", ChatMessageSerializer.Deserialize(stream.Current.Data)?.Text);

        Assert.True(await MoveNextWithin(stream, Promptly, "The second history item never arrived."));
        Assert.Equal("zweite", ChatMessageSerializer.Deserialize(stream.Current.Data)?.Text);

        // And only then the live tail.
        broadcaster.Publish(Message("general", "live"), offset: 2);
        Assert.True(await MoveNextWithin(stream, Promptly, "The live message never arrived."));
        Assert.Equal("live", ChatMessageSerializer.Deserialize(stream.Current.Data)?.Text);

        await cts.CancelAsync();
    }

    /// <summary>
    /// The id is the Kafka offset, and it is the whole mechanism behind resuming a dropped
    /// connection: the browser sends the last one back as <c>Last-Event-ID</c>.
    /// </summary>
    [Fact]
    public async Task DataFramesCarryTheirOffsetAsTheEventId()
    {
        var broadcaster = CreateBroadcaster();
        using var cts = new CancellationTokenSource();

        await using var stream = ChatStream
            .Create(broadcaster, "general", NoHeartbeatDuringThisTest, ct: cts.Token)
            .GetAsyncEnumerator(cts.Token);

        Assert.True(await MoveNextWithin(stream, Promptly, "The priming item never arrived."));

        // A heartbeat must not carry an id, or it would push the client's resume point past
        // messages it never received.
        Assert.Null(stream.Current.EventId);

        broadcaster.Publish(Message("general", "hallo"), offset: 42);
        Assert.True(await MoveNextWithin(stream, Promptly, "The published message never arrived."));

        Assert.Equal("42", stream.Current.EventId);

        await cts.CancelAsync();
    }

    [Fact]
    public async Task AResumingClientOnlyGetsWhatItMissed()
    {
        var broadcaster = CreateBroadcaster();
        using var cts = new CancellationTokenSource();

        broadcaster.Publish(Message("general", "gesehen"), offset: 4);
        broadcaster.Publish(Message("general", "verpasst"), offset: 5);

        await using var stream = ChatStream
            .Create(broadcaster, "general", NoHeartbeatDuringThisTest, lastEventId: 4, ct: cts.Token)
            .GetAsyncEnumerator(cts.Token);

        Assert.True(await MoveNextWithin(stream, Promptly, "The priming item never arrived."));

        Assert.True(await MoveNextWithin(stream, Promptly, "The missed message never arrived."));
        Assert.Equal("verpasst", ChatMessageSerializer.Deserialize(stream.Current.Data)?.Text);

        // Nothing else: had "gesehen" been replayed it would have come before this one, and the
        // stream is now back to waiting for the far-off heartbeat.
        var moved = stream.MoveNextAsync().AsTask();
        var winner = await Task.WhenAny(
            moved, Task.Delay(TimeSpan.FromMilliseconds(500), CancellationToken.None));

        Assert.False(ReferenceEquals(winner, moved), "The already-seen message was replayed.");

        // Awaited rather than abandoned: disposing the enumerator while a MoveNextAsync is still
        // in flight throws NotSupportedException, and cancelling alone does not guarantee the
        // pending one has observed it yet.
        await cts.CancelAsync();
        Assert.False(await moved, "The stream yielded another item after cancellation.");
    }

    [Fact]
    public async Task MessagesForOtherRoomsNeverReachTheStream()
    {
        var broadcaster = CreateBroadcaster();
        using var cts = new CancellationTokenSource();

        await using var stream = ChatStream
            .Create(broadcaster, "general", NoHeartbeatDuringThisTest, ct: cts.Token)
            .GetAsyncEnumerator(cts.Token);

        Assert.True(await MoveNextWithin(stream, Promptly, "The priming item never arrived."));

        broadcaster.Publish(Message("andererraum", "geheim"), offset: 0);

        // The heartbeat is far out and the message belongs to another room, so nothing should
        // arrive at all. A short window is enough: delivery is in-memory and immediate.
        var moved = stream.MoveNextAsync().AsTask();
        var winner = await Task.WhenAny(
            moved, Task.Delay(TimeSpan.FromMilliseconds(500), CancellationToken.None));

        Assert.False(ReferenceEquals(winner, moved), "A message leaked in from another room.");

        await cts.CancelAsync();
    }

    /// <summary>
    /// Pins the other half of the active-connections metric: <see cref="ChatBroadcaster.Count"/>
    /// only returns to zero if the stream disposes its subscription when the browser goes away.
    /// </summary>
    [Fact]
    public async Task DisposingTheStreamReleasesTheSubscription()
    {
        var broadcaster = CreateBroadcaster();
        using var cts = new CancellationTokenSource();
        Assert.Equal(0, broadcaster.Count);

        var stream = ChatStream
            .Create(broadcaster, "general", NoHeartbeatDuringThisTest, ct: cts.Token)
            .GetAsyncEnumerator(cts.Token);

        // The subscription is taken on first enumeration, not at Create.
        Assert.True(await MoveNextWithin(stream, Promptly, "The priming item never arrived."));
        Assert.Equal(1, broadcaster.Count);

        await stream.DisposeAsync();
        Assert.Equal(0, broadcaster.Count);
    }

    [Fact]
    public async Task CancellingTheRequestEndsTheStreamCleanly()
    {
        var broadcaster = CreateBroadcaster();
        using var cts = new CancellationTokenSource();

        var seen = 0;

        // A browser navigating away cancels RequestAborted mid-wait. That must end the enumeration
        // normally rather than surface as an unhandled OperationCanceledException.
        await foreach (var _ in ChatStream.Create(
                           broadcaster, "general", TimeSpan.FromMilliseconds(50), ct: cts.Token))
        {
            if (++seen == 2)
            {
                await cts.CancelAsync();
            }
        }

        Assert.Equal(2, seen);
        Assert.Equal(0, broadcaster.Count);
    }
}
