using System.Net.ServerSentEvents;
using Microsoft.Extensions.Logging.Abstractions;
using RedePanda.Contracts;

namespace RedePanda.Backend.Tests;

public class ChatStreamTests
{
    private static readonly TimeSpan NoHeartbeatDuringThisTest = TimeSpan.FromSeconds(30);

    private static readonly TimeSpan Promptly = TimeSpan.FromSeconds(5);

    private static ChatBroadcaster CreateBroadcaster()
    {
        var meterFactory = new TestMeterFactory();
        return new ChatBroadcaster(
            TestOptions.Create(),
            meterFactory,
            new ChatMetrics(meterFactory),
            NullLogger<ChatBroadcaster>.Instance);
    }

    private static ChatMessage Message(string room, string text) =>
        new(room, "alice", text, DateTimeOffset.UtcNow);

    private static IAsyncEnumerable<SseItem<string>> Stream(
        ChatBroadcaster broadcaster,
        string room,
        TimeSpan heartbeatInterval,
        CancellationToken ct,
        long lastEventId = -1) =>
        ChatStream.Create(
            broadcaster,
            room,
            heartbeatInterval,
            lastEventId,
            shutdown: CancellationToken.None,
            ct: ct);

    private static async Task<bool> MoveNextWithin(
        IAsyncEnumerator<SseItem<string>> stream, TimeSpan timeout, string because)
    {
        var moved = stream.MoveNextAsync().AsTask();
        var winner = await Task.WhenAny(moved, Task.Delay(timeout, CancellationToken.None));

        Assert.True(ReferenceEquals(winner, moved), because);
        return await moved;
    }

    [Fact]
    public async Task FirstItemArrivesWithoutWaitingForTheHeartbeat()
    {
        var broadcaster = CreateBroadcaster();
        using var cts = new CancellationTokenSource();

        await using var stream = Stream(broadcaster, "general", NoHeartbeatDuringThisTest, cts.Token)
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
        await foreach (var item in Stream(
                           broadcaster, "general", TimeSpan.FromMilliseconds(50), cts.Token))
        {
            items.Add(item);
            if (items.Count == 3)
            {
                break;
            }
        }

        Assert.Equal(3, items.Count);
        Assert.All(items, item => Assert.Equal(ChatStream.HeartbeatEventType, item.EventType));
        Assert.All(items, item => Assert.Equal(string.Empty, item.Data));
    }

    [Fact]
    public async Task PublishedMessageIsCarriedAsADefaultEvent()
    {
        var broadcaster = CreateBroadcaster();
        using var cts = new CancellationTokenSource();

        await using var stream = Stream(broadcaster, "general", NoHeartbeatDuringThisTest, cts.Token)
            .GetAsyncEnumerator(cts.Token);

        Assert.True(await MoveNextWithin(stream, Promptly, "The priming item never arrived."));

        broadcaster.Publish(Message("general", "hallo"), offset: 0);
        Assert.True(await MoveNextWithin(stream, Promptly, "The published message never arrived."));

        Assert.Equal("message", stream.Current.EventType);

        var received = ChatMessageSerializer.Deserialize(stream.Current.Data);
        Assert.NotNull(received);
        Assert.Equal("hallo", received.Text);
        Assert.Equal("general", received.Room);

        await cts.CancelAsync();
    }

    [Fact]
    public async Task TheRoomsHistoryIsReplayedBeforeAnythingLive()
    {
        var broadcaster = CreateBroadcaster();
        using var cts = new CancellationTokenSource();

        broadcaster.Publish(Message("general", "erste"), offset: 0);
        broadcaster.Publish(Message("general", "zweite"), offset: 1);

        await using var stream = Stream(broadcaster, "general", NoHeartbeatDuringThisTest, cts.Token)
            .GetAsyncEnumerator(cts.Token);

        Assert.True(await MoveNextWithin(stream, Promptly, "The priming item never arrived."));
        Assert.Equal(ChatStream.HeartbeatEventType, stream.Current.EventType);

        Assert.True(await MoveNextWithin(stream, Promptly, "The first history item never arrived."));
        Assert.Equal("erste", ChatMessageSerializer.Deserialize(stream.Current.Data)?.Text);

        Assert.True(await MoveNextWithin(stream, Promptly, "The second history item never arrived."));
        Assert.Equal("zweite", ChatMessageSerializer.Deserialize(stream.Current.Data)?.Text);

        broadcaster.Publish(Message("general", "live"), offset: 2);
        Assert.True(await MoveNextWithin(stream, Promptly, "The live message never arrived."));
        Assert.Equal("live", ChatMessageSerializer.Deserialize(stream.Current.Data)?.Text);

        await cts.CancelAsync();
    }

    [Fact]
    public async Task DataFramesCarryTheirOffsetAsTheEventId()
    {
        var broadcaster = CreateBroadcaster();
        using var cts = new CancellationTokenSource();

        await using var stream = Stream(broadcaster, "general", NoHeartbeatDuringThisTest, cts.Token)
            .GetAsyncEnumerator(cts.Token);

        Assert.True(await MoveNextWithin(stream, Promptly, "The priming item never arrived."));

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

        await using var stream =
            Stream(broadcaster, "general", NoHeartbeatDuringThisTest, cts.Token, lastEventId: 4)
                .GetAsyncEnumerator(cts.Token);

        Assert.True(await MoveNextWithin(stream, Promptly, "The priming item never arrived."));

        Assert.True(await MoveNextWithin(stream, Promptly, "The missed message never arrived."));
        Assert.Equal("verpasst", ChatMessageSerializer.Deserialize(stream.Current.Data)?.Text);

        var moved = stream.MoveNextAsync().AsTask();
        var winner = await Task.WhenAny(
            moved, Task.Delay(TimeSpan.FromMilliseconds(500), CancellationToken.None));

        Assert.False(ReferenceEquals(winner, moved), "The already-seen message was replayed.");

        await cts.CancelAsync();
        Assert.False(await moved, "The stream yielded another item after cancellation.");
    }

    [Fact]
    public async Task MessagesForOtherRoomsNeverReachTheStream()
    {
        var broadcaster = CreateBroadcaster();
        using var cts = new CancellationTokenSource();

        await using var stream = Stream(broadcaster, "general", NoHeartbeatDuringThisTest, cts.Token)
            .GetAsyncEnumerator(cts.Token);

        Assert.True(await MoveNextWithin(stream, Promptly, "The priming item never arrived."));

        broadcaster.Publish(Message("andererraum", "geheim"), offset: 0);

        var moved = stream.MoveNextAsync().AsTask();
        var winner = await Task.WhenAny(
            moved, Task.Delay(TimeSpan.FromMilliseconds(500), CancellationToken.None));

        Assert.False(ReferenceEquals(winner, moved), "A message leaked in from another room.");

        await cts.CancelAsync();
        Assert.False(await moved, "Cancelling the request must end the stream, not yield a frame.");
    }

    [Fact]
    public async Task DisposingTheStreamReleasesTheSubscription()
    {
        var broadcaster = CreateBroadcaster();
        using var cts = new CancellationTokenSource();
        Assert.Equal(0, broadcaster.Count);

        var stream = Stream(broadcaster, "general", NoHeartbeatDuringThisTest, cts.Token)
            .GetAsyncEnumerator(cts.Token);

        Assert.True(await MoveNextWithin(stream, Promptly, "The priming item never arrived."));
        Assert.Equal(1, broadcaster.Count);

        await stream.DisposeAsync();
        Assert.Equal(0, broadcaster.Count);
    }

    [Fact]
    public async Task ShutdownEndsTheStreamSoTheBrowserReconnectsElsewhere()
    {
        var broadcaster = CreateBroadcaster();
        using var request = new CancellationTokenSource();
        using var shutdown = new CancellationTokenSource();

        await using var stream = ChatStream
            .Create(
                broadcaster,
                "general",
                NoHeartbeatDuringThisTest,
                shutdown: shutdown.Token,
                ct: request.Token)
            .GetAsyncEnumerator(request.Token);

        Assert.True(await MoveNextWithin(stream, Promptly, "The priming item never arrived."));
        Assert.Equal(1, broadcaster.Count);

        var pending = stream.MoveNextAsync().AsTask();
        Assert.False(pending.IsCompleted, "The stream never reached its wait.");

        await shutdown.CancelAsync();

        var winner = await Task.WhenAny(pending, Task.Delay(Promptly, CancellationToken.None));
        var endedOnShutdown = ReferenceEquals(winner, pending);

        if (!endedOnShutdown)
        {
            await request.CancelAsync();
            await pending;
        }

        Assert.True(
            endedOnShutdown,
            $"The stream was still waiting {Promptly.TotalSeconds}s after the host started "
            + "stopping. A browser on this pod would sit on a dying connection instead of "
            + "reconnecting to the replica that is already ready.");

        Assert.False(await pending, "The stream yielded another frame after the host began stopping.");

        Assert.False(request.IsCancellationRequested);
        Assert.Equal(0, broadcaster.Count);
    }

    [Fact]
    public async Task CancellingTheRequestEndsTheStreamCleanly()
    {
        var broadcaster = CreateBroadcaster();
        using var cts = new CancellationTokenSource();

        var seen = 0;

        await foreach (var _ in Stream(
                           broadcaster, "general", TimeSpan.FromMilliseconds(50), cts.Token))
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
