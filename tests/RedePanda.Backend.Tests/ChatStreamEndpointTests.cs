using System.Net;
using Microsoft.AspNetCore.Hosting;
using Microsoft.AspNetCore.Mvc.Testing;
using Microsoft.Extensions.DependencyInjection;
using RedePanda.Contracts;

namespace RedePanda.Backend.Tests;

/// <summary>
/// Drives <c>GET /api/stream</c> over real HTTP.
/// <para>
/// The unit tests around <see cref="ChatStream"/> pin our own iterator; these pin what
/// <c>TypedResults.ServerSentEvents</c> actually puts on the wire. The distinction matters: the
/// stall this suite exists to prevent came from the framework writing nothing until the first
/// item, not from a bug in the iterator, so a future ASP.NET Core upgrade could reintroduce it
/// without any of our code changing.
/// </para>
/// </summary>
public class ChatStreamEndpointTests : IClassFixture<ChatStreamEndpointTests.BrokerlessBackend>
{
    /// <summary>How long the first frame may take before the test calls it a stall.</summary>
    private static readonly TimeSpan Promptly = TimeSpan.FromSeconds(5);

    private readonly BrokerlessBackend _factory;

    public ChatStreamEndpointTests(BrokerlessBackend factory) => _factory = factory;

    /// <summary>
    /// Boots the real application minus the Kafka consumer. The stream endpoint reads from the
    /// <see cref="ChatBroadcaster"/> and never touches a broker, so dropping the consumer keeps
    /// the test hermetic and the output free of connection warnings.
    /// </summary>
    public sealed class BrokerlessBackend : WebApplicationFactory<Program>
    {
        protected override void ConfigureWebHost(IWebHostBuilder builder)
        {
            builder.ConfigureServices(services =>
            {
                var consumer = services.SingleOrDefault(
                    d => d.ImplementationType == typeof(ChatConsumerService));

                if (consumer is not null)
                {
                    services.Remove(consumer);
                }
            });
        }
    }

    [Fact]
    public async Task MissingRoomIsRejected()
    {
        using var client = _factory.CreateClient();

        using var response = await client.GetAsync("/api/stream", TestContext.Current.CancellationToken);

        Assert.Equal(HttpStatusCode.BadRequest, response.StatusCode);
        var body = await response.Content.ReadAsStringAsync(TestContext.Current.CancellationToken);
        Assert.Contains("'room' is required", body);
    }

    /// <summary>
    /// The regression test proper: on a silent room a frame must reach the wire without waiting
    /// out the 15 s production heartbeat, or a browser sits in EventSource.CONNECTING for that
    /// whole interval.
    /// </summary>
    [Fact]
    public async Task FirstFrameArrivesBeforeAnyMessageIsPublished()
    {
        using var client = _factory.CreateClient();
        using var abort = new CancellationTokenSource(TimeSpan.FromSeconds(30));

        var connection = await Connect(client, "stille", abort.Token, "on a silent room");
        using var response = connection.Response;
        using var reader = connection.Reader;

        Assert.Equal($"event: {ChatStream.HeartbeatEventType}", connection.FirstLine);
    }

    [Fact]
    public async Task ResponseHeadersMarkTheStreamUncachedAndUnbuffered()
    {
        using var client = _factory.CreateClient();
        using var abort = new CancellationTokenSource(TimeSpan.FromSeconds(30));

        using var response = await client.GetAsync(
            "/api/stream?room=header-probe",
            HttpCompletionOption.ResponseHeadersRead,
            abort.Token);

        Assert.Equal("text/event-stream", response.Content.Headers.ContentType?.MediaType);

        // A cached or re-encoded event stream is a broken event stream. Asserted on the parsed
        // directives rather than the raw string, which differs only in whitespace.
        Assert.True(response.Headers.CacheControl?.NoCache, "Cache-Control is missing no-cache.");
        Assert.True(response.Headers.CacheControl?.NoStore, "Cache-Control is missing no-store.");
        Assert.Contains("identity", response.Content.Headers.ContentEncoding);

        // Ours, not the framework's: nginx buffers proxied responses unless told otherwise.
        Assert.Equal("no", Assert.Single(response.Headers.GetValues("X-Accel-Buffering")));
    }

    [Fact]
    public async Task PublishedMessageIsWrittenAsADataFrame()
    {
        using var client = _factory.CreateClient();
        using var abort = new CancellationTokenSource(TimeSpan.FromSeconds(30));

        var connection = await Connect(client, "general", abort.Token, "before publishing");
        using var response = connection.Response;
        using var reader = connection.Reader;

        // Having read the priming frame proves the subscription is in place, so the publish below
        // cannot race the subscribe.
        Assert.Equal($"event: {ChatStream.HeartbeatEventType}", connection.FirstLine);
        Assert.Equal("data: ", await ReadLineWithin(reader, abort.Token, "priming frame truncated"));
        Assert.Equal(string.Empty, await ReadLineWithin(reader, abort.Token, "priming frame truncated"));

        Publish("general", "hallo", offset: 0);

        var frame = await ReadLineWithin(reader, abort.Token, "the published message never arrived");

        // No "event:" line at all — that is what makes it the default type, and the only shape
        // EventSource.onmessage will hand to the frontend.
        Assert.StartsWith("data: ", frame);

        var received = ChatMessageSerializer.Deserialize(frame["data: ".Length..]);
        Assert.NotNull(received);
        Assert.Equal("hallo", received.Text);
        Assert.Equal("general", received.Room);
    }

    /// <summary>
    /// The history a browser gets on join, on the wire. The <c>id:</c> line is the half of the
    /// contract the browser fulfils itself: it echoes the last one back as <c>Last-Event-ID</c>.
    /// </summary>
    [Fact]
    public async Task JoiningARoomReplaysWhatWasSaidInIt()
    {
        using var client = _factory.CreateClient();
        using var abort = new CancellationTokenSource(TimeSpan.FromSeconds(30));

        Publish("verlauf", "vorher gesagt", offset: 11);

        var connection = await Connect(client, "verlauf", abort.Token, "on a room with history");
        using var response = connection.Response;
        using var reader = connection.Reader;

        Assert.Equal($"event: {ChatStream.HeartbeatEventType}", connection.FirstLine);
        Assert.Equal("data: ", await ReadLineWithin(reader, abort.Token, "priming frame truncated"));
        Assert.Equal(string.Empty, await ReadLineWithin(reader, abort.Token, "priming frame truncated"));

        // Asserted as a set of lines rather than in a fixed order: the order of "data:" and "id:"
        // within one frame is the framework's business, not a promise this endpoint makes.
        var frame = await ReadFrameWithin(reader, abort.Token, "the replayed message never arrived");

        Assert.Contains("id: 11", frame);

        var data = Assert.Single(frame, line => line.StartsWith("data: ", StringComparison.Ordinal));
        Assert.Equal("vorher gesagt", ChatMessageSerializer.Deserialize(data["data: ".Length..])?.Text);
    }

    /// <summary>
    /// What the pod-delete demo depends on: EventSource reconnects on its own and resends the last
    /// id it saw, and the room must not arrive a second time on top of what is already rendered.
    /// </summary>
    [Fact]
    public async Task AReconnectWithLastEventIdDoesNotRepeatTheRoom()
    {
        using var client = _factory.CreateClient();
        using var abort = new CancellationTokenSource(TimeSpan.FromSeconds(30));

        Publish("wiederaufnahme", "gesehen", offset: 20);
        Publish("wiederaufnahme", "verpasst", offset: 21);

        using var request = new HttpRequestMessage(
            HttpMethod.Get, "/api/stream?room=wiederaufnahme");

        // TryAddWithoutValidation: Last-Event-ID is not one of HttpClient's known headers.
        request.Headers.TryAddWithoutValidation("Last-Event-ID", "20");

        using var response = await client.SendAsync(
            request, HttpCompletionOption.ResponseHeadersRead, abort.Token);

        Assert.Equal(HttpStatusCode.OK, response.StatusCode);

        using var reader = new StreamReader(await response.Content.ReadAsStreamAsync(abort.Token));

        Assert.Equal(
            $"event: {ChatStream.HeartbeatEventType}",
            await ReadLineWithin(reader, abort.Token, "the priming frame never arrived"));
        Assert.Equal("data: ", await ReadLineWithin(reader, abort.Token, "priming frame truncated"));
        Assert.Equal(string.Empty, await ReadLineWithin(reader, abort.Token, "priming frame truncated"));

        // The first thing after the priming frame is the message the client missed. If the already
        // seen one were replayed it would have to appear here instead, because the backlog is
        // ordered by offset.
        var frame = await ReadFrameWithin(reader, abort.Token, "the missed message never arrived");

        Assert.Contains("id: 21", frame);

        var data = Assert.Single(frame, line => line.StartsWith("data: ", StringComparison.Ordinal));
        Assert.Equal("verpasst", ChatMessageSerializer.Deserialize(data["data: ".Length..])?.Text);
    }

    /// <summary>
    /// The half of the resume contract the browser cannot fulfil. After a <em>fatal</em>
    /// EventSource error the frontend opens a brand-new EventSource, and no JS API can put
    /// <c>Last-Event-ID</c> on one — so the server sees a first-time client and replays the whole
    /// room. The frontend drops what it has already rendered by comparing SSE ids, which only works
    /// if they arrive strictly increasing. That precondition is what this pins.
    /// </summary>
    [Fact]
    public async Task AFreshConnectionReplaysTheRoomWithStrictlyIncreasingIds()
    {
        using var client = _factory.CreateClient();
        using var abort = new CancellationTokenSource(TimeSpan.FromSeconds(30));

        Publish("ohne-header", "erste", offset: 30);
        Publish("ohne-header", "zweite", offset: 31);
        Publish("ohne-header", "dritte", offset: 32);

        // No Last-Event-ID at all: exactly what a new EventSource sends.
        var connection = await Connect(client, "ohne-header", abort.Token, "on a fresh connection");
        using var response = connection.Response;
        using var reader = connection.Reader;

        Assert.Equal($"event: {ChatStream.HeartbeatEventType}", connection.FirstLine);
        Assert.Equal("data: ", await ReadLineWithin(reader, abort.Token, "priming frame truncated"));
        Assert.Equal(string.Empty, await ReadLineWithin(reader, abort.Token, "priming frame truncated"));

        var ids = new List<long>();
        foreach (var expected in new[] { "erste", "zweite", "dritte" })
        {
            var frame = await ReadFrameWithin(
                reader, abort.Token, $"the replayed message '{expected}' never arrived");

            var id = Assert.Single(frame, line => line.StartsWith("id: ", StringComparison.Ordinal));
            ids.Add(long.Parse(id["id: ".Length..]));

            var data = Assert.Single(frame, line => line.StartsWith("data: ", StringComparison.Ordinal));
            Assert.Equal(expected, ChatMessageSerializer.Deserialize(data["data: ".Length..])?.Text);
        }

        Assert.Equal([30L, 31L, 32L], ids);
        Assert.Equal(ids.Order(), ids);
    }

    private void Publish(string room, string text, long offset) =>
        _factory.Services
            .GetRequiredService<ChatBroadcaster>()
            .Publish(new ChatMessage(room, "alice", text, DateTimeOffset.UtcNow), offset);

    private sealed record Connection(
        HttpResponseMessage Response, StreamReader Reader, string? FirstLine);

    /// <summary>
    /// Opens the stream and reads its first line, failing if that takes longer than
    /// <see cref="Promptly"/>.
    /// <para>
    /// The bound covers the whole open-and-read rather than just the read. TestServer hands back
    /// the response head as soon as the handler returns the result, and it is
    /// <c>ReadAsStreamAsync</c> that blocks until the first body bytes exist — so bounding only
    /// the read leaves the stall outside the measurement and passes a stalled stream.
    /// </para>
    /// </summary>
    private static async Task<Connection> Connect(
        HttpClient client, string room, CancellationToken ct, string when)
    {
        var connecting = Open(client, room, ct);
        var winner = await Task.WhenAny(connecting, Task.Delay(Promptly, CancellationToken.None));

        Assert.True(
            ReferenceEquals(winner, connecting),
            $"No frame reached the wire within {Promptly.TotalSeconds}s {when}. The stream is "
            + "waiting for the heartbeat before it writes anything, so the browser stays in "
            + "EventSource.CONNECTING that long.");

        return await connecting;

        static async Task<Connection> Open(HttpClient client, string room, CancellationToken ct)
        {
            var response = await client.GetAsync(
                $"/api/stream?room={room}", HttpCompletionOption.ResponseHeadersRead, ct);

            Assert.Equal(HttpStatusCode.OK, response.StatusCode);

            var body = await response.Content.ReadAsStreamAsync(ct);
            var reader = new StreamReader(body);

            return new Connection(response, reader, await reader.ReadLineAsync(ct));
        }
    }

    /// <summary>
    /// Reads one whole SSE frame — every line up to the blank line that terminates it.
    /// </summary>
    private static async Task<List<string>> ReadFrameWithin(
        StreamReader reader, CancellationToken ct, string because)
    {
        var lines = new List<string>();

        while (true)
        {
            var line = await ReadLineWithin(reader, ct, because);
            if (line.Length == 0)
            {
                return lines;
            }

            lines.Add(line);
        }
    }

    /// <summary>
    /// Reads one further line from an already-flowing stream. Bounded so a stalled stream reports
    /// the stall rather than hanging until the test's own cancellation fires.
    /// </summary>
    private static async Task<string> ReadLineWithin(
        StreamReader reader, CancellationToken ct, string because)
    {
        var read = reader.ReadLineAsync(ct).AsTask();
        var winner = await Task.WhenAny(read, Task.Delay(Promptly, CancellationToken.None));

        Assert.True(ReferenceEquals(winner, read), because);

        var line = await read;
        Assert.NotNull(line);
        return line;
    }
}
