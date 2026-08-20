using System.Net;
using Microsoft.AspNetCore.Hosting;
using Microsoft.AspNetCore.Mvc.Testing;
using Microsoft.Extensions.DependencyInjection;
using RedeTim.Contracts;

namespace RedeTim.Backend.Tests;

public class ChatStreamEndpointTests : IClassFixture<ChatStreamEndpointTests.BrokerlessBackend>
{
    private static readonly TimeSpan Promptly = TimeSpan.FromSeconds(5);

    private readonly BrokerlessBackend _factory;

    public ChatStreamEndpointTests(BrokerlessBackend factory) => _factory = factory;

    public sealed class BrokerlessBackend : WebApplicationFactory<Program>
    {
        protected override void ConfigureWebHost(IWebHostBuilder builder)
        {
            builder.ConfigureServices(services =>
            {
                foreach (var type in new[] { typeof(ChatConsumerService), typeof(PresenceConsumerService) })
                {
                    var descriptor = services.SingleOrDefault(d => d.ImplementationType == type);
                    if (descriptor is not null)
                    {
                        services.Remove(descriptor);
                    }
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

        Assert.True(response.Headers.CacheControl?.NoCache, "Cache-Control is missing no-cache.");
        Assert.True(response.Headers.CacheControl?.NoStore, "Cache-Control is missing no-store.");
        Assert.Contains("identity", response.Content.Headers.ContentEncoding);

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

        Assert.Equal($"event: {ChatStream.HeartbeatEventType}", connection.FirstLine);
        Assert.Equal("data: ", await ReadLineWithin(reader, abort.Token, "priming frame truncated"));
        Assert.Equal(string.Empty, await ReadLineWithin(reader, abort.Token, "priming frame truncated"));

        Publish("general", "hallo", offset: 0);

        var frame = await ReadLineWithin(reader, abort.Token, "the published message never arrived");

        Assert.StartsWith("data: ", frame);

        var received = ChatMessageSerializer.Deserialize(frame["data: ".Length..]);
        Assert.NotNull(received);
        Assert.Equal("hallo", received.Text);
        Assert.Equal("general", received.Room);
    }

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

        var frame = await ReadFrameWithin(reader, abort.Token, "the replayed message never arrived");

        Assert.Contains("id: 11", frame);

        var data = Assert.Single(frame, line => line.StartsWith("data: ", StringComparison.Ordinal));
        Assert.Equal("vorher gesagt", ChatMessageSerializer.Deserialize(data["data: ".Length..])?.Text);
    }

    [Fact]
    public async Task AReconnectWithLastEventIdDoesNotRepeatTheRoom()
    {
        using var client = _factory.CreateClient();
        using var abort = new CancellationTokenSource(TimeSpan.FromSeconds(30));

        Publish("wiederaufnahme", "gesehen", offset: 20);
        Publish("wiederaufnahme", "verpasst", offset: 21);

        using var request = new HttpRequestMessage(
            HttpMethod.Get, "/api/stream?room=wiederaufnahme");

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

        var frame = await ReadFrameWithin(reader, abort.Token, "the missed message never arrived");

        Assert.Contains("id: 21", frame);

        var data = Assert.Single(frame, line => line.StartsWith("data: ", StringComparison.Ordinal));
        Assert.Equal("verpasst", ChatMessageSerializer.Deserialize(data["data: ".Length..])?.Text);
    }

    [Fact]
    public async Task AFreshConnectionReplaysTheRoomWithStrictlyIncreasingIds()
    {
        using var client = _factory.CreateClient();
        using var abort = new CancellationTokenSource(TimeSpan.FromSeconds(30));

        Publish("ohne-header", "erste", offset: 30);
        Publish("ohne-header", "zweite", offset: 31);
        Publish("ohne-header", "dritte", offset: 32);

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
