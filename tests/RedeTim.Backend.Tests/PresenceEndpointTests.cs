using System.Net;
using System.Net.Http.Json;
using Microsoft.Extensions.DependencyInjection;
using RedeTim.Contracts;

namespace RedeTim.Backend.Tests;

public class PresenceEndpointTests : IClassFixture<ChatStreamEndpointTests.BrokerlessBackend>
{
    private readonly ChatStreamEndpointTests.BrokerlessBackend _factory;

    public PresenceEndpointTests(ChatStreamEndpointTests.BrokerlessBackend factory) => _factory = factory;

    private sealed record PresenceResponse(string[] Nicknames);

    [Fact]
    public async Task MissingRoomIsRejected()
    {
        using var client = _factory.CreateClient();

        using var response = await client.GetAsync("/api/presence", TestContext.Current.CancellationToken);

        Assert.Equal(HttpStatusCode.BadRequest, response.StatusCode);
    }

    [Fact]
    public async Task AnOversizedRoomIsRejected()
    {
        using var client = _factory.CreateClient();

        var oversizedRoom = new string('a', ChatMessage.MaxRoomLength + 1);
        using var response = await client.GetAsync(
            $"/api/presence?room={oversizedRoom}", TestContext.Current.CancellationToken);

        Assert.Equal(HttpStatusCode.BadRequest, response.StatusCode);
        var body = await response.Content.ReadAsStringAsync(TestContext.Current.CancellationToken);
        Assert.Contains("must not exceed", body);
    }

    [Fact]
    public async Task AnEmptyRoomHasNoOneInIt()
    {
        using var client = _factory.CreateClient();

        var body = await client.GetFromJsonAsync<PresenceResponse>(
            "/api/presence?room=leerer-raum-" + Guid.NewGuid(), TestContext.Current.CancellationToken);

        Assert.NotNull(body);
        Assert.Empty(body.Nicknames);
    }

    [Fact]
    public async Task ActiveNicknamesAreReturnedSorted()
    {
        var room = "presence-endpoint-" + Guid.NewGuid();
        var store = _factory.Services.GetRequiredService<PresenceStore>();
        store.Apply(room, "bob", DateTimeOffset.UtcNow);
        store.Apply(room, "alice", DateTimeOffset.UtcNow);

        using var client = _factory.CreateClient();
        var body = await client.GetFromJsonAsync<PresenceResponse>(
            $"/api/presence?room={room}", TestContext.Current.CancellationToken);

        Assert.NotNull(body);
        Assert.Equal(["alice", "bob"], body.Nicknames);
    }
}
