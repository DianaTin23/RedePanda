using System.Net;
using System.Net.Http.Json;
using Microsoft.Extensions.DependencyInjection;

namespace RedeTim.Backend.Tests;

public class JoinEndpointTests : IClassFixture<BrokerlessBackend>
{
    private readonly BrokerlessBackend _factory;

    public JoinEndpointTests(BrokerlessBackend factory) => _factory = factory;

    [Fact]
    public async Task AValidJoinIsAccepted()
    {
        using var client = _factory.CreateClient();

        using var response = await client.PostAsJsonAsync(
            "/api/join",
            new { room = "general", nickname = "alice-valid-join" },
            TestContext.Current.CancellationToken);

        Assert.Equal(HttpStatusCode.OK, response.StatusCode);
        Assert.Contains(("general", "alice-valid-join"), _factory.Producer.Renewals);
    }

    [Fact]
    public async Task AReservedNicknameIsRejected()
    {
        using var client = _factory.CreateClient();

        using var response = await client.PostAsJsonAsync(
            "/api/join",
            new { room = "general", nickname = "claude" },
            TestContext.Current.CancellationToken);

        Assert.Equal(HttpStatusCode.BadRequest, response.StatusCode);
        var body = await response.Content.ReadAsStringAsync(TestContext.Current.CancellationToken);
        Assert.Contains("reserved", body);
    }

    [Fact]
    public async Task AnAlreadyTakenNicknameIsRejected()
    {
        using var client = _factory.CreateClient();

        var store = _factory.Services.GetRequiredService<PresenceStore>();
        store.Apply("umkaempft", "bob", DateTimeOffset.UtcNow);

        using var response = await client.PostAsJsonAsync(
            "/api/join",
            new { room = "umkaempft", nickname = "bob" },
            TestContext.Current.CancellationToken);

        Assert.Equal(HttpStatusCode.Conflict, response.StatusCode);
    }

    [Fact]
    public async Task AnEmptyNicknameIsRejected()
    {
        using var client = _factory.CreateClient();

        using var response = await client.PostAsJsonAsync(
            "/api/join",
            new { room = "general", nickname = "" },
            TestContext.Current.CancellationToken);

        Assert.Equal(HttpStatusCode.BadRequest, response.StatusCode);
    }
}
