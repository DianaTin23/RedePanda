using System.Net;
using System.Net.Http.Json;
using Microsoft.AspNetCore.Hosting;
using Microsoft.AspNetCore.Mvc.Testing;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.DependencyInjection.Extensions;

namespace RedeTim.Backend.Tests;

public class JoinEndpointTests : IClassFixture<JoinEndpointTests.BrokerlessBackend>
{
    private readonly BrokerlessBackend _factory;

    public JoinEndpointTests(BrokerlessBackend factory) => _factory = factory;

    public sealed class BrokerlessBackend : WebApplicationFactory<Program>
    {
        public FakePresenceProducer Producer { get; } = new();

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

                services.RemoveAll<IPresenceProducer>();
                services.AddSingleton<IPresenceProducer>(Producer);
            });
        }
    }

    public sealed class FakePresenceProducer : IPresenceProducer
    {
        public List<(string Room, string Nickname)> Renewals { get; } = [];

        public Task RenewAsync(string room, string nickname, CancellationToken cancellationToken)
        {
            lock (Renewals)
            {
                Renewals.Add((room, nickname));
            }

            return Task.CompletedTask;
        }

        public Task ReleaseAsync(string room, string nickname, CancellationToken cancellationToken) =>
            Task.CompletedTask;
    }

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
