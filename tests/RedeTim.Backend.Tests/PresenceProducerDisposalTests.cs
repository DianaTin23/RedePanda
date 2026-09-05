using Microsoft.Extensions.DependencyInjection;

namespace RedeTim.Backend.Tests;

public class PresenceProducerDisposalTests
{
    [Fact]
    public async Task ResolvingIPresenceProducerDoesNotDoubleDisposeItOnHostShutdown()
    {
        // Two disposal-tracked slots for one object made a real host throw on every shutdown:
        // the second Dispose() called Flush() on a destroyed librdkafka handle. Pins the fix.
        await using var factory = new ChatStreamEndpointTests.BrokerlessBackend();
        using var client = factory.CreateClient();

        // Mirrors what GET /api/stream and POST /api/join do on every request.
        _ = factory.Services.GetRequiredService<IPresenceProducer>();

        await factory.DisposeAsync();
    }
}
