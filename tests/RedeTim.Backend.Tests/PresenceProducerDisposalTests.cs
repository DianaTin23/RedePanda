using Microsoft.Extensions.DependencyInjection;

namespace RedeTim.Backend.Tests;

public class PresenceProducerDisposalTests
{
    [Fact]
    public async Task ResolvingIPresenceProducerDoesNotDoubleDisposeItOnHostShutdown()
    {
        // Registering the concrete PresenceProducer as its own singleton *and* wrapping it in a
        // second AddSingleton<IPresenceProducer>(sp => sp.GetRequiredService<PresenceProducer>())
        // gives the container two disposal-tracked slots for the same object. Every request
        // handler that takes an IPresenceProducer (GET /api/stream, POST /api/join) resolves
        // both slots, so a real host hit this on every shutdown: the second Dispose() called
        // Flush() on an already-destroyed librdkafka handle and threw. Program.cs now registers
        // IPresenceProducer once, via ActivatorUtilities, with no separate PresenceProducer
        // registration -- this pins that fix.
        await using var factory = new ChatStreamEndpointTests.BrokerlessBackend();
        using var client = factory.CreateClient();

        // Mirrors what GET /api/stream and POST /api/join do on every request.
        _ = factory.Services.GetRequiredService<IPresenceProducer>();

        await factory.DisposeAsync();
    }
}
