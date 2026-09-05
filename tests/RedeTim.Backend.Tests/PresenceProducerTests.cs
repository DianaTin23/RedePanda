using System.Diagnostics;
using Confluent.Kafka;
using Microsoft.Extensions.Logging.Abstractions;

namespace RedeTim.Backend.Tests;

public class PresenceProducerTests
{
    // Config and StatusCodeFor are shared with ChatProducer and tested there; what is specific
    // to this producer is that a renewal actually reaches the wire under the same timeout.
    [Fact]
    public async Task RenewingAgainstAnUnreachableBrokerGivesUpAfterTheTimeout()
    {
        using var meterFactory = new TestMeterFactory();
        var options = TestOptions.Create() with
        {
            BootstrapServers = "127.0.0.1:1",
            PresenceTopic = "unreachable-presence",
            ProduceTimeoutMs = 2000,
        };

        using var producer = new PresenceProducer(
            options, new ChatMetrics(meterFactory), NullLogger<PresenceProducer>.Instance);

        var stopwatch = Stopwatch.StartNew();
        var failure = await Assert.ThrowsAsync<ProduceException<string, string>>(
            () => producer.RenewAsync("general", "alice", TestContext.Current.CancellationToken));
        stopwatch.Stop();

        Assert.Equal(ErrorCode.Local_MsgTimedOut, failure.Error.Code);
        Assert.True(
            stopwatch.Elapsed < TimeSpan.FromSeconds(15),
            $"The produce took {stopwatch.Elapsed.TotalSeconds:F1}s, so the timeout did not apply.");
    }
}
