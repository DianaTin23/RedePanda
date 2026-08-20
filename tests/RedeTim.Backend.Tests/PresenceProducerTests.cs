using System.Diagnostics;
using Confluent.Kafka;
using Microsoft.AspNetCore.Http;
using Microsoft.Extensions.Logging.Abstractions;

namespace RedeTim.Backend.Tests;

public class PresenceProducerTests
{
    [Fact]
    public void TheConfiguredTimeoutBoundsTheMessage()
    {
        var config = PresenceProducer.BuildConfig(TestOptions.Create() with { ProduceTimeoutMs = 8000 });

        Assert.Equal(8000, config.MessageTimeoutMs);
    }

    [Fact]
    public void ARetriedRecordMayNotOvertakeALaterOne()
    {
        var config = PresenceProducer.BuildConfig(TestOptions.Create());

        Assert.True(config.EnableIdempotence);
        Assert.Equal(Acks.All, config.Acks);
    }

    [Theory]
    [InlineData(ErrorCode.Local_MsgTimedOut, StatusCodes.Status504GatewayTimeout)]
    [InlineData(ErrorCode.Local_TimedOut, StatusCodes.Status504GatewayTimeout)]
    [InlineData(ErrorCode.MsgSizeTooLarge, StatusCodes.Status502BadGateway)]
    [InlineData(ErrorCode.Local_AllBrokersDown, StatusCodes.Status502BadGateway)]
    public void ATimeoutIsReportedAsAGatewayTimeout(ErrorCode code, int expected)
    {
        Assert.Equal(expected, PresenceProducer.StatusCodeFor(new Error(code)));
    }

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
