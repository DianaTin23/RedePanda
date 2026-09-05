using System.Diagnostics;
using Confluent.Kafka;
using Microsoft.AspNetCore.Http;
using Microsoft.Extensions.Logging.Abstractions;
using RedeTim.Contracts;

namespace RedeTim.Backend.Tests;

public class ChatProducerTests
{
    [Fact]
    public void TheConfiguredTimeoutBoundsTheMessage()
    {
        var config = ChatProducer.BuildConfig(TestOptions.Create() with { ProduceTimeoutMs = 8000 });

        Assert.Equal(8000, config.MessageTimeoutMs);
    }

    [Fact]
    public void OneRequestMayNotConsumeTheWholeBudget()
    {
        var config = ChatProducer.BuildConfig(TestOptions.Create() with { ProduceTimeoutMs = 8000 });

        Assert.NotNull(config.RequestTimeoutMs);
        Assert.True(config.RequestTimeoutMs < config.MessageTimeoutMs);
    }

    [Fact]
    public void ARetriedRecordMayNotOvertakeALaterOne()
    {
        var config = ChatProducer.BuildConfig(TestOptions.Create());

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
        Assert.Equal(expected, KafkaJsonProducer.StatusCodeFor(new Error(code)));
    }

    [Fact]
    public async Task ProducingToAnUnreachableBrokerGivesUpAfterTheTimeout()
    {
        using var meterFactory = new TestMeterFactory();
        var options = TestOptions.Create() with
        {
            BootstrapServers = "127.0.0.1:1",
            Topic = "unreachable",
            ProduceTimeoutMs = 2000,
        };

        using var producer = new ChatProducer(
            options, new ChatMetrics(meterFactory), NullLogger<ChatProducer>.Instance);

        Assert.True(ChatMessage.TryCreate(
            "general", "tester", "hello", DateTimeOffset.UtcNow,
            ChatMessage.DefaultMaxTextLength, out var message, out _));

        var stopwatch = Stopwatch.StartNew();
        var failure = await Assert.ThrowsAsync<ProduceException<string, string>>(
            () => producer.ProduceAsync(message!, TestContext.Current.CancellationToken));
        stopwatch.Stop();

        Assert.Equal(ErrorCode.Local_MsgTimedOut, failure.Error.Code);
        Assert.True(
            stopwatch.Elapsed < TimeSpan.FromSeconds(15),
            $"The produce took {stopwatch.Elapsed.TotalSeconds:F1}s, so the timeout did not apply.");
    }
}
