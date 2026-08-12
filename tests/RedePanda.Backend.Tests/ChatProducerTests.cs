using System.Diagnostics;
using Confluent.Kafka;
using Microsoft.AspNetCore.Http;
using Microsoft.Extensions.Logging.Abstractions;
using RedePanda.Contracts;

namespace RedePanda.Backend.Tests;

/// <summary>
/// The produce path has to fail fast. librdkafka's <c>message.timeout.ms</c> defaults to five
/// minutes, and the POST behind it holds a browser's composer for exactly as long.
/// </summary>
public class ChatProducerTests
{
    [Fact]
    public void TheConfiguredTimeoutBoundsTheMessage()
    {
        var config = ChatProducer.BuildConfig(TestOptions.Create() with { ProduceTimeoutMs = 8000 });

        Assert.Equal(8000, config.MessageTimeoutMs);
    }

    /// <summary>
    /// A per-request timeout at or above the message timeout would let a single in-flight request
    /// consume the whole budget, leaving no room for the retry the message timeout is meant to
    /// cover.
    /// </summary>
    [Fact]
    public void OneRequestMayNotConsumeTheWholeBudget()
    {
        var config = ChatProducer.BuildConfig(TestOptions.Create() with { ProduceTimeoutMs = 8000 });

        Assert.NotNull(config.RequestTimeoutMs);
        Assert.True(config.RequestTimeoutMs < config.MessageTimeoutMs);
    }

    /// <summary>
    /// A timeout is not a broken gateway: the request was never answered either way, and 504 is
    /// what says so. Everything else the broker rejects outright stays 502.
    /// </summary>
    [Theory]
    [InlineData(ErrorCode.Local_MsgTimedOut, StatusCodes.Status504GatewayTimeout)]
    [InlineData(ErrorCode.Local_TimedOut, StatusCodes.Status504GatewayTimeout)]
    [InlineData(ErrorCode.MsgSizeTooLarge, StatusCodes.Status502BadGateway)]
    [InlineData(ErrorCode.Local_AllBrokersDown, StatusCodes.Status502BadGateway)]
    public void ATimeoutIsReportedAsAGatewayTimeout(ErrorCode code, int expected)
    {
        Assert.Equal(expected, ChatProducer.StatusCodeFor(new Error(code)));
    }

    /// <summary>
    /// The test that would have caught the bug: everything above only pins the shape of the
    /// configuration, this one pins the behaviour. Port 1 is closed on every machine, so the
    /// producer never reaches a broker and the only thing that can end the call is the timeout.
    /// <para>
    /// The assertion is deliberately loose — 15 s against a configured 2 s. It is not measuring
    /// librdkafka's precision, it is separating "seconds" from the five minutes that shipped.
    /// </para>
    /// </summary>
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
