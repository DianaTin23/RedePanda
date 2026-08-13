namespace RedePanda.Backend.Tests;

/// <summary>
/// Every replica used to replay the whole topic before it could report ready, so both startup time
/// and broker read load grew with the topic <em>and</em> with the replica count — worst exactly
/// when an autoscaler adds pods because the existing ones are already loaded.
/// <para>
/// Reading a bounded window back from the end fixes that, and the arithmetic is the part worth
/// pinning: an offset below the low watermark is not rejected by librdkafka. It silently falls
/// back to <c>auto.offset.reset</c>, the pod replays everything after all, and from the outside
/// that is indistinguishable from the fix working.
/// </para>
/// </summary>
public class ChatConsumerStartOffsetTests
{
    /// <summary>The ordinary case: a long topic, and only the tail of it is wanted.</summary>
    [Fact]
    public void ALongTopicIsReadFromTheEndBackwards()
    {
        Assert.Equal(8_000, ChatConsumerService.StartOffsetFor(low: 0, high: 10_000, replayRecords: 2_000));
    }

    /// <summary>
    /// The case the <c>Math.Max</c> exists for. Retention has deleted the first 9 000 records, so
    /// the window the caller asked for starts before anything the broker still has — and asking
    /// for offset 8 000 would put the pod back to replaying the entire partition.
    /// </summary>
    [Fact]
    public void AWindowReachingBeforeRetentionStartsAtTheOldestSurvivingRecord()
    {
        Assert.Equal(9_000, ChatConsumerService.StartOffsetFor(low: 9_000, high: 10_000, replayRecords: 2_000));
    }

    /// <summary>A topic shorter than the window is simply read in full.</summary>
    [Fact]
    public void AShortTopicIsReadFromItsBeginning()
    {
        Assert.Equal(0, ChatConsumerService.StartOffsetFor(low: 0, high: 50, replayRecords: 2_000));
    }

    [Fact]
    public void AnEmptyTopicStartsAtTheBeginning()
    {
        Assert.Equal(0, ChatConsumerService.StartOffsetFor(low: 0, high: 0, replayRecords: 2_000));
    }

    /// <summary>
    /// Zero keeps the original behaviour deliberately available: everything the broker still
    /// holds, however much that is.
    /// </summary>
    [Theory]
    [InlineData(0, 10_000, 0)]
    [InlineData(9_000, 10_000, 9_000)]
    public void ZeroMeansEverythingTheBrokerStillHolds(long low, long high, long expected)
    {
        Assert.Equal(expected, ChatConsumerService.StartOffsetFor(low, high, replayRecords: 0));
    }

    /// <summary>
    /// The result is never below the low watermark and never past the end, whatever the three
    /// numbers are. Those are the only two ways this can be wrong.
    /// </summary>
    [Theory]
    [InlineData(0, 0, 1)]
    [InlineData(0, 1, 1)]
    [InlineData(5, 5, 100)]
    [InlineData(100, 100_000, 1)]
    [InlineData(99_999, 100_000, 500_000)]
    [InlineData(0, 100, int.MaxValue)]
    public void TheStartIsAlwaysInsideWhatTheBrokerHolds(long low, long high, int replayRecords)
    {
        var start = ChatConsumerService.StartOffsetFor(low, high, replayRecords);

        Assert.True(start >= low, $"start {start} is below the low watermark {low}");
        Assert.True(start <= high, $"start {start} is past the high watermark {high}");
    }
}
