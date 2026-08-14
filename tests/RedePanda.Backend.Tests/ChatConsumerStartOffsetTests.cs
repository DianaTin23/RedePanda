namespace RedePanda.Backend.Tests;

public class ChatConsumerStartOffsetTests
{
    [Fact]
    public void ALongTopicIsReadFromTheEndBackwards()
    {
        Assert.Equal(8_000, ChatConsumerService.StartOffsetFor(low: 0, high: 10_000, replayRecords: 2_000));
    }

    [Fact]
    public void AWindowReachingBeforeRetentionStartsAtTheOldestSurvivingRecord()
    {
        Assert.Equal(9_000, ChatConsumerService.StartOffsetFor(low: 9_000, high: 10_000, replayRecords: 2_000));
    }

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

    [Theory]
    [InlineData(0, 10_000, 0)]
    [InlineData(9_000, 10_000, 9_000)]
    public void ZeroMeansEverythingTheBrokerStillHolds(long low, long high, long expected)
    {
        Assert.Equal(expected, ChatConsumerService.StartOffsetFor(low, high, replayRecords: 0));
    }

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
