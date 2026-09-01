using System.Diagnostics;

namespace RedeTim.Backend.Tests;

public class ChatConsumerShutdownTests
{
    private static readonly TimeSpan ShortBudget = TimeSpan.FromMilliseconds(200);

    [Fact]
    public void ACloseThatFinishesInTimeIsWaitedFor()
    {
        var closed = false;

        var finished = ChatConsumerService.TryCloseWithin(
            () => closed = true, TimeSpan.FromSeconds(5), out var failure);

        Assert.True(finished);
        Assert.True(closed);
        Assert.Null(failure);
    }

    [Fact]
    public void ACloseThatBlocksIsAbandonedWhenTheBudgetIsSpent()
    {
        using var release = new ManualResetEventSlim(false);
        using var started = new ManualResetEventSlim(false);
        var watch = Stopwatch.StartNew();

        var finished = ChatConsumerService.TryCloseWithin(
            () =>
            {
                started.Set();
                release.Wait();
            },
            ShortBudget,
            out var failure);

        watch.Stop();

        Assert.False(finished);
        Assert.Null(failure);
        Assert.True(started.IsSet, "the close was never attempted");

        Assert.True(
            watch.Elapsed < TimeSpan.FromSeconds(10),
            $"waited {watch.Elapsed} for a {ShortBudget} budget");

        release.Set();
    }

    [Fact]
    public void ACloseThatThrowsIsReportedRatherThanRaised()
    {
        var finished = ChatConsumerService.TryCloseWithin(
            () => throw new InvalidOperationException("no broker"), TimeSpan.FromSeconds(5),
            out var failure);

        Assert.True(finished);
        Assert.IsType<InvalidOperationException>(failure);
    }
}
