using System.Diagnostics;

namespace RedePanda.Backend.Tests;

/// <summary>
/// The one step of the shutdown that nothing used to bound. <c>HostOptions.ShutdownTimeout</c>
/// cancels the token handed to <c>StopAsync</c>; it does not abandon a service that blocks inside
/// it, and librdkafka's <c>Close()</c> is a leave-group round trip that blocks — so against an
/// unreachable broker the pod sat past the 5 + 25 + 5 s the README budgets for a shutdown and was
/// SIGKILLed at 45 s with nothing in the log to say why.
/// </summary>
public class ChatConsumerShutdownTests
{
    private static readonly TimeSpan ShortBudget = TimeSpan.FromMilliseconds(200);

    /// <summary>The ordinary case: the broker answers, so the close is waited for and completes.</summary>
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

    /// <summary>
    /// The case that mattered: the broker is gone, the round trip never returns, and the pod has to
    /// stop waiting for it rather than take the whole grace period with it.
    /// </summary>
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

        // Generous, because a loaded machine is not a bug. The point is that it returned at all
        // rather than blocking until something else killed the process.
        Assert.True(
            watch.Elapsed < TimeSpan.FromSeconds(10),
            $"waited {watch.Elapsed} for a {ShortBudget} budget");

        // Let the abandoned thread finish, so it does not outlive the test run.
        release.Set();
    }

    /// <summary>
    /// A close that fails is not a reason to fail the shutdown: the pod is leaving either way, and
    /// throwing here would replace a warning with a stack trace on every broker outage.
    /// </summary>
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
