namespace RedePanda.Backend.Tests;

/// <summary>
/// librdkafka calls the error callback once per broker connection attempt, so an unreachable
/// broker wrote on the order of twenty lines a second per pod — burying the log at exactly the
/// moment someone would go looking in it.
/// <para>
/// The throttle folds that repetition without hiding it, which is the part worth pinning: a
/// suppressing logger that forgets what it suppressed is worse than a noisy one, because the
/// quiet then reads as "it only happened once".
/// </para>
/// </summary>
public class LogThrottleTests
{
    [Fact]
    public void TheFirstMessageAlwaysGetsThrough()
    {
        var throttle = new LogThrottle(TimeSpan.FromMinutes(1));

        Assert.True(throttle.ShouldLog(out var suppressed));
        Assert.Equal(0, suppressed);
    }

    [Fact]
    public void FurtherMessagesInsideTheIntervalAreHeldBack()
    {
        var throttle = new LogThrottle(TimeSpan.FromMinutes(1));
        Assert.True(throttle.ShouldLog(out _));

        for (var i = 0; i < 50; i++)
        {
            Assert.False(throttle.ShouldLog(out _));
        }
    }

    /// <summary>
    /// The count is the whole point. Without it the next line through would claim a single event
    /// where fifty happened.
    /// </summary>
    [Fact]
    public void WhatWasHeldBackIsReportedOnTheNextMessageThrough()
    {
        // Zero, so the interval has always elapsed and no test has to wait for it.
        var throttle = new LogThrottle(TimeSpan.Zero);
        Assert.True(throttle.ShouldLog(out _));

        var longThrottle = new LogThrottle(TimeSpan.FromMilliseconds(50));
        Assert.True(longThrottle.ShouldLog(out _));
        for (var i = 0; i < 7; i++)
        {
            Assert.False(longThrottle.ShouldLog(out _));
        }

        Thread.Sleep(TimeSpan.FromMilliseconds(80));

        Assert.True(longThrottle.ShouldLog(out var suppressed));
        Assert.Equal(7, suppressed);
    }

    /// <summary>And the count resets, rather than accumulating for the life of the process.</summary>
    [Fact]
    public void TheSuppressedCountStartsAgainAfterItIsReported()
    {
        var throttle = new LogThrottle(TimeSpan.Zero);

        Assert.True(throttle.ShouldLog(out _));
        Assert.True(throttle.ShouldLog(out var second));

        Assert.Equal(0, second);
    }

    [Theory]
    [InlineData(0, "")]
    [InlineData(1, " (1 further suppressed)")]
    [InlineData(42, " (42 further suppressed)")]
    public void TheClauseIsOmittedEntirelyWhenNothingWasSuppressed(long suppressed, string expected)
    {
        Assert.Equal(expected, LogThrottle.Describe(suppressed));
    }
}
