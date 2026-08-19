namespace RedeTim.Backend.Tests;

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

    [Fact]
    public void WhatWasHeldBackIsReportedOnTheNextMessageThrough()
    {
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
