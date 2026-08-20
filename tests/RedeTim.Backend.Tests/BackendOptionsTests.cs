using Microsoft.Extensions.Logging;

namespace RedeTim.Backend.Tests;

public class BackendOptionsTests
{
    [Theory]
    [InlineData("Debug", LogLevel.Debug)]
    [InlineData("warning", LogLevel.Warning)]
    [InlineData("  Error  ", LogLevel.Error)]
    public void AKnownLogLevelIsAccepted(string raw, LogLevel expected)
    {
        Assert.Equal(
            expected,
            BackendOptions.ReadLogLevel("LOG_LEVEL", BackendOptions.DefaultLogLevel, raw));
    }

    [Theory]
    [InlineData("Verbose")]      // a real level name, but in another logging framework
    [InlineData("WARN")]         // the abbreviation half the world writes
    [InlineData("99")]           // parses as an enum without Enum.IsDefined, and silences everything
    public void AnUnknownLogLevelIsRefused(string raw)
    {
        var failure = Assert.Throws<InvalidOperationException>(
            () => BackendOptions.ReadLogLevel("LOG_LEVEL", BackendOptions.DefaultLogLevel, raw));

        Assert.Contains("LOG_LEVEL", failure.Message, StringComparison.Ordinal);
        Assert.Contains("Information", failure.Message, StringComparison.Ordinal);
    }

    [Theory]
    [InlineData("")]
    [InlineData("   ")]
    public void AnUnsetLogLevelFallsBackQuietly(string raw)
    {
        Assert.Equal(
            BackendOptions.DefaultLogLevel,
            BackendOptions.ReadLogLevel("LOG_LEVEL", BackendOptions.DefaultLogLevel, raw));
    }

    [Fact]
    public void ConsumerGroupIdIsUniquePerPod()
    {
        var first = TestOptions.Create() with { PodName = "redetim-backend-abc" };
        var second = TestOptions.Create() with { PodName = "redetim-backend-xyz" };

        Assert.NotEqual(first.ConsumerGroupId, second.ConsumerGroupId);
    }

    [Fact]
    public void ConsumerGroupIdIsDerivedFromThePodName()
    {
        var options = TestOptions.Create() with { PodName = "redetim-backend-abc" };

        Assert.Contains("redetim-backend-abc", options.ConsumerGroupId, StringComparison.Ordinal);
        Assert.Equal(options.ConsumerGroupId, options.ConsumerGroupId);
    }

    [Fact]
    public void TheDefaultHistorySizeIsBounded()
    {
        Assert.True(BackendOptions.DefaultHistorySize > 0);
    }

    [Fact]
    public void TheDefaultRoomLimitIsBounded()
    {
        Assert.True(BackendOptions.DefaultMaxRooms > 0);
    }

    [Fact]
    public void ThePodNameFromTheEnvironmentWins()
    {
        var resolved = BackendOptions.ResolvePodName(
            "redetim-backend-abc", kubernetesServiceHost: "10.96.0.1",
            machineName: "some-host", processId: 1234);

        Assert.Equal("redetim-backend-abc", resolved);
    }

    [Fact]
    public void AMissingPodNameInAClusterIsFatal()
    {
        var failure = Assert.Throws<InvalidOperationException>(() =>
            BackendOptions.ResolvePodName(
                podName: null, kubernetesServiceHost: "10.96.0.1",
                machineName: "some-node", processId: 1234));

        Assert.Contains("POD_NAME", failure.Message, StringComparison.Ordinal);
    }

    [Fact]
    public void AnEmptyPodNameInAClusterIsFatalToo()
    {
        Assert.Throws<InvalidOperationException>(() =>
            BackendOptions.ResolvePodName(
                podName: "   ", kubernetesServiceHost: "10.96.0.1",
                machineName: "some-node", processId: 1234));
    }

    [Fact]
    public void PresenceConsumerGroupIdIsUniquePerPod()
    {
        var first = TestOptions.Create() with { PodName = "redetim-backend-abc" };
        var second = TestOptions.Create() with { PodName = "redetim-backend-xyz" };

        Assert.NotEqual(first.PresenceConsumerGroupId, second.PresenceConsumerGroupId);
    }

    [Fact]
    public void PresenceConsumerGroupIdIsDerivedFromThePodNameAndSeparateFromTheChatGroup()
    {
        var options = TestOptions.Create() with { PodName = "redetim-backend-abc" };

        Assert.Contains(
            "redetim-backend-abc", options.PresenceConsumerGroupId, StringComparison.Ordinal);
        Assert.NotEqual(options.ConsumerGroupId, options.PresenceConsumerGroupId);
    }

    [Fact]
    public void TwoLocalProcessesDoNotShareAConsumerGroup()
    {
        var first = TestOptions.Create() with
        {
            PodName = BackendOptions.ResolvePodName(null, null, "dev-laptop", 1234),
        };
        var second = TestOptions.Create() with
        {
            PodName = BackendOptions.ResolvePodName(null, null, "dev-laptop", 5678),
        };

        Assert.NotEqual(first.ConsumerGroupId, second.ConsumerGroupId);
        Assert.Contains("dev-laptop", first.ConsumerGroupId, StringComparison.Ordinal);
    }
}
