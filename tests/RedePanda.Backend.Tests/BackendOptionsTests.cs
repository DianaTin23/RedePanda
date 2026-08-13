using Microsoft.Extensions.Logging;

namespace RedePanda.Backend.Tests;

/// <summary>
/// The one property that makes a second replica correct rather than harmful: two pods must land in
/// two consumer groups. Share a group and Kafka hands the single partition to exactly one of them —
/// every browser on every other pod would then sit in front of a silent room.
/// <para>
/// Read from the record directly rather than through <c>FromEnvironment</c>: environment variables
/// are process-global and these tests run in parallel with everything else.
/// </para>
/// </summary>
public class BackendOptionsTests
{
    /// <summary>
    /// LOG_LEVEL was the one setting with a silent fallback, and it is the worst possible one to
    /// have it: a misspelling left the process running at Information while whoever set it
    /// believed they had turned the detail up, so the mistake hid the evidence of itself.
    /// </summary>
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

    /// <summary>An unset value is not a mistake; it is the documented default.</summary>
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
        var first = TestOptions.Create() with { PodName = "redepanda-backend-abc" };
        var second = TestOptions.Create() with { PodName = "redepanda-backend-xyz" };

        Assert.NotEqual(first.ConsumerGroupId, second.ConsumerGroupId);
    }

    /// <summary>
    /// Deterministic, unlike the GUID it replaced: the group id in <c>rpk group list</c> names the
    /// pod it belongs to, and it is the same string on every read.
    /// </summary>
    [Fact]
    public void ConsumerGroupIdIsDerivedFromThePodName()
    {
        var options = TestOptions.Create() with { PodName = "redepanda-backend-abc" };

        Assert.Contains("redepanda-backend-abc", options.ConsumerGroupId, StringComparison.Ordinal);
        Assert.Equal(options.ConsumerGroupId, options.ConsumerGroupId);
    }

    /// <summary>
    /// Bounded, because this buffer lives in the memory of every replica and an autoscaler
    /// multiplies it. Zero would still be legal — it must just not be what ships.
    /// </summary>
    [Fact]
    public void TheDefaultHistorySizeIsBounded()
    {
        Assert.True(BackendOptions.DefaultHistorySize > 0);
    }

    /// <summary>
    /// The other half of the same bound. A per-room limit alone leaves the number of rooms open,
    /// and that number is chosen by whoever names a room rather than by configuration — so zero
    /// here is legal but must not be what ships either.
    /// </summary>
    [Fact]
    public void TheDefaultRoomLimitIsBounded()
    {
        Assert.True(BackendOptions.DefaultMaxRooms > 0);
    }

    /// <summary>The supplied name is used verbatim, so the group id keeps naming its pod.</summary>
    [Fact]
    public void ThePodNameFromTheEnvironmentWins()
    {
        var resolved = BackendOptions.ResolvePodName(
            "redepanda-backend-abc", kubernetesServiceHost: "10.96.0.1",
            machineName: "some-host", processId: 1234);

        Assert.Equal("redepanda-backend-abc", resolved);
    }

    /// <summary>
    /// The failure mode the tests above never covered. In a cluster POD_NAME comes from a
    /// fieldRef, and <c>metadata.name</c> is unique per namespace — so the only way to reach this
    /// branch is a Deployment that lost the fieldRef. Falling back to the machine name there would
    /// give every pod on one node the same group id, Kafka would hand the partition to exactly one
    /// of them, and every browser on the others would sit in a silent room. That is a failure
    /// nobody sees in a log; a crash-looping pod is.
    /// </summary>
    [Fact]
    public void AMissingPodNameInAClusterIsFatal()
    {
        var failure = Assert.Throws<InvalidOperationException>(() =>
            BackendOptions.ResolvePodName(
                podName: null, kubernetesServiceHost: "10.96.0.1",
                machineName: "some-node", processId: 1234));

        Assert.Contains("POD_NAME", failure.Message, StringComparison.Ordinal);
    }

    /// <summary>
    /// Blank counts as missing: an empty env var is what an unset ConfigMap key produces, and it
    /// would otherwise become a group id of "redepanda-backend-".
    /// </summary>
    [Fact]
    public void AnEmptyPodNameInAClusterIsFatalToo()
    {
        Assert.Throws<InvalidOperationException>(() =>
            BackendOptions.ResolvePodName(
                podName: "   ", kubernetesServiceHost: "10.96.0.1",
                machineName: "some-node", processId: 1234));
    }

    /// <summary>
    /// Outside a cluster there is no fieldRef to demand, and the same collision is reachable by
    /// running the backend twice on one machine against one broker — the ordinary way to try the
    /// fan-out locally. The process id is what separates the two.
    /// </summary>
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
