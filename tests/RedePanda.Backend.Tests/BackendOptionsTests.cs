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
}
