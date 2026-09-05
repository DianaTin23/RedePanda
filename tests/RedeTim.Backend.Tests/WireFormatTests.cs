using System.Text.Json;
using RedeTim.Contracts;

namespace RedeTim.Backend.Tests;

public class WireFormatTests
{
    [Fact]
    public void RoundTripPreservesEveryField()
    {
        var original = new ChatMessage(
            "general", "alice", "hallo welt",
            new DateTimeOffset(2026, 8, 11, 12, 34, 56, TimeSpan.Zero));

        var restored = WireFormat.Deserialize<ChatMessage>(WireFormat.Serialize(original));

        Assert.Equal(original, restored);
    }

    [Fact]
    public void RoundTripSurvivesUnicodeAndNewlines()
    {
        var original = new ChatMessage(
            "räum-ü", "älice 🐼", "Zeile 1\nZeile 2\t\"quoted\"",
            DateTimeOffset.UtcNow);

        var restored = WireFormat.Deserialize<ChatMessage>(WireFormat.Serialize(original));

        Assert.Equal(original, restored);
    }

    [Fact]
    public void PayloadUsesCamelCaseFieldNames()
    {
        var json = WireFormat.Serialize(
            new ChatMessage("general", "alice", "hallo", DateTimeOffset.UtcNow));

        using var document = JsonDocument.Parse(json);
        var properties = document.RootElement.EnumerateObject().Select(p => p.Name).ToArray();

        Assert.Equal(["room", "nickname", "text", "timestamp"], properties);
    }

    [Theory]
    [InlineData("not json at all")]
    [InlineData("{\"room\":")]
    [InlineData("[]")]
    public void UnreadablePayloadsReturnNullRatherThanThrowing(string payload)
    {
        Assert.Null(WireFormat.Deserialize<ChatMessage>(payload));
    }

    // The presence payload shared these options as a second copy before WireFormat existed, and
    // nothing ever round-tripped it. It does now.
    [Fact]
    public void APresenceRecordRoundTripsThroughTheSameOptions()
    {
        var original = new PresenceRecord(
            "räum-ü", "älice 🐼", "redetim-backend-0",
            new DateTimeOffset(2026, 8, 11, 12, 34, 56, TimeSpan.Zero));

        var json = WireFormat.Serialize(original);

        using var document = JsonDocument.Parse(json);
        Assert.Equal(
            ["room", "nickname", "podName", "renewedAt"],
            document.RootElement.EnumerateObject().Select(p => p.Name).ToArray());

        Assert.Equal(original, WireFormat.Deserialize<PresenceRecord>(json));
    }

    [Fact]
    public void AnUnreadablePresencePayloadReturnsNullRatherThanThrowing()
    {
        Assert.Null(WireFormat.Deserialize<PresenceRecord>("{\"room\":"));
    }
}
