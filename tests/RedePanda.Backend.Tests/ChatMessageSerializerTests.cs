using System.Text.Json;
using RedePanda.Contracts;

namespace RedePanda.Backend.Tests;

/// <summary>
/// The Kafka payload format is the contract between the backend and the console client. If it
/// changes silently the two stop understanding each other, so it is pinned here.
/// </summary>
public class ChatMessageSerializerTests
{
    [Fact]
    public void RoundTripPreservesEveryField()
    {
        var original = new ChatMessage(
            "general", "alice", "hallo welt",
            new DateTimeOffset(2026, 8, 11, 12, 34, 56, TimeSpan.Zero));

        var restored = ChatMessageSerializer.Deserialize(ChatMessageSerializer.Serialize(original));

        Assert.Equal(original, restored);
    }

    [Fact]
    public void RoundTripSurvivesUnicodeAndNewlines()
    {
        var original = new ChatMessage(
            "räum-ü", "älice 🐼", "Zeile 1\nZeile 2\t\"quoted\"",
            DateTimeOffset.UtcNow);

        var restored = ChatMessageSerializer.Deserialize(ChatMessageSerializer.Serialize(original));

        Assert.Equal(original, restored);
    }

    [Fact]
    public void PayloadUsesCamelCaseFieldNames()
    {
        var json = ChatMessageSerializer.Serialize(
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
        // A foreign or corrupt record on the topic must not take down the consumer loop.
        Assert.Null(ChatMessageSerializer.Deserialize(payload));
    }
}
