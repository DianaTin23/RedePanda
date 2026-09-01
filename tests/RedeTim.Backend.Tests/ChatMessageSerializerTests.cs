using System.Text.Json;
using RedeTim.Contracts;

namespace RedeTim.Backend.Tests;

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
        Assert.Null(ChatMessageSerializer.Deserialize(payload));
    }
}
