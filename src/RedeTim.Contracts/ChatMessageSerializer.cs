using System.Text.Json;
using System.Text.Json.Serialization;

namespace RedeTim.Contracts;

/// <summary>The single place where the Kafka payload format is defined.</summary>
public static class ChatMessageSerializer
{
    private static readonly JsonSerializerOptions Options = new()
    {
        PropertyNamingPolicy = JsonNamingPolicy.CamelCase,
        DefaultIgnoreCondition = JsonIgnoreCondition.Never,
    };

    public static string Serialize(ChatMessage message) => JsonSerializer.Serialize(message, Options);

    /// <summary>Deserializes a topic payload, returning <c>null</c> for anything unreadable.</summary>
    public static ChatMessage? Deserialize(string payload)
    {
        try
        {
            return JsonSerializer.Deserialize<ChatMessage>(payload, Options);
        }
        catch (JsonException)
        {
            return null;
        }
    }
}
