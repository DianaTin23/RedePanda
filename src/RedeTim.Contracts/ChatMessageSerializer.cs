using System.Text.Json;
using System.Text.Json.Serialization;

namespace RedeTim.Contracts;

public static class ChatMessageSerializer
{
    private static readonly JsonSerializerOptions Options = new()
    {
        PropertyNamingPolicy = JsonNamingPolicy.CamelCase,
        DefaultIgnoreCondition = JsonIgnoreCondition.Never,
    };

    public static string Serialize(ChatMessage message) => JsonSerializer.Serialize(message, Options);

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
