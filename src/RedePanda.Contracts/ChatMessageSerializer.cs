using System.Text.Json;
using System.Text.Json.Serialization;

namespace RedePanda.Contracts;

/// <summary>
/// The single place where the Kafka payload format is defined.
/// <para>
/// Backend and console client must serialize identically or they silently stop understanding
/// each other, so neither is allowed to call <see cref="JsonSerializer"/> with its own options.
/// </para>
/// </summary>
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
            // A foreign or corrupt record must not take down the consumer loop.
            return null;
        }
    }
}
