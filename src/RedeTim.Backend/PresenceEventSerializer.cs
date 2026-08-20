using System.Text.Json;
using System.Text.Json.Serialization;

namespace RedeTim.Backend;

/// <summary>The single place where the presence topic's payload format is defined.</summary>
internal static class PresenceEventSerializer
{
    private static readonly JsonSerializerOptions Options = new()
    {
        PropertyNamingPolicy = JsonNamingPolicy.CamelCase,
        DefaultIgnoreCondition = JsonIgnoreCondition.Never,
    };

    public static string Serialize(PresenceRecord record) => JsonSerializer.Serialize(record, Options);

    /// <summary>Deserializes a topic payload, returning <c>null</c> for anything unreadable.</summary>
    public static PresenceRecord? Deserialize(string payload)
    {
        try
        {
            return JsonSerializer.Deserialize<PresenceRecord>(payload, Options);
        }
        catch (JsonException)
        {
            return null;
        }
    }
}
