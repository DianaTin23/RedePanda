using System.Text.Json;
using System.Text.Json.Serialization;

namespace RedeTim.Backend;

internal static class PresenceEventSerializer
{
    private static readonly JsonSerializerOptions Options = new()
    {
        PropertyNamingPolicy = JsonNamingPolicy.CamelCase,
        DefaultIgnoreCondition = JsonIgnoreCondition.Never,
    };

    public static string Serialize(PresenceRecord record) => JsonSerializer.Serialize(record, Options);

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
