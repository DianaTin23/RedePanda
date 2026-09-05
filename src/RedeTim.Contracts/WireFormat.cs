using System.Text.Json;
using System.Text.Json.Serialization;

namespace RedeTim.Contracts;

// The single JsonSerializer configuration in the repo. Every wire payload goes through here --
// chat messages and presence records alike. Two copies of these options is exactly how the two
// ends stop understanding each other without anything failing loudly.
//
// PresenceKey is the deliberate exception: its JSON is an opaque Kafka record *key*, never a
// payload, and its shape is fixed by the records already in the compacted topic.
public static class WireFormat
{
    private static readonly JsonSerializerOptions Options = new()
    {
        PropertyNamingPolicy = JsonNamingPolicy.CamelCase,
        DefaultIgnoreCondition = JsonIgnoreCondition.Never,
    };

    public static string Serialize<T>(T value) => JsonSerializer.Serialize(value, Options);

    public static T? Deserialize<T>(string payload)
    {
        try
        {
            return JsonSerializer.Deserialize<T>(payload, Options);
        }
        catch (JsonException)
        {
            return default;
        }
    }
}
