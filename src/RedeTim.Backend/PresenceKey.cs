using System.Diagnostics.CodeAnalysis;
using System.Text.Json;

namespace RedeTim.Backend;

/// <summary>
/// Encodes a (room, nickname) pair as the presence topic's Kafka key. Neither field has a
/// restricted character set, so a raw delimiter (e.g. "room|nickname") could collide; JSON
/// makes the encoding unambiguous instead.
/// </summary>
internal static class PresenceKey
{
    private readonly record struct Payload(string Room, string Nickname);

    public static string Encode(string room, string nickname) =>
        JsonSerializer.Serialize(new Payload(room, nickname));

    /// <summary>
    /// Decodes a key. This is the only source of room/nickname for a tombstone, whose value is
    /// null by definition.
    /// </summary>
    public static bool TryDecode(
        string key, [NotNullWhen(true)] out string? room, [NotNullWhen(true)] out string? nickname)
    {
        try
        {
            var payload = JsonSerializer.Deserialize<Payload>(key);
            if (string.IsNullOrEmpty(payload.Room) || string.IsNullOrEmpty(payload.Nickname))
            {
                room = null;
                nickname = null;
                return false;
            }

            room = payload.Room;
            nickname = payload.Nickname;
            return true;
        }
        catch (JsonException)
        {
            room = null;
            nickname = null;
            return false;
        }
    }
}
