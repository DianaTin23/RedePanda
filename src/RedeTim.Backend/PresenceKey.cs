using System.Diagnostics.CodeAnalysis;
using System.Text.Json;

namespace RedeTim.Backend;

internal static class PresenceKey
{
    private readonly record struct Payload(string Room, string Nickname);

    public static string Encode(string room, string nickname) =>
        JsonSerializer.Serialize(new Payload(room, nickname));

    // The only source of room and nickname for a tombstone, whose value is null by definition.
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
