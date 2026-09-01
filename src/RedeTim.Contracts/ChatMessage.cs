using System.Diagnostics.CodeAnalysis;
using System.Globalization;
using System.Text;

namespace RedeTim.Contracts;

/// <summary>A single chat message as it travels over the Kafka topic.</summary>
public sealed record ChatMessage(string Room, string Nickname, string Text, DateTimeOffset Timestamp)
{
    public const int MaxRoomLength = 64;
    public const int MaxNicknameLength = 32;
    public const int DefaultMaxTextLength = 500;

    /// <summary>Validates untrusted input and builds a <see cref="ChatMessage"/> from it.</summary>
    public static bool TryCreate(
        string? room,
        string? nickname,
        string? text,
        DateTimeOffset timestamp,
        int maxTextLength,
        [NotNullWhen(true)] out ChatMessage? message,
        [NotNullWhen(false)] out string? error)
    {
        message = null;

        if (!TryNormalizeRoomAndNickname(room, nickname, out var normalizedRoom, out var normalizedNickname, out error))
        {
            return false;
        }

        if (!TryNormalize(text, "text", maxTextLength, out var normalizedText, out error))
        {
            return false;
        }

        message = new ChatMessage(normalizedRoom, normalizedNickname, normalizedText, timestamp);
        error = null;
        return true;
    }

    /// <summary>
    /// Validates and trims a room and nickname, rejecting a nickname reserved for a non-human
    /// participant (see <see cref="ReservedNicknames"/>). Shared by <see cref="TryCreate"/> and
    /// <c>POST /api/join</c>, which needs the same rules before a message has even been typed.
    /// </summary>
    public static bool TryNormalizeRoomAndNickname(
        string? room,
        string? nickname,
        [NotNullWhen(true)] out string? normalizedRoom,
        [NotNullWhen(true)] out string? normalizedNickname,
        [NotNullWhen(false)] out string? error)
    {
        normalizedNickname = null;

        if (!TryNormalizeRoom(room, out normalizedRoom, out error))
        {
            return false;
        }

        if (!TryNormalizeNickname(nickname, out normalizedNickname, out error))
        {
            normalizedRoom = null;
            return false;
        }

        return true;
    }

    /// <summary>
    /// Validates and trims a room. The only normalization rule a room needs on its own — every
    /// caller that also has a nickname should go through <see cref="TryNormalizeRoomAndNickname"/>
    /// instead, so the two stay consistent. Used directly by <c>GET /api/stream</c> and
    /// <c>GET /api/presence</c>, which have no nickname to validate alongside it.
    /// </summary>
    public static bool TryNormalizeRoom(
        string? room,
        [NotNullWhen(true)] out string? normalizedRoom,
        [NotNullWhen(false)] out string? error) =>
        TryNormalize(StripInvisibleCharacters(room), "room", MaxRoomLength, out normalizedRoom, out error);

    /// <summary>
    /// Validates and trims a nickname, rejecting one reserved for a non-human participant (see
    /// <see cref="ReservedNicknames"/>). Used directly by <c>GET /api/stream</c>, where an invalid
    /// nickname isn't a reason to reject the connection but must still be normalized the same way
    /// before it is trusted for presence tracking — otherwise a stripped or invisible-character
    /// variant of a reserved or taken name would slip past the check <see cref="TryCreate"/> and
    /// <c>POST /api/join</c> enforce.
    /// </summary>
    public static bool TryNormalizeNickname(
        string? nickname,
        [NotNullWhen(true)] out string? normalizedNickname,
        [NotNullWhen(false)] out string? error)
    {
        if (!TryNormalize(
                StripInvisibleCharacters(nickname), "nickname", MaxNicknameLength,
                out var trimmedNickname, out error))
        {
            normalizedNickname = null;
            return false;
        }

        if (ReservedNicknames.IsReserved(trimmedNickname))
        {
            normalizedNickname = null;
            error = "'nickname' is reserved.";
            return false;
        }

        normalizedNickname = trimmedNickname;
        error = null;
        return true;
    }

    /// <summary>
    /// Folds compatibility characters to their canonical form (e.g. full-width Latin letters)
    /// and drops zero-width/formatting characters (e.g. U+200B ZERO WIDTH SPACE, U+202E
    /// RIGHT-TO-LEFT OVERRIDE). Without this, "cl​aude" with a zero-width space inserted reads as
    /// "claude" to a human but does not string-match it, silently defeating the reserved-name
    /// check and, the same way, letting two visually identical nicknames coexist in a room.
    /// </summary>
    private static string? StripInvisibleCharacters(string? value)
    {
        if (value is null)
        {
            return null;
        }

        var normalized = value.Normalize(NormalizationForm.FormKC);
        var builder = new StringBuilder(normalized.Length);

        foreach (var rune in normalized.EnumerateRunes())
        {
            if (CharUnicodeInfo.GetUnicodeCategory(rune) != UnicodeCategory.Format)
            {
                builder.Append(rune);
            }
        }

        return builder.ToString();
    }

    private static bool TryNormalize(
        string? value,
        string fieldName,
        int maxLength,
        [NotNullWhen(true)] out string? normalized,
        [NotNullWhen(false)] out string? error)
    {
        normalized = value?.Trim();

        if (string.IsNullOrEmpty(normalized))
        {
            normalized = null;
            error = $"'{fieldName}' must not be empty.";
            return false;
        }

        if (normalized.Length > maxLength)
        {
            normalized = null;
            error = $"'{fieldName}' must not exceed {maxLength} characters.";
            return false;
        }

        error = null;
        return true;
    }
}
