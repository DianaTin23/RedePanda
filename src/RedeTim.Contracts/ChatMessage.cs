using System.Diagnostics.CodeAnalysis;
using System.Globalization;
using System.Text;

namespace RedeTim.Contracts;

public sealed record ChatMessage(string Room, string Nickname, string Text, DateTimeOffset Timestamp)
{
    public const int MaxRoomLength = 64;
    public const int MaxNicknameLength = 32;
    public const int DefaultMaxTextLength = 500;

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

    public static bool TryNormalizeRoom(
        string? room,
        [NotNullWhen(true)] out string? normalizedRoom,
        [NotNullWhen(false)] out string? error) =>
        TryNormalize(StripInvisibleCharacters(room), "room", MaxRoomLength, out normalizedRoom, out error);

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

    // Folds compatibility forms and drops zero-width and RTL-override characters: without this a
    // disguised variant of a reserved or taken nickname slips past the checks below.
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
            if (Rune.GetUnicodeCategory(rune) != UnicodeCategory.Format)
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
