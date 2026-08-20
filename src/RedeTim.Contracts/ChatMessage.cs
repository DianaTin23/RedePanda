using System.Diagnostics.CodeAnalysis;

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

        if (!TryNormalize(room, "room", MaxRoomLength, out normalizedRoom, out error))
        {
            return false;
        }

        if (!TryNormalize(nickname, "nickname", MaxNicknameLength, out var trimmedNickname, out error))
        {
            return false;
        }

        if (ReservedNicknames.IsReserved(trimmedNickname))
        {
            normalizedRoom = null;
            error = "'nickname' is reserved.";
            return false;
        }

        normalizedNickname = trimmedNickname;
        error = null;
        return true;
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
