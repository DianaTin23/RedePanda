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

        if (!TryNormalize(room, "room", MaxRoomLength, out var normalizedRoom, out error))
        {
            return false;
        }

        if (!TryNormalize(nickname, "nickname", MaxNicknameLength, out var normalizedNickname, out error))
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
