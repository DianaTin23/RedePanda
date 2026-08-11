using System.Diagnostics.CodeAnalysis;

namespace RedePanda.Contracts;

/// <summary>
/// A single chat message as it travels over the Kafka topic.
/// <para>
/// The Kafka record key is <see cref="Room"/>, so all messages of one room land on the same
/// partition and keep their order even if the topic is ever repartitioned.
/// </para>
/// </summary>
public sealed record ChatMessage(string Room, string Nickname, string Text, DateTimeOffset Timestamp)
{
    public const int MaxRoomLength = 64;
    public const int MaxNicknameLength = 32;
    public const int DefaultMaxTextLength = 500;

    /// <summary>
    /// Validates untrusted input and builds a <see cref="ChatMessage"/> from it.
    /// <para>
    /// The timestamp is a parameter rather than client input on purpose: the server passes its
    /// own clock, so a timestamp sent by a client can never reach the topic.
    /// </para>
    /// </summary>
    /// <returns><c>true</c> when <paramref name="message"/> was built; otherwise <c>false</c> and
    /// <paramref name="error"/> explains why.</returns>
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

        // Length is checked after trimming so trailing whitespace cannot be used to trip the limit.
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
