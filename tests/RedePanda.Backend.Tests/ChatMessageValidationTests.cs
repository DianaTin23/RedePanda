using RedePanda.Contracts;

namespace RedePanda.Backend.Tests;

/// <summary>
/// Covers the validation rules that the backend applies to every incoming message. These are
/// the rules the HTTP endpoint depends on, so they are tested here rather than through HTTP.
/// </summary>
public class ChatMessageValidationTests
{
    private static readonly DateTimeOffset ServerClock =
        new(2026, 8, 11, 12, 0, 0, TimeSpan.Zero);

    private const int MaxTextLength = 500;

    private static bool TryCreate(
        string? room, string? nickname, string? text,
        out ChatMessage? message, out string? error) =>
        ChatMessage.TryCreate(room, nickname, text, ServerClock, MaxTextLength, out message, out error);

    [Fact]
    public void ValidMessageIsAccepted()
    {
        var ok = TryCreate("general", "alice", "hallo welt", out var message, out var error);

        Assert.True(ok);
        Assert.Null(error);
        Assert.NotNull(message);
        Assert.Equal("general", message.Room);
        Assert.Equal("alice", message.Nickname);
        Assert.Equal("hallo welt", message.Text);
    }

    [Fact]
    public void SurroundingWhitespaceIsTrimmed()
    {
        var ok = TryCreate("  general  ", "  alice  ", "  hallo  ", out var message, out _);

        Assert.True(ok);
        Assert.NotNull(message);
        Assert.Equal("general", message.Room);
        Assert.Equal("alice", message.Nickname);
        Assert.Equal("hallo", message.Text);
    }

    [Theory]
    // Empty, whitespace-only and null are all rejected, for each of the three fields.
    [InlineData(null, "alice", "hallo", "room")]
    [InlineData("", "alice", "hallo", "room")]
    [InlineData("   ", "alice", "hallo", "room")]
    [InlineData("general", null, "hallo", "nickname")]
    [InlineData("general", "", "hallo", "nickname")]
    [InlineData("general", "   ", "hallo", "nickname")]
    [InlineData("general", "alice", null, "text")]
    [InlineData("general", "alice", "", "text")]
    [InlineData("general", "alice", "   ", "text")]
    public void EmptyFieldsAreRejected(string? room, string? nickname, string? text, string expectedField)
    {
        var ok = TryCreate(room, nickname, text, out var message, out var error);

        Assert.False(ok);
        Assert.Null(message);
        Assert.NotNull(error);
        Assert.Contains(expectedField, error);
    }

    [Fact]
    public void TextOverTheLimitIsRejected()
    {
        var tooLong = new string('x', MaxTextLength + 1);

        var ok = TryCreate("general", "alice", tooLong, out var message, out var error);

        Assert.False(ok);
        Assert.Null(message);
        Assert.Contains("text", error);
        Assert.Contains(MaxTextLength.ToString(), error);
    }

    [Fact]
    public void TextExactlyAtTheLimitIsAccepted()
    {
        var atLimit = new string('x', MaxTextLength);

        var ok = TryCreate("general", "alice", atLimit, out var message, out _);

        Assert.True(ok);
        Assert.NotNull(message);
        Assert.Equal(MaxTextLength, message.Text.Length);
    }

    [Fact]
    public void LengthIsMeasuredAfterTrimming()
    {
        // Padding a limit-length text with whitespace must not push it over the limit.
        var padded = "  " + new string('x', MaxTextLength) + "  ";

        var ok = TryCreate("general", "alice", padded, out var message, out _);

        Assert.True(ok);
        Assert.NotNull(message);
        Assert.Equal(MaxTextLength, message.Text.Length);
    }

    [Fact]
    public void OverlongNicknameAndRoomAreRejected()
    {
        Assert.False(TryCreate(
            "general", new string('n', ChatMessage.MaxNicknameLength + 1), "hallo", out _, out var nickError));
        Assert.Contains("nickname", nickError);

        Assert.False(TryCreate(
            new string('r', ChatMessage.MaxRoomLength + 1), "alice", "hallo", out _, out var roomError));
        Assert.Contains("room", roomError);
    }

    [Fact]
    public void TimestampComesFromTheServerNotTheCaller()
    {
        var ok = TryCreate("general", "alice", "hallo", out var message, out _);

        Assert.True(ok);
        Assert.NotNull(message);

        // The only way a timestamp can enter a ChatMessage is as an explicit argument, which the
        // endpoint supplies from its own clock.
        Assert.Equal(ServerClock, message.Timestamp);
    }
}
