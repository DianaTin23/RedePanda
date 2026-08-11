using System.Text.Json;
using RedePanda.Contracts;

namespace RedePanda.Backend.Tests;

/// <summary>
/// A client must not be able to choose its own message timestamp. That is enforced structurally
/// rather than by a check: the request type has no timestamp field for one to bind to.
/// </summary>
public class SendMessageRequestTests
{
    // Matches what minimal APIs use to bind a request body.
    private static readonly JsonSerializerOptions WebOptions = new(JsonSerializerDefaults.Web);

    [Fact]
    public void ClientSuppliedTimestampIsIgnoredAndReplacedByTheServerClock()
    {
        // A client trying to backdate a message by 27 years.
        const string body = """
            {"room":"general","nickname":"alice","text":"hallo","timestamp":"1999-01-01T00:00:00Z"}
            """;

        var request = JsonSerializer.Deserialize<SendMessageRequest>(body, WebOptions);
        Assert.NotNull(request);

        var serverClock = new DateTimeOffset(2026, 8, 11, 12, 0, 0, TimeSpan.Zero);
        var ok = ChatMessage.TryCreate(
            request.Room, request.Nickname, request.Text,
            serverClock, ChatMessage.DefaultMaxTextLength,
            out var message, out _);

        Assert.True(ok);
        Assert.NotNull(message);
        Assert.Equal(serverClock, message.Timestamp);
        Assert.NotEqual(1999, message.Timestamp.Year);
    }

    [Fact]
    public void RequestTypeExposesNoTimestampMember()
    {
        // Guards the design itself: adding a Timestamp property would silently reintroduce the
        // ability for a client to set it, and the test above would still pass.
        Assert.DoesNotContain(
            typeof(SendMessageRequest).GetProperties(),
            p => p.Name.Contains("time", StringComparison.OrdinalIgnoreCase));
    }

    [Fact]
    public void MissingFieldsBindAsNullAndAreThenRejected()
    {
        var request = JsonSerializer.Deserialize<SendMessageRequest>("{}", WebOptions);
        Assert.NotNull(request);

        var ok = ChatMessage.TryCreate(
            request.Room, request.Nickname, request.Text,
            DateTimeOffset.UtcNow, ChatMessage.DefaultMaxTextLength,
            out _, out var error);

        Assert.False(ok);
        Assert.NotNull(error);
    }
}
