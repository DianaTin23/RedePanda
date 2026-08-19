using System.Text.Json;
using RedeTim.Contracts;

namespace RedeTim.Backend.Tests;

public class SendMessageRequestTests
{
    private static readonly JsonSerializerOptions WebOptions = new(JsonSerializerDefaults.Web);

    [Fact]
    public void ClientSuppliedTimestampIsIgnoredAndReplacedByTheServerClock()
    {
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
