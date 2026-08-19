namespace RedeTim.Backend;

/// <summary>The body accepted by <c>POST /api/messages</c>.</summary>
public sealed record SendMessageRequest(string? Room, string? Nickname, string? Text);
