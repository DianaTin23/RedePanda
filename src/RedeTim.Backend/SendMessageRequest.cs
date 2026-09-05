namespace RedeTim.Backend;

public sealed record SendMessageRequest(string? Room, string? Nickname, string? Text);
