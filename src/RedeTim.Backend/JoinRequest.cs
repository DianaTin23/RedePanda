namespace RedeTim.Backend;

/// <summary>The body accepted by <c>POST /api/join</c>.</summary>
public sealed record JoinRequest(string? Room, string? Nickname);
