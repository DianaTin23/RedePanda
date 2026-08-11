namespace RedePanda.Backend;

/// <summary>
/// The body accepted by <c>POST /api/messages</c>.
/// <para>
/// There is deliberately no timestamp field. The server stamps every message from its own clock,
/// so a client cannot backdate or forward-date a message even by sending one — the value simply
/// has nowhere to bind to.
/// </para>
/// </summary>
public sealed record SendMessageRequest(string? Room, string? Nickname, string? Text);
