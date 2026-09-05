namespace RedeTim.Backend;

internal sealed record PresenceRecord(string Room, string Nickname, string PodName, DateTimeOffset RenewedAt);
