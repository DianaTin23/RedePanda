namespace RedeTim.Backend;

/// <summary>A presence heartbeat as it travels over the presence topic. A tombstone (null value) means "left".</summary>
internal sealed record PresenceRecord(string Room, string Nickname, string PodName, DateTimeOffset RenewedAt);
