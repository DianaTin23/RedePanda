using System.Collections.Concurrent;

namespace RedeTim.Backend;

/// <summary>
/// The in-memory projection of the presence topic this pod has consumed. Read-only from the
/// caller's side; <see cref="PresenceConsumerService"/> is the only writer.
/// </summary>
public sealed class PresenceStore
{
    private readonly ConcurrentDictionary<(string Room, string Nickname), DateTimeOffset> _renewedAt = new();
    private readonly TimeSpan _ttl;

    public PresenceStore(BackendOptions options)
    {
        _ttl = TimeSpan.FromSeconds(options.PresenceTtlSeconds);
    }

    /// <summary>Whether a live (not TTL-expired) reservation for (room, nickname) exists.</summary>
    public bool IsTaken(string room, string nickname, DateTimeOffset now) =>
        _renewedAt.TryGetValue((room, nickname), out var renewedAt) && now - renewedAt < _ttl;

    /// <summary>
    /// Records a heartbeat. Keeps the later of the existing and the new timestamp, so an
    /// out-of-order replay can never move a reservation's clock backwards.
    /// </summary>
    public void Apply(string room, string nickname, DateTimeOffset renewedAt)
    {
        var key = (room, nickname);
        _renewedAt.AddOrUpdate(
            key,
            renewedAt,
            (_, existing) => renewedAt > existing ? renewedAt : existing);
    }

    /// <summary>Frees a reservation immediately, from a tombstone on the presence topic.</summary>
    public void Remove(string room, string nickname) => _renewedAt.TryRemove((room, nickname), out _);
}
