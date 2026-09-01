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

    // Ticks (DateTimeOffset.UtcTicks), not a DateTimeOffset -- Interlocked only works on the
    // primitive types, and this is read/written from whichever request thread happens to reach
    // the sweep first.
    private long _nextSweepDueTicks;

    public PresenceStore(BackendOptions options)
    {
        _ttl = TimeSpan.FromSeconds(options.PresenceTtlSeconds);
    }

    /// <summary>Live entries currently held, whether or not their TTL has lapsed. Test-only.</summary>
    internal int Count => _renewedAt.Count;

    /// <summary>Whether a live (not TTL-expired) reservation for (room, nickname) exists.</summary>
    public bool IsTaken(string room, string nickname, DateTimeOffset now)
    {
        SweepExpiredIfDue(now);
        return _renewedAt.TryGetValue((room, nickname), out var renewedAt) && now - renewedAt < _ttl;
    }

    /// <summary>The nicknames currently (not TTL-expired) active in a room, for display only.</summary>
    public IReadOnlyList<string> ActiveNicknames(string room, DateTimeOffset now)
    {
        SweepExpiredIfDue(now);
        return _renewedAt
            .Where(entry => entry.Key.Room == room && now - entry.Value < _ttl)
            .Select(entry => entry.Key.Nickname)
            .OrderBy(nickname => nickname, StringComparer.Ordinal)
            .ToList();
    }

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

    /// <summary>
    /// Physically drops entries whose TTL has lapsed. <see cref="IsTaken"/> and
    /// <see cref="ActiveNicknames"/> already treat a stale entry as gone, but without this the
    /// dictionary itself only ever shrinks via <see cref="Remove"/> -- a tombstone from a clean
    /// <c>POST /api/join</c>-then-leave. A crashed tab or dropped connection never produces one,
    /// so its row would sit in memory forever; a steady trickle of short-lived joins under
    /// changing nicknames grows the pod's memory without bound. Piggy-backing the sweep on the two
    /// read paths (hit by every <c>/api/join</c> and by the browser's presence poll) means it runs
    /// roughly once per TTL window under any real traffic, with no dedicated timer or thread.
    /// </summary>
    private void SweepExpiredIfDue(DateTimeOffset now)
    {
        var nowTicks = now.UtcTicks;
        var due = Interlocked.Read(ref _nextSweepDueTicks);
        if (nowTicks < due ||
            Interlocked.CompareExchange(ref _nextSweepDueTicks, nowTicks + _ttl.Ticks, due) != due)
        {
            return;
        }

        foreach (var (key, renewedAt) in _renewedAt)
        {
            if (now - renewedAt >= _ttl)
            {
                // The conditional (key AND value) overload guards against a renewal that lands
                // between the snapshot above and this removal from wiping out fresh data.
                ((ICollection<KeyValuePair<(string Room, string Nickname), DateTimeOffset>>)_renewedAt)
                    .Remove(new(key, renewedAt));
            }
        }
    }
}
