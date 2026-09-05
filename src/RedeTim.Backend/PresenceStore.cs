using System.Collections.Concurrent;

namespace RedeTim.Backend;

public sealed class PresenceStore
{
    private readonly ConcurrentDictionary<(string Room, string Nickname), DateTimeOffset> _renewedAt = new();
    private readonly TimeSpan _ttl;

    // Ticks, not DateTimeOffset: Interlocked needs a primitive, and whichever request thread
    // reaches the sweep first reads and writes this.
    private long _nextSweepDueTicks;

    public PresenceStore(BackendOptions options)
    {
        _ttl = TimeSpan.FromSeconds(options.PresenceTtlSeconds);
    }

    internal int Count => _renewedAt.Count;

    public bool IsTaken(string room, string nickname, DateTimeOffset now)
    {
        SweepExpiredIfDue(now);
        return _renewedAt.TryGetValue((room, nickname), out var renewedAt) && now - renewedAt < _ttl;
    }

    public IReadOnlyList<string> ActiveNicknames(string room, DateTimeOffset now)
    {
        SweepExpiredIfDue(now);
        return _renewedAt
            .Where(entry => entry.Key.Room == room && now - entry.Value < _ttl)
            .Select(entry => entry.Key.Nickname)
            .OrderBy(nickname => nickname, StringComparer.Ordinal)
            .ToList();
    }

    public void Apply(string room, string nickname, DateTimeOffset renewedAt)
    {
        var key = (room, nickname);
        _renewedAt.AddOrUpdate(
            key,
            renewedAt,
            (_, existing) => renewedAt > existing ? renewedAt : existing);
    }

    public void Remove(string room, string nickname) => _renewedAt.TryRemove((room, nickname), out _);

    // Piggy-backed on the read paths, no timer. See docs/kafka.md#presence-topic.
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
