namespace RedeTim.Backend.Tests;

// Records what the endpoints and the stream ask of the presence producer, so a test can assert
// on it without a broker. Both lists are guarded: the SSE stream renews from its own task.
internal sealed class FakePresenceProducer : IPresenceProducer
{
    public List<(string Room, string Nickname)> Renewals { get; } = [];

    public List<(string Room, string Nickname)> Releases { get; } = [];

    public Task RenewAsync(string room, string nickname, CancellationToken cancellationToken)
    {
        lock (Renewals)
        {
            Renewals.Add((room, nickname));
        }

        return Task.CompletedTask;
    }

    public Task ReleaseAsync(string room, string nickname, CancellationToken cancellationToken)
    {
        lock (Releases)
        {
            Releases.Add((room, nickname));
        }

        return Task.CompletedTask;
    }
}
