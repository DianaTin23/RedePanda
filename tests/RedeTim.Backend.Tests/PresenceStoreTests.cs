namespace RedeTim.Backend.Tests;

public class PresenceStoreTests
{
    private static readonly DateTimeOffset Now = new(2026, 8, 20, 12, 0, 0, TimeSpan.Zero);

    private static PresenceStore CreateStore(int ttlSeconds = 45) =>
        new(TestOptions.Create() with { PresenceTtlSeconds = ttlSeconds });

    [Fact]
    public void AFreshRenewalIsTaken()
    {
        var store = CreateStore();

        store.Apply("general", "alice", Now);

        Assert.True(store.IsTaken("general", "alice", Now));
    }

    [Fact]
    public void ANameNeverRenewedIsFree()
    {
        var store = CreateStore();

        Assert.False(store.IsTaken("general", "alice", Now));
    }

    [Fact]
    public void ARenewalOlderThanTheTtlIsFreeEvenWithoutAnExplicitRemove()
    {
        var store = CreateStore(ttlSeconds: 45);

        store.Apply("general", "alice", Now);

        Assert.False(store.IsTaken("general", "alice", Now + TimeSpan.FromSeconds(46)));
    }

    [Fact]
    public void ARenewalJustUnderTheTtlIsStillTaken()
    {
        var store = CreateStore(ttlSeconds: 45);

        store.Apply("general", "alice", Now);

        Assert.True(store.IsTaken("general", "alice", Now + TimeSpan.FromSeconds(44)));
    }

    [Fact]
    public void RemoveFreesTheNameImmediatelyRegardlessOfFreshness()
    {
        var store = CreateStore();

        store.Apply("general", "alice", Now);
        store.Remove("general", "alice");

        Assert.False(store.IsTaken("general", "alice", Now));
    }

    [Fact]
    public void AnOutOfOrderApplyNeverMovesTheClockBackwards()
    {
        var store = CreateStore(ttlSeconds: 45);

        store.Apply("general", "alice", Now);
        store.Apply("general", "alice", Now - TimeSpan.FromSeconds(30));

        Assert.True(store.IsTaken("general", "alice", Now + TimeSpan.FromSeconds(44)));
    }

    [Fact]
    public void RoomsAreKeptApart()
    {
        var store = CreateStore();

        store.Apply("general", "alice", Now);

        Assert.False(store.IsTaken("andererraum", "alice", Now));
    }
}
