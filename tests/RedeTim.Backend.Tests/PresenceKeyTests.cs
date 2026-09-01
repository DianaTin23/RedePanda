namespace RedeTim.Backend.Tests;

public class PresenceKeyTests
{
    [Theory]
    [InlineData("general", "alice")]
    [InlineData("with|pipe", "with|pipe")]
    [InlineData("with\"quote", "with\\backslash")]
    [InlineData("mit ümlaut", "😀 emoji")]
    public void EncodingThenDecodingRoundTrips(string room, string nickname)
    {
        var key = PresenceKey.Encode(room, nickname);

        Assert.True(PresenceKey.TryDecode(key, out var decodedRoom, out var decodedNickname));
        Assert.Equal(room, decodedRoom);
        Assert.Equal(nickname, decodedNickname);
    }

    [Theory]
    [InlineData("")]
    [InlineData("not json")]
    [InlineData("{\"Room\":\"general\"}")]
    [InlineData("{\"Room\":\"\",\"Nickname\":\"alice\"}")]
    public void GarbageOrIncompleteKeysAreRejected(string key)
    {
        Assert.False(PresenceKey.TryDecode(key, out var room, out var nickname));
        Assert.Null(room);
        Assert.Null(nickname);
    }

    [Fact]
    public void DifferentPairsEncodeDifferently()
    {
        var first = PresenceKey.Encode("general", "alice");
        var second = PresenceKey.Encode("general", "bob");

        Assert.NotEqual(first, second);
    }
}
