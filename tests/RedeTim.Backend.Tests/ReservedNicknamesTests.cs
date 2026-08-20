using RedeTim.Contracts;

namespace RedeTim.Backend.Tests;

public class ReservedNicknamesTests
{
    [Theory]
    [InlineData("claude")]
    [InlineData("Claude")]
    [InlineData("CLAUDE")]
    [InlineData("ClAuDe")]
    public void ClaudeIsReservedRegardlessOfCase(string nickname)
    {
        Assert.True(ReservedNicknames.IsReserved(nickname));
    }

    [Theory]
    [InlineData("claudia")]
    [InlineData("claude2")]
    [InlineData("the-claude")]
    [InlineData("alice")]
    public void NamesThatMerelyContainItAreNotReserved(string nickname)
    {
        Assert.False(ReservedNicknames.IsReserved(nickname));
    }
}
