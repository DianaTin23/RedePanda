namespace RedeTim.Contracts;

public static class ReservedNicknames
{
    private static readonly HashSet<string> Names = new(StringComparer.OrdinalIgnoreCase)
    {
        "claude",
    };

    public static bool IsReserved(string nickname) => Names.Contains(nickname);
}
