namespace RedeTim.Contracts;

/// <summary>Nicknames no user may take because they belong to a non-human participant.</summary>
public static class ReservedNicknames
{
    private static readonly HashSet<string> Names = new(StringComparer.OrdinalIgnoreCase)
    {
        "claude",
    };

    /// <summary>Whether <paramref name="nickname"/> is an exact, case-insensitive match for a reserved name.</summary>
    public static bool IsReserved(string nickname) => Names.Contains(nickname);
}
