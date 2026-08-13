using RedePanda.Contracts;

namespace RedePanda.Backend.Tests;

/// <summary>
/// Builds the <see cref="BackendOptions"/> the broadcaster needs.
/// <para>
/// Shared rather than duplicated per file — the convention everywhere else in this project — only
/// because the record has required members that have nothing to do with what these tests are
/// about: none of the broker settings are ever read by a broadcaster.
/// </para>
/// </summary>
internal static class TestOptions
{
    /// <param name="historySize">Messages kept per room; 0 keeps everything.</param>
    public static BackendOptions Create(int historySize = 0) => new()
    {
        BootstrapServers = "broker-that-is-never-contacted:9092",
        Topic = "unused-in-these-tests",
        MaxMessageLength = ChatMessage.DefaultMaxTextLength,
        PodName = "test",
        HistorySize = historySize,
        MaxRooms = BackendOptions.DefaultMaxRooms,
        ReplayRecords = BackendOptions.DefaultReplayRecords,
        ProduceTimeoutMs = BackendOptions.DefaultProduceTimeoutMs,
        LogLevel = BackendOptions.DefaultLogLevel,
    };
}
