using RedePanda.Contracts;

namespace RedePanda.Backend.Tests;

internal static class TestOptions
{
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
