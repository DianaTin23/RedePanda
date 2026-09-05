namespace RedeTim.Backend;

public sealed record BackendOptions
{
    public required string BootstrapServers { get; init; }
    public required string Topic { get; init; }
    public required int MaxMessageLength { get; init; }
    public required string PodName { get; init; }

    public required int ProduceTimeoutMs { get; init; }

    public const int DefaultProduceTimeoutMs = 10_000;

    public required int HistorySize { get; init; }

    public const int DefaultHistorySize = 200;

    public required int MaxRooms { get; init; }

    public const int DefaultMaxRooms = 200;

    public required int ReplayRecords { get; init; }

    public const int DefaultReplayRecords = 2_000;

    public required string PresenceTopic { get; init; }

    public required int PresenceTtlSeconds { get; init; }

    // Three times the SSE heartbeat interval, so one missed beat never evicts a connected user.
    public const int DefaultPresenceTtlSeconds = 45;

    public required LogLevel LogLevel { get; init; }

    public const LogLevel DefaultLogLevel = Microsoft.Extensions.Logging.LogLevel.Information;

    public string ConsumerGroupId => $"redetim-backend-{PodName}";

    public string PresenceConsumerGroupId => $"redetim-presence-{PodName}";

    public static BackendOptions FromEnvironment()
    {
        return new BackendOptions
        {
            BootstrapServers = Read("REDPANDA_BOOTSTRAP_SERVERS", "redpanda:9092"),
            Topic = Read("REDPANDA_TOPIC", "redetim-chat"),
            MaxMessageLength = ReadInt("MAX_MESSAGE_LENGTH", Contracts.ChatMessage.DefaultMaxTextLength),
            HistorySize = ReadInt("CHAT_HISTORY_SIZE", DefaultHistorySize, allowZero: true),
            MaxRooms = ReadInt("CHAT_MAX_ROOMS", DefaultMaxRooms, allowZero: true),
            ReplayRecords = ReadInt("CHAT_REPLAY_RECORDS", DefaultReplayRecords, allowZero: true),
            PresenceTopic = Read("REDPANDA_PRESENCE_TOPIC", "redetim-presence"),
            PresenceTtlSeconds = ReadInt("PRESENCE_TTL_SECONDS", DefaultPresenceTtlSeconds),
            ProduceTimeoutMs = ReadInt("PRODUCE_TIMEOUT_MS", DefaultProduceTimeoutMs),
            LogLevel = ReadLogLevel("LOG_LEVEL", DefaultLogLevel),

            PodName = ResolvePodName(
                Environment.GetEnvironmentVariable("POD_NAME"),
                Environment.GetEnvironmentVariable("KUBERNETES_SERVICE_HOST"),
                Environment.MachineName,
                Environment.ProcessId),
        };
    }

    internal static string ResolvePodName(
        string? podName, string? kubernetesServiceHost, string machineName, int processId)
    {
        if (!string.IsNullOrWhiteSpace(podName))
        {
            return podName;
        }

        if (!string.IsNullOrWhiteSpace(kubernetesServiceHost))
        {
            throw new InvalidOperationException(
                "POD_NAME is not set, but this process is running in Kubernetes. It must come " +
                "from a fieldRef on metadata.name: without it every replica would share one " +
                "consumer group, and all but one of them would stop receiving messages.");
        }

        return $"{machineName}-{processId}";
    }

    private static string Read(string key, string fallback)
    {
        var value = Environment.GetEnvironmentVariable(key);
        return string.IsNullOrWhiteSpace(value) ? fallback : value;
    }

    internal static LogLevel ReadLogLevel(string key, LogLevel fallback, string? raw = null)
    {
        raw ??= Environment.GetEnvironmentVariable(key);
        if (string.IsNullOrWhiteSpace(raw))
        {
            return fallback;
        }

        if (!Enum.TryParse<LogLevel>(raw.Trim(), ignoreCase: true, out var level) ||
            !Enum.IsDefined(level))
        {
            throw new InvalidOperationException(
                $"{key} is '{raw}', which is not a known level. Accepted: " +
                $"{string.Join(", ", Enum.GetNames<LogLevel>())}.");
        }

        return level;
    }

    private static int ReadInt(string key, int fallback, bool allowZero = false)
    {
        var raw = Environment.GetEnvironmentVariable(key);
        if (string.IsNullOrWhiteSpace(raw))
        {
            return fallback;
        }

        if (!int.TryParse(raw, out var value) || value < 0 || (value == 0 && !allowZero))
        {
            var expected = allowZero ? "a non-negative integer" : "a positive integer";
            throw new InvalidOperationException($"{key} must be {expected}, but was '{raw}'.");
        }

        return value;
    }
}
