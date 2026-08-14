namespace RedePanda.Backend;

/// <summary>Every setting the application itself owns, read straight from the environment.</summary>
public sealed record BackendOptions
{
    public required string BootstrapServers { get; init; }
    public required string Topic { get; init; }
    public required int MaxMessageLength { get; init; }
    public required string PodName { get; init; }

    /// <summary>librdkafka's <c>message.timeout.ms</c> for the request-path producer.</summary>
    public required int ProduceTimeoutMs { get; init; }

    public const int DefaultProduceTimeoutMs = 10_000;

    /// <summary>Messages kept per room, or <c>0</c> for everything the topic still holds.</summary>
    public required int HistorySize { get; init; }

    public const int DefaultHistorySize = 200;

    /// <summary>Rooms a replica keeps a history for at once, or <c>0</c> for as many as arrive.</summary>
    public required int MaxRooms { get; init; }

    public const int DefaultMaxRooms = 200;

    /// <summary>Records a starting pod reads back per partition, or <c>0</c> for the whole topic.</summary>
    public required int ReplayRecords { get; init; }

    public const int DefaultReplayRecords = 2_000;

    /// <summary>Minimum level for everything this process logs.</summary>
    public required LogLevel LogLevel { get; init; }

    public const LogLevel DefaultLogLevel = Microsoft.Extensions.Logging.LogLevel.Information;

    /// <summary>Consumer group id, unique per pod so every pod receives every message.</summary>
    public string ConsumerGroupId => $"redepanda-backend-{PodName}";

    public static BackendOptions FromEnvironment()
    {
        return new BackendOptions
        {
            BootstrapServers = Read("REDPANDA_BOOTSTRAP_SERVERS", "redpanda:9092"),
            Topic = Read("REDPANDA_TOPIC", "redepanda-chat"),
            MaxMessageLength = ReadInt("MAX_MESSAGE_LENGTH", Contracts.ChatMessage.DefaultMaxTextLength),
            HistorySize = ReadInt("CHAT_HISTORY_SIZE", DefaultHistorySize, allowZero: true),
            MaxRooms = ReadInt("CHAT_MAX_ROOMS", DefaultMaxRooms, allowZero: true),
            ReplayRecords = ReadInt("CHAT_REPLAY_RECORDS", DefaultReplayRecords, allowZero: true),
            ProduceTimeoutMs = ReadInt("PRODUCE_TIMEOUT_MS", DefaultProduceTimeoutMs),
            LogLevel = ReadLogLevel("LOG_LEVEL", DefaultLogLevel),

            PodName = ResolvePodName(
                Environment.GetEnvironmentVariable("POD_NAME"),
                Environment.GetEnvironmentVariable("KUBERNETES_SERVICE_HOST"),
                Environment.MachineName,
                Environment.ProcessId),
        };
    }

    /// <summary>The pod's identity, and with it the consumer group id.</summary>
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

    /// <summary>Reads a log level, rejecting values that are not defined members of the enum.</summary>
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
