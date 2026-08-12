namespace RedePanda.Backend;

/// <summary>
/// Every setting the application itself owns, read straight from the environment.
/// <para>
/// ASP.NET's configuration binder expects <c>Section__Key</c>; the assignment asks for plain
/// names such as <c>REDPANDA_TOPIC</c>, so these are read explicitly instead of relying on
/// auto-binding. That also keeps this class and the README configuration table in step.
/// </para>
/// <para>
/// <c>OTEL_*</c> variables are deliberately absent: they are part of the OpenTelemetry
/// specification and the SDK reads them itself. Re-reading them here would create a second
/// source of truth.
/// </para>
/// </summary>
public sealed record BackendOptions
{
    public required string BootstrapServers { get; init; }
    public required string Topic { get; init; }
    public required int MaxMessageLength { get; init; }
    public required string PodName { get; init; }

    /// <summary>
    /// Messages kept per room and replayed to a browser on join, or <c>0</c> for everything the
    /// topic still holds. Zero is the default: the history is then bounded by the broker's
    /// retention alone, which is what makes a room look like a real chat after a redeploy — at the
    /// price of a buffer that grows with the topic. The limit exists for the day that price is
    /// too high.
    /// </summary>
    public required int HistorySize { get; init; }

    /// <summary>Consumer group id. Unique per pod on purpose: each pod then receives every
    /// message (fan-out) instead of the pods splitting the partitions between them, which is
    /// what lets more than one replica serve browsers correctly.</summary>
    public string ConsumerGroupId => $"redepanda-backend-{PodName}";

    public static BackendOptions FromEnvironment()
    {
        return new BackendOptions
        {
            BootstrapServers = Read("REDPANDA_BOOTSTRAP_SERVERS", "redpanda:9092"),
            Topic = Read("REDPANDA_TOPIC", "redepanda-chat"),
            MaxMessageLength = ReadInt("MAX_MESSAGE_LENGTH", Contracts.ChatMessage.DefaultMaxTextLength),
            HistorySize = ReadInt("CHAT_HISTORY_SIZE", 0, allowZero: true),

            // Supplied via fieldRef in the Deployment; the machine name keeps it useful locally.
            PodName = Read("POD_NAME", Environment.MachineName),
        };
    }

    private static string Read(string key, string fallback)
    {
        var value = Environment.GetEnvironmentVariable(key);
        return string.IsNullOrWhiteSpace(value) ? fallback : value;
    }

    /// <param name="allowZero">
    /// Set where 0 is a meaningful setting rather than a typo — <c>CHAT_HISTORY_SIZE=0</c> means
    /// "keep everything", while <c>MAX_MESSAGE_LENGTH=0</c> would silently reject every message.
    /// </param>
    private static int ReadInt(string key, int fallback, bool allowZero = false)
    {
        var raw = Environment.GetEnvironmentVariable(key);
        if (string.IsNullOrWhiteSpace(raw))
        {
            return fallback;
        }

        // A typo here would otherwise silently reject every message, so fail loudly at startup.
        if (!int.TryParse(raw, out var value) || value < 0 || (value == 0 && !allowZero))
        {
            var expected = allowZero ? "a non-negative integer" : "a positive integer";
            throw new InvalidOperationException($"{key} must be {expected}, but was '{raw}'.");
        }

        return value;
    }
}
