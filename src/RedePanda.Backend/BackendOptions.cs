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
    /// How long a single message may spend inside the producer before it is given up on.
    /// <para>
    /// This is librdkafka's <c>message.timeout.ms</c>, whose default is <b>300 000</b> — five
    /// minutes. That default is written for a background pipeline, and this producer sits in the
    /// request path of an HTTP POST: against an unreachable broker the browser would hold an open
    /// request, with its composer waiting on it, for five minutes before learning what the first
    /// few seconds already decided.
    /// </para>
    /// </summary>
    public required int ProduceTimeoutMs { get; init; }

    /// <summary>
    /// Long enough to survive a leader election or a broker restart, short enough that a user
    /// sees an error rather than a frozen composer.
    /// </summary>
    public const int DefaultProduceTimeoutMs = 10_000;

    /// <summary>
    /// Messages kept per room and replayed to a browser on join, or <c>0</c> for everything the
    /// topic still holds.
    /// <para>
    /// Bounded by default, and no longer <c>0</c>: this buffer lives in the memory of <b>every</b>
    /// replica. Unbounded it grows with the broker's retention, and an autoscaler then multiplies
    /// it by the replica count exactly when the pods are already under load. Zero stays legal and
    /// still means "everything the topic holds".
    /// </para>
    /// </summary>
    public required int HistorySize { get; init; }

    /// <summary>
    /// Roughly a screenful of scrollback. At the maximum message length that is about 240 KB per
    /// room, two orders of magnitude below the 512Mi the chart gives the pod — and it stays that
    /// way however many replicas the autoscaler decides to run.
    /// <para>
    /// It bounds memory, not startup time: the consumer still replays the whole topic before the
    /// pod reports ready.
    /// </para>
    /// </summary>
    public const int DefaultHistorySize = 200;

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
            HistorySize = ReadInt("CHAT_HISTORY_SIZE", DefaultHistorySize, allowZero: true),
            ProduceTimeoutMs = ReadInt("PRODUCE_TIMEOUT_MS", DefaultProduceTimeoutMs),

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
