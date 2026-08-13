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
    /// </summary>
    public const int DefaultHistorySize = 200;

    /// <summary>
    /// How many rooms a replica keeps a history for at once, or <c>0</c> for as many as arrive.
    /// <para>
    /// <see cref="HistorySize"/> bounds a room; this bounds how many rooms there can be. Without it
    /// the buffer is only half bounded, and the unbounded half is the one nobody outside chooses:
    /// a room is created by naming it in a query string or in a message, so the number of them is
    /// set by whoever is talking to the pod rather than by configuration. Every replica holds every
    /// room, because every replica consumes the whole topic.
    /// </para>
    /// <para>
    /// The room whose last message is oldest is dropped to make space. What is lost is what that
    /// pod can replay on a join; the messages are still in the topic, which is the same trade
    /// <see cref="ReplayRecords"/> already makes at startup.
    /// </para>
    /// </summary>
    public required int MaxRooms { get; init; }

    /// <summary>
    /// At <see cref="DefaultHistorySize"/> messages of the maximum length this is about 48 MB per
    /// replica in the worst case — an order of magnitude below the 512Mi the chart gives the pod,
    /// and unlike the old behaviour it stays there however many rooms are named.
    /// </summary>
    public const int DefaultMaxRooms = 200;

    /// <summary>
    /// How many records back a starting pod reads <b>per partition</b> before it reports ready, or
    /// <c>0</c> for everything the broker still holds.
    /// <para>
    /// Deliberately a separate setting from <see cref="HistorySize"/> rather than derived from it,
    /// because the two count different things and conflating them hides the difference.
    /// <see cref="HistorySize"/> is per <em>room</em> and bounds memory; this is per
    /// <em>partition</em> and bounds startup. A pod that read back exactly
    /// <see cref="HistorySize"/> records would under-fill every room as soon as more than one was
    /// busy, and nothing in the name would have warned anyone.
    /// </para>
    /// <para>
    /// It exists because every replica used to replay the entire topic before becoming ready, so
    /// both startup time and broker read load grew with the topic <em>and</em> with the replica
    /// count — worst exactly when an autoscaler adds pods because the pods are already loaded.
    /// </para>
    /// </summary>
    public required int ReplayRecords { get; init; }

    /// <summary>
    /// Ten times the default per-room history, so several busy rooms can each still fill their
    /// buffer from one replay. At the maximum message length this is about 1 MB per partition.
    /// </summary>
    public const int DefaultReplayRecords = 2_000;

    /// <summary>
    /// Minimum level for everything this process logs.
    /// <para>
    /// Read here rather than inline in <c>Program.cs</c> so this record is what its own summary
    /// claims to be — every setting the application owns — and so a misspelling fails at startup
    /// like every other setting instead of silently falling back to Information, which is the one
    /// way a configuration mistake here could hide the evidence of itself.
    /// </para>
    /// </summary>
    public required LogLevel LogLevel { get; init; }

    /// <summary>Loud enough to explain the pod's own lifecycle, quiet enough to read.</summary>
    public const LogLevel DefaultLogLevel = Microsoft.Extensions.Logging.LogLevel.Information;

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

    /// <summary>
    /// The pod's identity, and with it the consumer group id. Two replicas that share a group id
    /// do not share the load, they silence each other: Kafka gives the single partition to one of
    /// them and every browser attached to the others waits in a room that never updates.
    /// <para>
    /// In a cluster the name comes from a <c>fieldRef</c> on <c>metadata.name</c>, which the API
    /// server guarantees to be unique in the namespace. A missing value there is a broken
    /// Deployment, not a situation to paper over with a fallback — so it throws, and the pod
    /// crash-loops with a message instead of joining the fan-out and quietly breaking it.
    /// </para>
    /// <para>
    /// Outside a cluster the same collision is reachable by running the backend twice on one
    /// machine, which is exactly how the fan-out gets tried locally. The process id separates the
    /// two while keeping the name readable in <c>rpk group list</c>.
    /// </para>
    /// </summary>
    internal static string ResolvePodName(
        string? podName, string? kubernetesServiceHost, string machineName, int processId)
    {
        if (!string.IsNullOrWhiteSpace(podName))
        {
            return podName;
        }

        // Set by the kubelet in every pod, and by nothing else.
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

    /// <summary>
    /// <c>Enum.TryParse</c> on its own is not enough: it accepts any number that fits the
    /// underlying type, so <c>LOG_LEVEL=99</c> would parse cleanly and silence the process
    /// entirely. <c>Enum.IsDefined</c> is what turns that into an error.
    /// </summary>
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
