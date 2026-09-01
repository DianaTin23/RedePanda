using Confluent.Kafka;
using Confluent.Kafka.Admin;
using RedeTim.Contracts;

namespace RedeTim.ChatClient;

internal static class Program
{
    private const string DefaultBootstrapServers = "redpanda:9092";
    private const string DefaultTopic = "redetim-chat";
    private const string DefaultPresenceTopic = "redetim-presence";
    private const string DefaultRoom = "general";

    public static async Task<int> Main(string[] args)
    {
        if (HasFlag(args, "--help") || HasFlag(args, "-h"))
        {
            PrintUsage();
            return 0;
        }

        var bootstrap = Env("REDPANDA_BOOTSTRAP_SERVERS", DefaultBootstrapServers);
        var topic = GetArg(args, "--topic") ?? Env("REDPANDA_TOPIC", DefaultTopic);

        if (HasFlag(args, "--ensure-topic"))
        {
            return await EnsureTopicAsync(bootstrap, topic);
        }

        if (HasFlag(args, "--describe-topic"))
        {
            return await DescribeTopicAsync(bootstrap, topic);
        }

        if (HasFlag(args, "--print-config"))
        {
            return PrintConfig(bootstrap, topic);
        }

        var room = GetArg(args, "--room") ?? DefaultRoom;
        var nick = GetArg(args, "--nick");
        var showHistory = GetArg(args, "--hist") == "true";
        var newTopic = GetArg(args, "--newTopic");

        if (string.IsNullOrWhiteSpace(nick))
        {
            Console.Error.WriteLine("Missing required argument --nick.");
            PrintUsage();
            return 1;
        }

        if (!string.IsNullOrWhiteSpace(newTopic))
        {
            topic = newTopic;

            if (await EnsureTopicAsync(bootstrap, topic) != 0)
            {
                return 1;
            }
        }

        Console.WriteLine($"Broker    {bootstrap}");
        Console.WriteLine($"Topic     {topic}");
        Console.WriteLine($"Room      {room}");
        Console.WriteLine("Press Ctrl+C to leave the chat.");
        Console.WriteLine();

        using var cts = new CancellationTokenSource();
        Console.CancelKeyPress += (_, e) =>
        {
            e.Cancel = true;
            cts.Cancel();
        };

        using var producer = new Producer(bootstrap, topic);
        using var consumer = new Consumer(bootstrap, topic, showHistory);

        var consumeTask = consumer.RunAsync(cts.Token);

        while (!cts.IsCancellationRequested)
        {
            var line = ReadInput();
            if (string.IsNullOrWhiteSpace(line))
            {
                break;
            }

            if (!ChatMessage.TryCreate(
                    room, nick, line, DateTimeOffset.UtcNow, ChatMessage.DefaultMaxTextLength,
                    out var message, out var error))
            {
                Console.Error.WriteLine($"Not sent: {error}");
                continue;
            }

            await producer.SendAsync(message, cts.Token);
        }

        await cts.CancelAsync();
        await consumeTask;

        Console.WriteLine("Chat was closed, shutting down...");
        return 0;
    }

    private static async Task<int> EnsureTopicAsync(string bootstrap, string topic)
    {
        var partitions = EnvInt("CHAT_PARTITIONS", 1);
        var replicationFactor = EnvInt("CHAT_REPLICATION_FACTOR", 1);
        var waitSeconds = EnvInt("TOPIC_WAIT_SECONDS", 180);

        var presenceTopic = Env("REDPANDA_PRESENCE_TOPIC", DefaultPresenceTopic);
        var presencePartitions = EnvInt("PRESENCE_PARTITIONS", 1);
        var presenceReplicationFactor = EnvInt("PRESENCE_REPLICATION_FACTOR", 1);

        Console.WriteLine(
            $"Ensuring topic '{topic}' on {bootstrap}: {partitions} partition(s), " +
            $"replication factor {replicationFactor}.");
        Console.WriteLine(
            $"Ensuring presence topic '{presenceTopic}': {presencePartitions} partition(s), " +
            $"replication factor {presenceReplicationFactor}, log-compacted.");

        var adminConfig = new AdminClientConfig { BootstrapServers = bootstrap };
        KafkaSecurity.ApplyTo(adminConfig);
        using var adminClient = new AdminClientBuilder(adminConfig).Build();

        if (!await WaitForBrokerAsync(adminClient, TimeSpan.FromSeconds(waitSeconds)))
        {
            Console.Error.WriteLine(
                $"No broker answered on {bootstrap} within {waitSeconds}s. Giving up; the Job " +
                "will be retried.");
            return 1;
        }

        var chatOk = await TryCreateTopicAsync(adminClient, topic, partitions, replicationFactor);
        var presenceOk = await TryCreateTopicAsync(
            adminClient, presenceTopic, presencePartitions, presenceReplicationFactor,
            configs: new Dictionary<string, string> { ["cleanup.policy"] = "compact" });

        return chatOk && presenceOk ? 0 : 1;
    }

    private static async Task<bool> WaitForBrokerAsync(IAdminClient adminClient, TimeSpan budget)
    {
        var deadline = DateTimeOffset.UtcNow + budget;
        var attempt = 0;

        while (true)
        {
            try
            {
                adminClient.GetMetadata(TimeSpan.FromSeconds(5));
                return true;
            }
            catch (KafkaException e)
            {
                if (DateTimeOffset.UtcNow >= deadline)
                {
                    Console.Error.WriteLine($"Last error while waiting: {e.Error.Reason}");
                    return false;
                }

                Console.WriteLine($"Waiting for the broker (attempt {++attempt}): {e.Error.Reason}");
                await Task.Delay(TimeSpan.FromSeconds(3));
            }
        }
    }

    private static async Task<bool> TryCreateTopicAsync(
        IAdminClient adminClient,
        string topic,
        int partitions,
        int replicationFactor,
        Dictionary<string, string>? configs = null)
    {
        try
        {
            await adminClient.CreateTopicsAsync([
                new TopicSpecification
                {
                    Name = topic,
                    NumPartitions = partitions,
                    ReplicationFactor = (short)replicationFactor,
                    Configs = configs,
                }
            ]);
            Console.WriteLine($"Topic '{topic}' created.");
            return true;
        }
        catch (CreateTopicsException e) when (e.Results.All(r => r.Error.Code == ErrorCode.TopicAlreadyExists))
        {
            return await ReconcilePartitionsAsync(adminClient, topic, partitions);
        }
        catch (Exception e)
        {
            Console.Error.WriteLine($"Topic '{topic}' could not be created: {e.Message}");
            return false;
        }
    }

    private static readonly TimeSpan AdminTimeout = TimeSpan.FromSeconds(10);

    private static async Task<int> DescribeTopicAsync(string bootstrap, string topic)
    {
        var adminConfig = new AdminClientConfig { BootstrapServers = bootstrap };
        KafkaSecurity.ApplyTo(adminConfig);
        using var adminClient = new AdminClientBuilder(adminConfig).Build();

        Metadata metadata;
        try
        {
            metadata = adminClient.GetMetadata(topic, AdminTimeout);
        }
        catch (KafkaException e)
        {
            Console.Error.WriteLine($"Could not reach {bootstrap}: {e.Error.Reason}");
            return 1;
        }

        var found = metadata.Topics.FirstOrDefault(t => t.Topic == topic);
        if (found is null || found.Error.IsError)
        {
            Console.Error.WriteLine(
                $"Topic '{topic}' could not be described: {found?.Error.Reason ?? "not found"}");
            return 1;
        }

        Console.WriteLine($"Topic       {topic}");
        Console.WriteLine($"Broker      {bootstrap}");
        Console.WriteLine($"Brokers     {metadata.Brokers.Count}");
        Console.WriteLine($"Partitions  {found.Partitions.Count}");
        Console.WriteLine();

        var consumerConfig = new ConsumerConfig
        {
            BootstrapServers = bootstrap,
            GroupId = "redetim-describe-topic",
        };
        KafkaSecurity.ApplyTo(consumerConfig);
        using var consumer = new ConsumerBuilder<Ignore, Ignore>(consumerConfig).Build();

        Console.WriteLine("PARTITION  LEADER  REPLICAS  LOW        HIGH       MESSAGES");
        long total = 0;
        foreach (var partition in found.Partitions.OrderBy(p => p.PartitionId))
        {
            var replicas = partition.Replicas.Length;
            string low = "?", high = "?", count = "?";
            try
            {
                var watermarks = consumer.QueryWatermarkOffsets(
                    new TopicPartition(topic, new Partition(partition.PartitionId)), AdminTimeout);
                low = watermarks.Low.Value.ToString();
                high = watermarks.High.Value.ToString();

                var available = watermarks.High.Value - watermarks.Low.Value;
                total += available;
                count = available.ToString();
            }
            catch (KafkaException e)
            {
                Console.Error.WriteLine(
                    $"  (watermarks for partition {partition.PartitionId}: {e.Error.Reason})");
            }

            Console.WriteLine(
                $"{partition.PartitionId,9}  {partition.Leader,6}  {replicas,8}  " +
                $"{low,-9}  {high,-9}  {count}");
        }

        Console.WriteLine();
        Console.WriteLine($"Retained    {total} message(s) across all partitions");
        Console.WriteLine();

        try
        {
            var described = await adminClient.DescribeConfigsAsync(
                [new ConfigResource { Type = ResourceType.Topic, Name = topic }]);

            var overrides = described
                .SelectMany(r => r.Entries.Values)
                .Where(e => !e.IsDefault)
                .OrderBy(e => e.Name, StringComparer.Ordinal)
                .ToList();

            Console.WriteLine("Topic configuration (non-default values only):");
            if (overrides.Count == 0)
            {
                Console.WriteLine("  (none -- every setting is at the broker default)");
            }

            foreach (var entry in overrides)
            {
                Console.WriteLine($"  {entry.Name,-32} {entry.Value}");
            }
        }
        catch (KafkaException e)
        {
            Console.WriteLine($"Topic configuration unavailable: {e.Error.Reason}");
        }

        return 0;
    }

    private static int PrintConfig(string bootstrap, string topic)
    {
        Console.WriteLine("Resolved configuration (defaults applied, password masked):");
        Console.WriteLine();
        Console.WriteLine($"  REDPANDA_BOOTSTRAP_SERVERS   {bootstrap}");
        Console.WriteLine($"  REDPANDA_TOPIC               {topic}");
        Console.WriteLine($"  CHAT_PARTITIONS              {EnvInt("CHAT_PARTITIONS", 1)}");
        Console.WriteLine($"  CHAT_REPLICATION_FACTOR      {EnvInt("CHAT_REPLICATION_FACTOR", 1)}");
        Console.WriteLine($"  TOPIC_WAIT_SECONDS           {EnvInt("TOPIC_WAIT_SECONDS", 180)}");
        Console.WriteLine($"  REDPANDA_PRESENCE_TOPIC      {Env("REDPANDA_PRESENCE_TOPIC", DefaultPresenceTopic)}");
        Console.WriteLine($"  PRESENCE_PARTITIONS          {EnvInt("PRESENCE_PARTITIONS", 1)}");
        Console.WriteLine($"  PRESENCE_REPLICATION_FACTOR  {EnvInt("PRESENCE_REPLICATION_FACTOR", 1)}");
        Console.WriteLine();
        Console.WriteLine($"  REDPANDA_SECURITY_PROTOCOL   {Env(KafkaSecurity.ProtocolVariable, "Plaintext (default)")}");
        Console.WriteLine($"  REDPANDA_SASL_MECHANISM      {Env(KafkaSecurity.MechanismVariable, "(unset)")}");
        Console.WriteLine($"  REDPANDA_SASL_USERNAME       {Env(KafkaSecurity.UsernameVariable, "(unset)")}");
        Console.WriteLine($"  REDPANDA_SASL_PASSWORD       {Mask(Environment.GetEnvironmentVariable(KafkaSecurity.PasswordVariable))}");
        Console.WriteLine($"  REDPANDA_SSL_CA_LOCATION     {Env(KafkaSecurity.CaLocationVariable, "(unset)")}");
        Console.WriteLine();

        var probe = new ClientConfig { BootstrapServers = bootstrap };
        try
        {
            KafkaSecurity.ApplyTo(probe);
            Console.WriteLine($"  Effective SecurityProtocol   {probe.SecurityProtocol?.ToString() ?? "Plaintext (librdkafka default, left unset)"}");
            Console.WriteLine($"  Effective SaslMechanism      {probe.SaslMechanism?.ToString() ?? "(none)"}");
            Console.WriteLine($"  Effective SslCaLocation      {probe.SslCaLocation ?? "(system trust store)"}");
        }
        catch (InvalidOperationException e)
        {
            Console.WriteLine($"  Effective settings           REJECTED: {e.Message}");
            return 1;
        }

        return 0;
    }

    private static string Mask(string? value) =>
        string.IsNullOrEmpty(value) ? "(unset)" : $"(set, {value.Length} characters)";

    private static async Task<bool> ReconcilePartitionsAsync(
        IAdminClient adminClient, string topic, int wanted)
    {
        var actual = PartitionCount(adminClient, topic);
        if (actual < 0)
        {
            Console.Error.WriteLine(
                $"Topic '{topic}' exists, but its metadata could not be read, so its partition " +
                "count could not be checked.");
            return false;
        }

        if (actual == wanted)
        {
            Console.WriteLine($"Topic '{topic}' already exists with {actual} partition(s).");
            return true;
        }

        if (actual > wanted)
        {
            Console.Error.WriteLine(
                $"Topic '{topic}' has {actual} partition(s), but {wanted} were requested. A " +
                "partition count cannot be reduced. Set CHAT_PARTITIONS back to " +
                $"{actual} or higher, or delete and recreate the topic -- which discards the whole " +
                "chat history, because the topic is the history.");
            return false;
        }

        Console.WriteLine($"Topic '{topic}' has {actual} partition(s); increasing to {wanted}.");

        if (actual >= 1 && wanted > 1)
        {
            Console.WriteLine(
                "Note: adding partitions rehashes the room-to-partition mapping. Rooms that move " +
                "may briefly appear to skip or repeat messages in browsers that are connected " +
                "across the change, because the SSE event id is the Kafka offset and offsets are " +
                "per partition. Reloading the page resolves it.");
        }

        try
        {
            await adminClient.CreatePartitionsAsync(
                [new PartitionsSpecification { Topic = topic, IncreaseTo = wanted }]);
            Console.WriteLine($"Topic '{topic}' now has {wanted} partition(s).");
            return true;
        }
        catch (CreatePartitionsException e)
        {
            Console.Error.WriteLine(
                $"Could not increase the partitions of '{topic}': " +
                $"{string.Join("; ", e.Results.Select(r => r.Error.Reason))}");
            return false;
        }
    }

    private static int PartitionCount(IAdminClient adminClient, string topic)
    {
        try
        {
            var metadata = adminClient.GetMetadata(topic, AdminTimeout);
            var found = metadata.Topics.FirstOrDefault(t => t.Topic == topic);
            return found is null || found.Error.IsError ? -1 : found.Partitions.Count;
        }
        catch (KafkaException)
        {
            return -1;
        }
    }

    private static string Env(string key, string fallback)
    {
        var value = Environment.GetEnvironmentVariable(key);
        return string.IsNullOrWhiteSpace(value) ? fallback : value;
    }

    private static int EnvInt(string key, int fallback)
    {
        var raw = Environment.GetEnvironmentVariable(key);
        if (string.IsNullOrWhiteSpace(raw))
        {
            return fallback;
        }

        if (!int.TryParse(raw, out var value) || value <= 0)
        {
            throw new InvalidOperationException(
                $"{key} must be a positive integer, but was '{raw}'.");
        }

        return value;
    }

    private static string? GetArg(string[] args, string key)
    {
        for (var i = 0; i < args.Length - 1; i++)
        {
            if (args[i] == key)
            {
                return args[i + 1];
            }
        }

        return null;
    }

    private static bool HasFlag(string[] args, string key) => Array.IndexOf(args, key) >= 0;

    private static string? ReadInput()
    {
        var line = Console.ReadLine();
        if (line is null)
        {
            return null;
        }

        if (!Console.IsOutputRedirected && Console.CursorTop > 0)
        {
            Console.SetCursorPosition(0, Console.CursorTop - 1);
            Console.Write(new string(' ', Console.WindowWidth));
            Console.SetCursorPosition(0, Console.CursorTop);
        }

        return line;
    }

    private static void PrintUsage()
    {
        Console.WriteLine("""
            Usage:
              dotnet run --project src/RedeTim.ChatClient -- --nick NAME [--room general] [--topic NAME] [--hist true] [--newTopic NAME]
              dotnet run --project src/RedeTim.ChatClient -- --ensure-topic
              dotnet run --project src/RedeTim.ChatClient -- --describe-topic
              dotnet run --project src/RedeTim.ChatClient -- --print-config

            The admin processes (12-Factor XII). Each runs from the application's own image, under
            the application's own configuration, and exits:

              --ensure-topic    waits for the broker, creates the chat topic if it is missing, and
                                raises its partition count to CHAT_PARTITIONS if it is lower.
                                Refuses to continue if it is higher, because Kafka cannot reduce
                                one. The chart runs this as an ordinary Job on every install and
                                upgrade -- not as a Helm hook, which could never run: a
                                post-install hook waits for a backend that is not ready until the
                                topic it would create exists. Also creates the log-compacted
                                presence topic the backend uses to enforce one nickname per room.
              --describe-topic  partitions, leaders, replicas and watermarks, plus any non-default
                                topic configuration. The watermarks are the only honest answer to
                                "how far back can a browser see".
              --print-config    the configuration this process actually resolved, defaults
                                included and the password masked.

            The chart can run any of them ad hoc, in the release's own image and configuration:

              helm upgrade redetim deploy/helm/redetim -f "$REL" \
                --set adminJob.enabled=true \
                --set-json 'adminJob.args=["--describe-topic"]'
              kubectl -n redetim logs job/redetim-admin-<revision>

            --newTopic runs that same --ensure-topic process before joining, so a topic created
            from here gets the release's own CHAT_PARTITIONS and CHAT_REPLICATION_FACTOR rather
            than a hard-coded single partition.

            Environment:
              REDPANDA_BOOTSTRAP_SERVERS   broker list                 (default: redpanda:9092)
              REDPANDA_TOPIC               topic to join               (default: redetim-chat)
              REDPANDA_SECURITY_PROTOCOL   Plaintext|Ssl|SaslPlaintext|SaslSsl
              REDPANDA_SASL_MECHANISM      required for a SASL protocol
              REDPANDA_SASL_USERNAME       required for a SASL protocol
              REDPANDA_SASL_PASSWORD       required for a SASL protocol
              REDPANDA_SSL_CA_LOCATION     CA bundle for a private CA  (TLS only)

            --ensure-topic and --newTopic additionally read:
              CHAT_PARTITIONS              partitions to create        (default: 1)
              CHAT_REPLICATION_FACTOR      replication factor          (default: 1)
              TOPIC_WAIT_SECONDS           how long to wait for a broker (default: 180)
              REDPANDA_PRESENCE_TOPIC      presence topic to ensure    (default: redetim-presence)
              PRESENCE_PARTITIONS          partitions to create        (default: 1)
              PRESENCE_REPLICATION_FACTOR  replication factor          (default: 1)
            """);
    }
}
