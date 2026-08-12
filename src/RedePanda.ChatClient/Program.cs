using Confluent.Kafka;
using Confluent.Kafka.Admin;
using RedePanda.Contracts;

namespace RedePanda.ChatClient;

internal static class Program
{
    private const string DefaultBootstrapServers = "redpanda:9092";
    private const string DefaultTopic = "redepanda-chat";
    private const string DefaultRoom = "general";

    public static async Task<int> Main(string[] args)
    {
        if (HasFlag(args, "--help") || HasFlag(args, "-h"))
        {
            PrintUsage();
            return 0;
        }

        // Configuration comes from the environment (12-Factor); arguments only cover per-session
        // choices such as the nickname. There is no config file any more.
        var bootstrap = Env("REDPANDA_BOOTSTRAP_SERVERS", DefaultBootstrapServers);
        var topic = GetArg(args, "--topic") ?? Env("REDPANDA_TOPIC", DefaultTopic);

        // The admin process (12-Factor XII): the same binary, the same image and the same
        // configuration as the chat itself, run as a one-off task instead of as a long-running
        // one. It needs no nickname and reads no input, so it returns before anything below.
        if (HasFlag(args, "--ensure-topic"))
        {
            return await EnsureTopicAsync(bootstrap, topic);
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
            if (!await TryCreateTopicAsync(bootstrap, topic))
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

        // Reading every room rather than filtering to `room` is deliberate: it makes visible in the
        // demo that the console client and the backend really do share one Kafka topic.
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

    /// <summary>
    /// Creates the chat topic if it is not there yet, then exits — the whole of the admin process.
    /// <para>
    /// It waits for the broker itself rather than assuming one is up: as a Helm hook this runs
    /// while Redpanda is still starting, and against an external broker there may be nothing else
    /// to wait on at all. A metadata request is the check, because it is the same request every
    /// client makes first and it needs no Admin API port, which a managed broker may not expose.
    /// </para>
    /// </summary>
    private static async Task<int> EnsureTopicAsync(string bootstrap, string topic)
    {
        var partitions = EnvInt("CHAT_PARTITIONS", 1);
        var replicationFactor = EnvInt("CHAT_REPLICATION_FACTOR", 1);
        var waitSeconds = EnvInt("TOPIC_WAIT_SECONDS", 180);

        Console.WriteLine(
            $"Ensuring topic '{topic}' on {bootstrap}: {partitions} partition(s), " +
            $"replication factor {replicationFactor}.");

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

        return await TryCreateTopicAsync(adminClient, topic, partitions, replicationFactor) ? 0 : 1;
    }

    /// <summary>Polls for broker metadata until it arrives or <paramref name="budget"/> is spent.</summary>
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

    private static async Task<bool> TryCreateTopicAsync(string bootstrap, string topic)
    {
        var adminConfig = new AdminClientConfig { BootstrapServers = bootstrap };
        KafkaSecurity.ApplyTo(adminConfig);
        using var adminClient = new AdminClientBuilder(adminConfig).Build();

        return await TryCreateTopicAsync(adminClient, topic, partitions: 1, replicationFactor: 1);
    }

    private static async Task<bool> TryCreateTopicAsync(
        IAdminClient adminClient, string topic, int partitions, int replicationFactor)
    {
        try
        {
            await adminClient.CreateTopicsAsync([
                new TopicSpecification
                {
                    Name = topic,
                    NumPartitions = partitions,
                    ReplicationFactor = (short)replicationFactor,
                }
            ]);
            Console.WriteLine($"Topic '{topic}' created.");
            return true;
        }
        catch (CreateTopicsException e) when (e.Results.All(r => r.Error.Code == ErrorCode.TopicAlreadyExists))
        {
            // Creating an existing topic is not a failure for a chat client.
            Console.WriteLine($"Topic '{topic}' already exists.");
            return true;
        }
        catch (Exception e)
        {
            Console.Error.WriteLine($"Topic '{topic}' could not be created: {e.Message}");
            return false;
        }
    }

    private static string Env(string key, string fallback)
    {
        var value = Environment.GetEnvironmentVariable(key);
        return string.IsNullOrWhiteSpace(value) ? fallback : value;
    }

    /// <summary>
    /// Fails loudly on a typo rather than silently creating a one-partition topic where three
    /// were meant — a topic's partition count cannot be lowered again afterwards.
    /// </summary>
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
        // Bounded by args.Length - 1 so a trailing key without a value cannot read past the end.
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

        // Erase the echoed input line so only the consumer's rendering of it remains.
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
              dotnet run --project src/RedePanda.ChatClient -- --nick NAME [--room general] [--topic NAME] [--hist true] [--newTopic NAME]
              dotnet run --project src/RedePanda.ChatClient -- --ensure-topic

            --ensure-topic is the admin process: it waits for the broker, creates the topic if it
            is missing, and exits. The chart runs this image with that flag as a Helm hook.

            Environment:
              REDPANDA_BOOTSTRAP_SERVERS   broker list                 (default: redpanda:9092)
              REDPANDA_TOPIC               topic to join               (default: redepanda-chat)
              REDPANDA_SECURITY_PROTOCOL   Plaintext|Ssl|SaslPlaintext|SaslSsl
              REDPANDA_SASL_MECHANISM      required for a SASL protocol
              REDPANDA_SASL_USERNAME       required for a SASL protocol
              REDPANDA_SASL_PASSWORD       required for a SASL protocol
              REDPANDA_SSL_CA_LOCATION     CA bundle for a private CA  (TLS only)

            --ensure-topic additionally reads:
              CHAT_PARTITIONS              partitions to create        (default: 1)
              CHAT_REPLICATION_FACTOR      replication factor          (default: 1)
              TOPIC_WAIT_SECONDS           how long to wait for a broker (default: 180)
            """);
    }
}
