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

    private static async Task<bool> TryCreateTopicAsync(string bootstrap, string topic)
    {
        var adminConfig = new AdminClientConfig { BootstrapServers = bootstrap };
        KafkaSecurity.ApplyTo(adminConfig);
        using var adminClient = new AdminClientBuilder(adminConfig).Build();

        try
        {
            await adminClient.CreateTopicsAsync([
                new TopicSpecification { Name = topic, NumPartitions = 1, ReplicationFactor = 1 }
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

            Environment:
              REDPANDA_BOOTSTRAP_SERVERS   broker list      (default: redpanda:9092)
              REDPANDA_TOPIC               topic to join    (default: redepanda-chat)
            """);
    }
}
