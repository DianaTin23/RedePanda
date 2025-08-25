// Program.cs — .NET 9, one binary for produce|consume
// Usage:
//   dotnet run --project RedePanda-chat-client -- produce <bootstrap> [--nick NAME] [--topic chat.room1]
//   dotnet run --project RedePanda-chat-client -- consume <bootstrap> [--topic chat.room1]

using System.Text;
using System.Text.Json;
using System.Text.RegularExpressions;
using Confluent.Kafka;
using Confluent.Kafka.Admin;
using DotNetEnv;
using Microsoft.Extensions.Configuration;

namespace RedePanda_chat_client
{
    internal static class Program
    {
        static string GetArg(string[] a, string key, string def = "")
        {
            for (int i = 0; i < a.Length - 1; i++) if (a[i] == key) return a[i + 1];
            return def;
        }

        static string ReadInput()
        {
            var line = Console.ReadLine();
            if (line is null) return string.Empty;
            
            Console.SetCursorPosition(0, Console.CursorTop - 1);
            Console.Write(new string(' ', Console.WindowWidth));
            Console.SetCursorPosition(0, Console.CursorTop);

            return line;
        }

        static async Task CreateTopic(string bootstrap, string topicName)
        {
            var adminConfig = new AdminClientConfig { BootstrapServers = bootstrap };
            using var adminClient = new AdminClientBuilder(adminConfig).Build();

            try
            {
                await adminClient.CreateTopicsAsync(new TopicSpecification[]
                {
                    new TopicSpecification
                    {
                        Name = topicName,
                        NumPartitions = 1
                    }
                });
            }
            catch (Exception e)
            {
                Console.WriteLine("Topic could not be created: " + e.Message);
            }
        }

        static string ConfigureBootstrap(string environment)
        {
            var content = File.ReadAllText($"{environment}.json");
            var json = JsonSerializer.Deserialize<Bootstrap>(content);

            return json.AdvertisedHost + ":" + json.Port;
        }

        public static async Task Main(string[] args)
        {
            string bootstrapArg = args[0];
            if ((bootstrapArg != "local") && (bootstrapArg != "lan"))
            {
                Console.WriteLine("Provide correct argument for bootstrap.");
            }
            string bootstrap = ConfigureBootstrap(bootstrapArg);
            
            string topic = GetArg(args, "--topic");
            string newTopic = GetArg(args, "--newTopic");
            if (!String.IsNullOrEmpty(newTopic))
            {
                await CreateTopic(bootstrap, newTopic);
                topic = newTopic;
            }
            if(String.IsNullOrEmpty(topic)) Console.WriteLine("There is no topic with that name.");
            
            string nick = GetArg(args, "--nick");
            string history = GetArg(args, "--hist");
            bool showHist = false;
            if (history == "true") showHist = true;

            var producer = new Producer(bootstrap, topic);
            var consumer = new Consumer(bootstrap, topic, showHist);
            
            if (args.Length < 2)
            {
                Console.WriteLine("Usage:\n  dotnet run --project RedePanda-chat-client -- <bootstrap> [--nick NAME] [--topic chat.room1] [--hist true]");
                return;
            }

            using var cts = new CancellationTokenSource();
            Console.CancelKeyPress += (_, e) =>
            {
                e.Cancel = true;
                cts.Cancel();
            };
            
            Console.WriteLine($"Chat under the topic {topic} was started. Press Ctrl+C to leave the chat.");
            
            consumer.ConsumeMessages(cts.Token);

            while (!cts.IsCancellationRequested)
            {
                var line = ReadInput();
                if (String.IsNullOrEmpty(line)) break;
                var msg = new ChatMsg(nick, DateTime.UtcNow.ToString("MM-dd HH:mm:ss"), line);
                await producer.SendMessages(msg);
            }
            
            Console.WriteLine("Chat was closed, shutting down...");
        }
    }
}
