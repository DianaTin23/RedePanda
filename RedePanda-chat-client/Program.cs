// Program.cs — .NET 9, one binary for produce|consume
// Usage:
//   dotnet run --project RedePanda-chat-client -- produce <bootstrap> [--nick NAME] [--topic chat.room1]
//   dotnet run --project RedePanda-chat-client -- consume <bootstrap> [--topic chat.room1]

using System.Text.Json;
using Confluent.Kafka;

namespace RedePanda_chat_client
{
    internal static class Program
    {
        static string GetArg(string[] a, string key, string def = "")
        {
            for (int i = 0; i < a.Length - 1; i++) if (a[i] == key) return a[i + 1];
            return def;
        }

        public static async Task Main(string[] args)
        {
            if (args.Length < 2)
            {
                Console.WriteLine("Usage:\n  dotnet run --project RedePanda-chat-client -- produce <bootstrap> [--nick NAME] [--topic chat.room1]\n  dotnet run --project RedePanda-chat-client -- consume <bootstrap> [--topic chat.room1]");
                return;
            }

            string mode = args[0];
            string bootstrap = args[1];
            string topic = GetArg(args, "--topic", "chat.room1");
            string nick = GetArg(args, "--nick");

            var producer = new Producer(bootstrap, topic);
            var consumer = new Consumer(bootstrap, topic);

            using var cts = new CancellationTokenSource();
            Console.CancelKeyPress += (RedePanda_chat_client, e) =>
            {
                e.Cancel = true;
                cts.Cancel();
            };
            
            consumer.ConsumeMessages(cts.Token);

            while (!cts.IsCancellationRequested)
            {
                var line = Console.ReadLine();
                if (line is null) break;
                var msg = new ChatMsg(nick, DateTime.UtcNow.ToString("HH:mm:ss"), line);
                producer.SendMessages(msg);
            }
        }
    }
}
