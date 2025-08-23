// Program.cs — .NET 9, one binary for produce|consume
// Usage:
//   dotnet run --project RedePanda-chat-client -- produce <bootstrap> [--nick NAME] [--topic chat.room1]
//   dotnet run --project RedePanda-chat-client -- consume <bootstrap> [--topic chat.room1]

using System.Text;
using System.Text.RegularExpressions;

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

        public static async Task Main(string[] args)
        {
            string bootstrap = args[0];
            string topic = GetArg(args, "--topic", "chat.room1");
            string nick = GetArg(args, "--nick");

            var producer = new Producer(bootstrap, topic);
            var consumer = new Consumer(bootstrap, topic);
            
            var bootstrapRegex = new Regex(@"^([a-zA-Z0-9.-]+:\d{1,5})(,[a-zA-Z0-9.-]+:\d{1,5})*$");
            if (args.Length < 2 || !bootstrapRegex.IsMatch(bootstrap))
            {
                Console.WriteLine("Usage:\n  dotnet run --project RedePanda-chat-client -- <bootstrap> [--nick NAME] [--topic chat.room1]\n  dotnet run --project RedePanda-chat-client -- consume <bootstrap> [--topic chat.room1]");
                return;
            }

            using var cts = new CancellationTokenSource();
            Console.CancelKeyPress += (_, e) =>
            {
                e.Cancel = true;
                cts.Cancel();
            };
            
            consumer.ConsumeMessages(cts.Token);

            while (!cts.IsCancellationRequested)
            {
                var line = ReadInput();
                if (String.IsNullOrEmpty(line)) break;
                var msg = new ChatMsg(nick, DateTime.UtcNow.ToString("HH:mm:ss"), line);
                await producer.SendMessages(msg);
            }
        }
    }
}
