// Program.cs — .NET 9, one binary for produce|consume
// Usage:
//   dotnet run --project RedePanda-chat-client -- produce <bootstrap> [--nick NAME] [--topic chat.room1]
//   dotnet run --project RedePanda-chat-client -- consume <bootstrap> [--topic chat.room1]

using System.Text.Json;
using Confluent.Kafka;

namespace RedePanda_chat_client
{
    record ChatMsg(string Nick, string Ts, string Text);

    internal static class Program
    {
        static string GetArg(string[] a, string key, string def = "")
        {
            for (int i = 0; i < a.Length - 1; i++) if (a[i] == key) return a[i + 1];
            return def;
        }

        public static async Task Main(string[] args)
        {
            if (args.Length < 2 || (args[0] != "produce" && args[0] != "consume"))
            {
                Console.WriteLine("Usage:\n  kchat produce <bootstrap> [--nick NAME] [--topic chat.room1]\n  kchat consume <bootstrap> [--topic chat.room1]");
                return;
            }

            string mode = args[0];
            string bootstrap = args[1];
            string topic = GetArg(args, "--topic", "chat.room1");

            if (mode == "produce")
            {
                string nick = GetArg(args, "--nick", "");
                if (string.IsNullOrWhiteSpace(nick))
                {
                    Console.Write("Nick: ");
                    nick = Console.ReadLine() ?? "anon";
                }

                var pconf = new ProducerConfig
                {
                    BootstrapServers = bootstrap,
                    Acks = Acks.Leader
                };

                using var producer = new ProducerBuilder<Null, string>(pconf).Build();
                Console.WriteLine($"Producer → {bootstrap} topic={topic}. Type and Enter. Ctrl+C exits.");
                Console.CancelKeyPress += (_, e) => { e.Cancel = true; };

                while (true)
                {
                    var line = Console.ReadLine();
                    if (line is null) break;
                    var msg = new ChatMsg(nick, DateTime.UtcNow.ToString("HH:mm:ss"), line);
                    await producer.ProduceAsync(topic, new Message<Null, string> { Value = JsonSerializer.Serialize(msg) });
                }

                producer.Flush(TimeSpan.FromSeconds(2));
            }
            else // consume
            {
                var cconf = new ConsumerConfig
                {
                    BootstrapServers = bootstrap,
                    GroupId = "kchat-" + Guid.NewGuid().ToString("N")[..6],
                    AutoOffsetReset = AutoOffsetReset.Latest,
                    EnablePartitionEof = true
                };

                using var consumer = new ConsumerBuilder<Ignore, string>(cconf).Build();
                consumer.Subscribe(topic);
                Console.WriteLine($"Consumer ← {bootstrap} topic={topic}. Ctrl+C exits.");

                var cts = new CancellationTokenSource();
                Console.CancelKeyPress += (_, e) => { e.Cancel = true; cts.Cancel(); };

                try
                {
                    while (!cts.IsCancellationRequested)
                    {
                        try
                        {
                            var cr = consumer.Consume(cts.Token);
                            if (cr?.Message == null) continue;
                            var m = JsonSerializer.Deserialize<ChatMsg>(cr.Message.Value);
                            if (m is not null) Console.WriteLine($"[{m.Ts}] {m.Nick}: {m.Text}");
                        }
                        catch (ConsumeException e) { Console.Error.WriteLine($"consume error: {e.Error.Reason}"); }
                    }
                }
                catch (OperationCanceledException) { }
                finally { consumer.Close(); }
            }
        }
    }
}
