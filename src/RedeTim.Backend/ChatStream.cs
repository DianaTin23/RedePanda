using System.Globalization;
using System.Net.ServerSentEvents;
using System.Runtime.CompilerServices;
using System.Threading.Channels;
using RedeTim.Contracts;

namespace RedeTim.Backend;

/// <summary>Turns one room's slice of the <see cref="ChatBroadcaster"/> into an SSE item sequence.</summary>
internal static class ChatStream
{
    /// <summary>How long a room may stay quiet before a heartbeat is emitted.</summary>
    internal static readonly TimeSpan DefaultHeartbeatInterval = TimeSpan.FromSeconds(15);

    /// <summary>Event type of a heartbeat, so <c>EventSource.onmessage</c> never sees one.</summary>
    internal const string HeartbeatEventType = "ping";

    /// <summary>Yields one item per chat message, and a heartbeat whenever the room stays quiet.</summary>
    internal static async IAsyncEnumerable<SseItem<string>> Create(
        ChatBroadcaster broadcaster,
        string room,
        TimeSpan heartbeatInterval,
        long lastEventId = -1,
        CancellationToken shutdown = default,
        [EnumeratorCancellation] CancellationToken ct = default)
    {
        using var stopping = CancellationTokenSource.CreateLinkedTokenSource(ct, shutdown);
        var token = stopping.Token;

        using var subscription = broadcaster.Subscribe(room, lastEventId);

        yield return Heartbeat();

        foreach (var record in subscription.Backlog)
        {
            yield return Data(record);
        }

        while (!token.IsCancellationRequested)
        {
            ChatRecord? record = null;
            var finished = false;

            using (var heartbeat = CancellationTokenSource.CreateLinkedTokenSource(token))
            {
                heartbeat.CancelAfter(heartbeatInterval);

                try
                {
                    record = await subscription.Reader.ReadAsync(heartbeat.Token);
                }
                catch (OperationCanceledException) when (!token.IsCancellationRequested)
                {
                    // Heartbeat interval elapsed; fall through and emit a ping.
                }
                catch (OperationCanceledException)
                {
                    finished = true;
                }
                catch (ChannelClosedException)
                {
                    finished = true;
                }
            }

            if (finished)
            {
                yield break;
            }

            yield return record is null ? Heartbeat() : Data(record.Value);
        }
    }

    private static SseItem<string> Data(ChatRecord record) =>
        new(ChatMessageSerializer.Serialize(record.Message))
        {
            EventId = record.Offset.ToString(CultureInfo.InvariantCulture),
        };

    private static SseItem<string> Heartbeat() =>
        new(string.Empty, eventType: HeartbeatEventType);
}
