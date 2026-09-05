using System.Globalization;
using System.Net.ServerSentEvents;
using System.Runtime.CompilerServices;
using System.Threading.Channels;
using RedeTim.Contracts;

namespace RedeTim.Backend;

internal static class ChatStream
{
    internal static readonly TimeSpan DefaultHeartbeatInterval = TimeSpan.FromSeconds(15);

    internal const string HeartbeatEventType = "ping";

    internal static async IAsyncEnumerable<SseItem<string>> Create(
        ChatBroadcaster broadcaster,
        string room,
        TimeSpan heartbeatInterval,
        long lastEventId = -1,
        PresenceSession? presence = null,
        CancellationToken shutdown = default,
        [EnumeratorCancellation] CancellationToken ct = default)
    {
        using var stopping = CancellationTokenSource.CreateLinkedTokenSource(ct, shutdown);
        var token = stopping.Token;

        using var subscription = broadcaster.Subscribe(room, lastEventId);

        if (presence is not null)
        {
            await presence.RenewAsync(token);
        }

        try
        {
            yield return Heartbeat();

            foreach (var record in subscription.Backlog)
            {
                yield return Data(record);
            }

            var lastRenewal = DateTimeOffset.UtcNow;

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

                // Every iteration, not just the heartbeat branch: a busy room keeps satisfying
                // ReadAsync before the heartbeat times out, and would never renew presence.
                if (presence is not null && DateTimeOffset.UtcNow - lastRenewal >= heartbeatInterval)
                {
                    lastRenewal = DateTimeOffset.UtcNow;
                    await presence.RenewAsync(token);
                }

                if (finished)
                {
                    yield break;
                }

                yield return record is null ? Heartbeat() : Data(record.Value);
            }
        }
        finally
        {
            if (presence is not null)
            {
                await presence.ReleaseAsync();
            }
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
