using System.Globalization;
using System.Net.ServerSentEvents;
using System.Runtime.CompilerServices;
using System.Threading.Channels;
using RedePanda.Contracts;

namespace RedePanda.Backend;

/// <summary>
/// Turns one room's slice of the <see cref="ChatBroadcaster"/> into the item sequence that
/// <c>TypedResults.ServerSentEvents</c> writes to a browser.
/// </summary>
internal static class ChatStream
{
    /// <summary>
    /// Keeps idle connections alive through proxies and reveals dead peers to us. Long enough to
    /// stay cheap, short enough to beat the common 60 s idle timeout.
    /// </summary>
    internal static readonly TimeSpan DefaultHeartbeatInterval = TimeSpan.FromSeconds(15);

    /// <summary>
    /// Event type of a heartbeat. Deliberately not the default type: EventSource.onmessage only
    /// fires for <c>message</c>, so the browser drops these without ever seeing them.
    /// <para>
    /// This used to be an SSE comment (<c>": ping"</c>). <see cref="SseItem{T}"/> cannot express a
    /// comment — it carries data, an event type and an id, nothing else — so it is a typed event
    /// now. Inert for this frontend either way, and still a no-op for any client that filters on
    /// event type rather than parsing every frame.
    /// </para>
    /// </summary>
    internal const string HeartbeatEventType = "ping";

    /// <summary>Yields one item per chat message, and a heartbeat whenever the room stays quiet.</summary>
    /// <param name="lastEventId">
    /// The offset from the client's <c>Last-Event-ID</c> header, or <c>-1</c> for a fresh
    /// connection. Everything up to and including it is left out of the replay.
    /// </param>
    internal static async IAsyncEnumerable<SseItem<string>> Create(
        ChatBroadcaster broadcaster,
        string room,
        TimeSpan heartbeatInterval,
        long lastEventId = -1,
        [EnumeratorCancellation] CancellationToken ct = default)
    {
        // Disposed when the framework disposes the enumerator, which is what drops the subscriber
        // out of the active-connection metric once the browser goes away.
        using var subscription = broadcaster.Subscribe(room, lastEventId);

        // Primes the response. ServerSentEvents writes nothing — not even the status line — until
        // the first item is yielded, so without this a quiet room would leave the client waiting a
        // full heartbeat interval before EventSource leaves CONNECTING and fires onopen. The
        // hand-written predecessor flushed the headers before its loop; this restores that.
        yield return Heartbeat();

        // The room as it stands, before anything live. Ordered by offset and therefore in the order
        // the messages were written, which is what lets the frontend keep appending blindly.
        foreach (var record in subscription.Backlog)
        {
            yield return Data(record);
        }

        while (!ct.IsCancellationRequested)
        {
            ChatRecord? record = null;
            var finished = false;

            using (var heartbeat = CancellationTokenSource.CreateLinkedTokenSource(ct))
            {
                heartbeat.CancelAfter(heartbeatInterval);

                try
                {
                    record = await subscription.Reader.ReadAsync(heartbeat.Token);
                }
                catch (OperationCanceledException) when (!ct.IsCancellationRequested)
                {
                    // Heartbeat interval elapsed with no message; fall through and emit a ping.
                }
                catch (OperationCanceledException)
                {
                    // The browser went away, or the process is shutting down. Both are normal.
                    finished = true;
                }
                catch (ChannelClosedException)
                {
                    // Subscription was completed during shutdown.
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

    /// <summary>
    /// One chat message, carrying its Kafka offset as the SSE event id. A browser echoes the last
    /// id it saw back in <c>Last-Event-ID</c> when it reconnects, which is what turns the replay
    /// into a resume instead of a second copy of the room.
    /// </summary>
    private static SseItem<string> Data(ChatRecord record) =>
        new(ChatMessageSerializer.Serialize(record.Message))
        {
            EventId = record.Offset.ToString(CultureInfo.InvariantCulture),
        };

    /// <summary>
    /// Deliberately without an event id: per the SSE specification a frame that carries no
    /// <c>id</c> field leaves the client's last-event-id buffer alone. Stamping heartbeats would
    /// move the resume point forward past messages the browser never received.
    /// </summary>
    private static SseItem<string> Heartbeat() =>
        new(string.Empty, eventType: HeartbeatEventType);
}
