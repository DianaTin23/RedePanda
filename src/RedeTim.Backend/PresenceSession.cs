using Confluent.Kafka;

namespace RedeTim.Backend;

// A Kafka failure here must never tear down the SSE stream, so everything is swallowed and logged.
internal sealed class PresenceSession(
    IPresenceProducer producer, string room, string nickname, ILogger logger)
{
    public async Task RenewAsync(CancellationToken cancellationToken)
    {
        try
        {
            await producer.RenewAsync(room, nickname, cancellationToken);
        }
        catch (ProduceException<string, string> e)
        {
            logger.LogWarning(
                "Could not renew presence for {Nickname} in {Room}: {Reason}", nickname, room, e.Error.Reason);
        }
    }

    public async Task ReleaseAsync()
    {
        try
        {
            await producer.ReleaseAsync(room, nickname, CancellationToken.None);
        }
        catch (ProduceException<string, string> e)
        {
            logger.LogWarning(
                "Could not release presence for {Nickname} in {Room}: {Reason}", nickname, room, e.Error.Reason);
        }
    }
}
