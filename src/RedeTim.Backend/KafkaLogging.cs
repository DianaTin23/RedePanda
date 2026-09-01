using Confluent.Kafka;

namespace RedeTim.Backend;

/// <summary>Gets librdkafka's own output into the same log stream as everything else.</summary>
internal static class KafkaLogging
{
    /// <summary>How often one Kafka client may report the same kind of trouble.</summary>
    public static readonly TimeSpan ErrorInterval = TimeSpan.FromSeconds(10);

    /// <summary>Maps librdkafka's syslog severities onto the framework's levels.</summary>
    public static LogLevel ToLogLevel(SyslogLevel level) => level switch
    {
        SyslogLevel.Emergency or SyslogLevel.Alert or SyslogLevel.Critical => LogLevel.Critical,
        SyslogLevel.Error => LogLevel.Error,
        SyslogLevel.Warning => LogLevel.Warning,
        SyslogLevel.Notice or SyslogLevel.Info => LogLevel.Information,
        _ => LogLevel.Debug,
    };
}

/// <summary>Lets one message through per interval and remembers how many it held back.</summary>
internal sealed class LogThrottle(TimeSpan interval)
{
    private readonly Lock _gate = new();
    private long _suppressed;
    private DateTimeOffset _nextAllowed = DateTimeOffset.MinValue;

    /// <summary>Whether a message may be logged now, and how many were suppressed since the last one.</summary>
    public bool ShouldLog(out long suppressed)
    {
        lock (_gate)
        {
            var now = DateTimeOffset.UtcNow;
            if (now < _nextAllowed)
            {
                _suppressed++;
                suppressed = 0;
                return false;
            }

            suppressed = _suppressed;
            _suppressed = 0;
            _nextAllowed = now + interval;
            return true;
        }
    }

    /// <summary>Renders the count as a clause, or nothing at all when there is nothing to add.</summary>
    public static string Describe(long suppressed) =>
        suppressed > 0 ? $" ({suppressed} further suppressed)" : string.Empty;
}
