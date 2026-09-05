using Confluent.Kafka;

namespace RedeTim.Backend;

internal static class KafkaLogging
{
    public static readonly TimeSpan ErrorInterval = TimeSpan.FromSeconds(10);

    public static LogLevel ToLogLevel(SyslogLevel level) => level switch
    {
        SyslogLevel.Emergency or SyslogLevel.Alert or SyslogLevel.Critical => LogLevel.Critical,
        SyslogLevel.Error => LogLevel.Error,
        SyslogLevel.Warning => LogLevel.Warning,
        SyslogLevel.Notice or SyslogLevel.Info => LogLevel.Information,
        _ => LogLevel.Debug,
    };
}

internal sealed class LogThrottle(TimeSpan interval)
{
    private readonly Lock _gate = new();
    private long _suppressed;
    private DateTimeOffset _nextAllowed = DateTimeOffset.MinValue;

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

    public static string Describe(long suppressed) =>
        suppressed > 0 ? $" ({suppressed} further suppressed)" : string.Empty;
}
