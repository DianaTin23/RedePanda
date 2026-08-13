using Confluent.Kafka;

namespace RedePanda.Backend;

/// <summary>
/// Gets librdkafka's own output into the same log stream as everything else, and keeps a broker
/// outage from burying that stream.
/// </summary>
internal static class KafkaLogging
{
    /// <summary>
    /// How often one Kafka client may report the same kind of trouble.
    /// <para>
    /// librdkafka invokes the error callback per broker connection attempt, so a single
    /// unreachable broker produced on the order of twenty lines a second per pod — enough to make
    /// the pod's log useless for anything else at exactly the moment someone reads it.
    /// </para>
    /// </summary>
    public static readonly TimeSpan ErrorInterval = TimeSpan.FromSeconds(10);

    /// <summary>
    /// librdkafka reports at syslog severities. Its routine connection chatter sits at notice and
    /// info, and only warning and above describes something an operator can act on.
    /// </summary>
    public static LogLevel ToLogLevel(SyslogLevel level) => level switch
    {
        SyslogLevel.Emergency or SyslogLevel.Alert or SyslogLevel.Critical => LogLevel.Critical,
        SyslogLevel.Error => LogLevel.Error,
        SyslogLevel.Warning => LogLevel.Warning,
        SyslogLevel.Notice or SyslogLevel.Info => LogLevel.Information,
        _ => LogLevel.Debug,
    };
}

/// <summary>
/// Lets one message through per interval and remembers how many it held back.
/// <para>
/// The suppressed count rides along on the next line that does get through, so a throttled log is
/// still an honest one: it folds repetition rather than hiding it. Metrics are deliberately
/// <b>not</b> throttled alongside it — a counter that skips events stops being a count, and
/// <c>redepanda_kafka_errors_total</c> is the instrument that should show the true rate.
/// </para>
/// </summary>
internal sealed class LogThrottle(TimeSpan interval)
{
    private readonly Lock _gate = new();
    private long _suppressed;
    private DateTimeOffset _nextAllowed = DateTimeOffset.MinValue;

    /// <param name="suppressed">
    /// How many messages were held back since the last one that got through. Only meaningful when
    /// this returns <c>true</c>.
    /// </param>
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
