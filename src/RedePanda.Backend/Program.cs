using System.Globalization;
using Confluent.Kafka;
using OpenTelemetry;
using OpenTelemetry.Metrics;
using RedePanda.Backend;
using RedePanda.Contracts;

var builder = WebApplication.CreateBuilder(args);
var options = BackendOptions.FromEnvironment();

// ---- Logging ---------------------------------------------------------------------------------
// Structured, to stdout, no log files: the platform collects them (12-Factor "Logs").
builder.Logging.ClearProviders();
builder.Logging.AddJsonConsole(c =>
{
    c.IncludeScopes = false;

    // Without this the JSON carries no event time whatsoever. An event stream whose events cannot
    // be ordered or correlated with anything else is a much smaller fraction of a log than it
    // looks, and README section 11 claimed a Timestamp field that was never emitted.
    //
    // Round-trippable and explicitly UTC, so nothing downstream has to guess an offset. No
    // trailing space: that is a convention of the plain-text console formatter, which needs to
    // separate the stamp from the message, and in JSON it would only corrupt the field's value.
    c.TimestampFormat = "yyyy-MM-dd'T'HH:mm:ss.fffffff'Z'";
    c.UseUtcTimestamp = true;
});
builder.Logging.SetMinimumLevel(options.LogLevel);

// The backend's equivalent of the Caddyfile's `log_skip` on /healthz. ASP.NET writes about six
// lines per request at Information, and most requests here are readiness probes -- so the
// application's own events were a minority of its own log.
//
// Suppressed only while nobody is actually debugging: someone who sets LOG_LEVEL=Debug is asking
// for everything, and silently withholding the framework half would be its own puzzle.
if (options.LogLevel > LogLevel.Debug)
{
    builder.Logging.AddFilter("Microsoft.AspNetCore", LogLevel.Warning);
}

// ---- Services --------------------------------------------------------------------------------
builder.Services.AddSingleton(options);
builder.Services.AddSingleton<ChatMetrics>();
builder.Services.AddSingleton<ChatBroadcaster>();
builder.Services.AddSingleton<ChatProducer>();
builder.Services.AddSingleton<BrokerReadiness>();
builder.Services.AddHostedService<ChatConsumerService>();

// The host's own shutdown timeout defaults to 30s, exactly the same as Kubernetes'
// terminationGracePeriodSeconds. Leaving both at the default makes them race, so this one is
// pulled in and the pod's grace period is raised in the chart.
builder.Services.Configure<HostOptions>(o => o.ShutdownTimeout = TimeSpan.FromSeconds(25));

// ---- OpenTelemetry ---------------------------------------------------------------------------
// The resource is intentionally NOT configured in code. OTEL_SERVICE_NAME and
// OTEL_RESOURCE_ATTRIBUTES are specified environment variables that the SDK reads itself, and
// calling ConfigureResource(r => r.AddService(...)) here would beat them: code wins over
// environment in Resource.Merge, and AddService also auto-generates a random service.instance.id.
// The Prometheus "instance" label would then be a fresh GUID after every restart.
builder.Services.AddOpenTelemetry()
    .WithMetrics(m => m
        .AddAspNetCoreInstrumentation()
        .AddRuntimeInstrumentation()
        .AddMeter(ChatMetrics.MeterName)

        // AddAspNetCoreInstrumentation only enables the Microsoft.AspNetCore.Hosting meter.
        .AddMeter("Microsoft.AspNetCore.Server.Kestrel")
        .AddOtlpExporter());

var app = builder.Build();

// ---- Endpoints -------------------------------------------------------------------------------
// There is deliberately no /metrics endpoint. The backend pushes over OTLP and never gets
// scraped, which is the whole point of putting a collector in front of Prometheus.

// Health is reachable twice on purpose: Kubernetes probes hit the pod directly on /health/*,
// while a browser reaches it through the frontend proxy, which only forwards /api/*.
foreach (var prefix in new[] { "", "/api" })
{
    app.MapGet($"{prefix}/health/live", () => Results.Ok("live"));

    app.MapGet($"{prefix}/health/ready", async (BrokerReadiness readiness, CancellationToken ct) =>
        await readiness.IsReadyAsync(ct)
            ? Results.Ok("ready")
            : Results.StatusCode(StatusCodes.Status503ServiceUnavailable));
}

app.MapPost("/api/messages", async (
    SendMessageRequest request,
    BackendOptions cfg,
    ChatProducer producer,
    ILogger<Program> logger,
    CancellationToken ct) =>
{
    // The timestamp comes from the server clock, never from the request.
    if (!ChatMessage.TryCreate(
            request.Room, request.Nickname, request.Text,
            DateTimeOffset.UtcNow, cfg.MaxMessageLength,
            out var message, out var error))
    {
        return Results.BadRequest(new { error });
    }

    try
    {
        await producer.ProduceAsync(message, ct);
    }
    catch (ProduceException<string, string> e)
    {
        logger.LogError("Produce failed: {Reason}", e.Error.Reason);
        return Results.StatusCode(ChatProducer.StatusCodeFor(e.Error));
    }

    return Results.Accepted();
});

app.MapGet("/api/stream", IResult (
    HttpContext context,
    string? room,
    ChatBroadcaster broadcaster,
    IHostApplicationLifetime lifetime) =>
{
    if (string.IsNullOrWhiteSpace(room))
    {
        return Results.BadRequest(new { error = "Query parameter 'room' is required." });
    }

    // ServerSentEvents sets Content-Type, Cache-Control, Pragma and Content-Encoding itself and
    // disables response buffering. This header is an nginx-specific hint it does not know about,
    // and it only matters when a buffering proxy sits in front — Caddy streams SSE unbuffered.
    context.Response.Headers["X-Accel-Buffering"] = "no";

    // EventSource resends the id of the last frame it saw whenever it reconnects on its own, which
    // is what the frontend relies on after a pod restart. Anything unparseable is treated as a
    // fresh connection: replaying the room again is the harmless answer, dropping it is not.
    var lastEventId =
        long.TryParse(
            context.Request.Headers["Last-Event-ID"].ToString(),
            NumberStyles.Integer,
            CultureInfo.InvariantCulture,
            out var seen)
            ? seen
            : -1;

    // ApplicationStopping, because RequestAborted alone never fires on a rolling update: the
    // browser is still there and the connection is still good. Without it the stream keeps
    // heartbeating from a terminating pod for the whole 25s shutdown timeout, and the browser has
    // no reason to move to the replica that is already ready.
    return TypedResults.ServerSentEvents(
        ChatStream.Create(
            broadcaster,
            room.Trim(),
            ChatStream.DefaultHeartbeatInterval,
            lastEventId,
            lifetime.ApplicationStopping));
});

app.Run();

// Exposed so the test project can reference the entry-point assembly.
public partial class Program;
