using System.Globalization;
using Confluent.Kafka;
using OpenTelemetry;
using OpenTelemetry.Metrics;
using RedeTim.Backend;
using RedeTim.Contracts;

var builder = WebApplication.CreateBuilder(args);
var options = BackendOptions.FromEnvironment();

builder.Logging.ClearProviders();
builder.Logging.AddJsonConsole(c =>
{
    c.IncludeScopes = false;
    c.TimestampFormat = "yyyy-MM-dd'T'HH:mm:ss.fffffff'Z'";
    c.UseUtcTimestamp = true;
});
builder.Logging.SetMinimumLevel(options.LogLevel);

if (options.LogLevel > LogLevel.Debug)
{
    builder.Logging.AddFilter("Microsoft.AspNetCore", LogLevel.Warning);
}

builder.Services.AddSingleton(options);
builder.Services.AddSingleton<ChatMetrics>();
builder.Services.AddSingleton<ChatBroadcaster>();
builder.Services.AddSingleton<ChatProducer>();
builder.Services.AddSingleton<BrokerReadiness>();
builder.Services.AddHostedService<ChatConsumerService>();

builder.Services.AddSingleton<PresenceStore>();

// A single registration, not AddSingleton<PresenceProducer>() plus a delegate wrapping it in
// IPresenceProducer: two registrations for the same disposable instance make the container
// track and dispose it twice, and the second Dispose() throws because librdkafka's handle is
// already gone by then.
builder.Services.AddSingleton<IPresenceProducer>(sp => ActivatorUtilities.CreateInstance<PresenceProducer>(sp));

builder.Services.AddHostedService<PresenceConsumerService>();

builder.Services.Configure<HostOptions>(o => o.ShutdownTimeout = TimeSpan.FromSeconds(25));

builder.Services.AddOpenTelemetry()
    .WithMetrics(m => m
        .AddAspNetCoreInstrumentation()
        .AddRuntimeInstrumentation()
        .AddMeter(ChatMetrics.MeterName)
        .AddMeter("Microsoft.AspNetCore.Server.Kestrel")
        .AddOtlpExporter());

var app = builder.Build();

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

app.MapPost("/api/join", async (
    JoinRequest request,
    PresenceStore store,
    IPresenceProducer producer,
    ILogger<Program> logger,
    CancellationToken ct) =>
{
    if (!ChatMessage.TryNormalizeRoomAndNickname(
            request.Room, request.Nickname, out var room, out var nickname, out var error))
    {
        return Results.BadRequest(new { error });
    }

    if (store.IsTaken(room, nickname, DateTimeOffset.UtcNow))
    {
        return Results.Conflict(new { error = $"'{nickname}' is already taken in '{room}'." });
    }

    try
    {
        await producer.RenewAsync(room, nickname, ct);
    }
    catch (ProduceException<string, string> e)
    {
        logger.LogError("Presence produce failed: {Reason}", e.Error.Reason);
        return Results.StatusCode(PresenceProducer.StatusCodeFor(e.Error));
    }

    return Results.Ok();
});

app.MapGet("/api/presence", (string? room, PresenceStore store) =>
{
    if (!ChatMessage.TryNormalizeRoom(room, out var normalizedRoom, out var error))
    {
        return Results.BadRequest(new { error });
    }

    return Results.Ok(new { nicknames = store.ActiveNicknames(normalizedRoom, DateTimeOffset.UtcNow) });
});

app.MapGet("/api/stream", IResult (
    HttpContext context,
    string? room,
    string? nickname,
    ChatBroadcaster broadcaster,
    IPresenceProducer presenceProducer,
    ILogger<Program> logger,
    IHostApplicationLifetime lifetime) =>
{
    if (!ChatMessage.TryNormalizeRoom(room, out var trimmedRoom, out var roomError))
    {
        return Results.BadRequest(new { error = roomError });
    }

    context.Response.Headers["X-Accel-Buffering"] = "no";

    var lastEventId =
        long.TryParse(
            context.Request.Headers["Last-Event-ID"].ToString(),
            NumberStyles.Integer,
            CultureInfo.InvariantCulture,
            out var seen)
            ? seen
            : -1;

    // A missing or invalid nickname just means "don't track presence for this connection" —
    // presence is a soft UX gate behind POST /api/join, not a security boundary the stream
    // itself must enforce, so there is no reason to fail the whole connection over it. A
    // nickname that *is* tracked, though, goes through the same normalization and
    // reserved-name check as /api/join, so this path can't be used to spoof presence under a
    // stripped or invisible-character variant of a reserved or already-taken name.
    var presence =
        ChatMessage.TryNormalizeNickname(nickname, out var trimmedNickname, out _)
            ? new PresenceSession(presenceProducer, trimmedRoom, trimmedNickname, logger)
            : null;

    return TypedResults.ServerSentEvents(
        ChatStream.Create(
            broadcaster,
            trimmedRoom,
            ChatStream.DefaultHeartbeatInterval,
            lastEventId,
            presence,
            lifetime.ApplicationStopping));
});

app.Run();

/// <summary>Exposed so the test project can reference the entry-point assembly.</summary>
public partial class Program;
