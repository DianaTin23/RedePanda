using System.Globalization;
using Confluent.Kafka;
using OpenTelemetry;
using OpenTelemetry.Metrics;
using RedePanda.Backend;
using RedePanda.Contracts;

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

    context.Response.Headers["X-Accel-Buffering"] = "no";

    var lastEventId =
        long.TryParse(
            context.Request.Headers["Last-Event-ID"].ToString(),
            NumberStyles.Integer,
            CultureInfo.InvariantCulture,
            out var seen)
            ? seen
            : -1;

    return TypedResults.ServerSentEvents(
        ChatStream.Create(
            broadcaster,
            room.Trim(),
            ChatStream.DefaultHeartbeatInterval,
            lastEventId,
            lifetime.ApplicationStopping));
});

app.Run();

/// <summary>Exposed so the test project can reference the entry-point assembly.</summary>
public partial class Program;
