using System.Threading.Channels;
using Confluent.Kafka;
using Microsoft.AspNetCore.Http.Features;
using OpenTelemetry;
using OpenTelemetry.Metrics;
using RedePanda.Backend;
using RedePanda.Contracts;

var builder = WebApplication.CreateBuilder(args);
var options = BackendOptions.FromEnvironment();

// ---- Logging ---------------------------------------------------------------------------------
// Structured, to stdout, no log files: the platform collects them (12-Factor "Logs").
builder.Logging.ClearProviders();
builder.Logging.AddJsonConsole(c => c.IncludeScopes = false);
builder.Logging.SetMinimumLevel(
    Enum.TryParse<LogLevel>(Environment.GetEnvironmentVariable("LOG_LEVEL"), ignoreCase: true, out var level)
        ? level
        : LogLevel.Information);

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

app.MapGet("/health/live", () => Results.Ok("live"));

app.MapGet("/health/ready", async (BrokerReadiness readiness, CancellationToken ct) =>
    await readiness.IsReadyAsync(ct)
        ? Results.Ok("ready")
        : Results.StatusCode(StatusCodes.Status503ServiceUnavailable));

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
        return Results.StatusCode(StatusCodes.Status502BadGateway);
    }

    return Results.Accepted();
});

app.MapGet("/api/stream", async (
    HttpContext context,
    string? room,
    ChatBroadcaster broadcaster,
    CancellationToken ct) =>
{
    if (string.IsNullOrWhiteSpace(room))
    {
        await Results.BadRequest(new { error = "Query parameter 'room' is required." })
            .ExecuteAsync(context);
        return;
    }

    // .NET 9 has no TypedResults.ServerSentEvents — that arrived in ASP.NET Core 10 — so the
    // response is shaped by hand. This mirrors what the built-in result does.
    var response = context.Response;
    response.ContentType = "text/event-stream";
    response.Headers.CacheControl = "no-cache,no-store";
    response.Headers.Pragma = "no-cache";
    response.Headers.ContentEncoding = "identity";

    // Only relevant when a buffering proxy sits in front. Caddy streams SSE unbuffered anyway.
    response.Headers["X-Accel-Buffering"] = "no";

    context.Features.Get<IHttpResponseBodyFeature>()?.DisableBuffering();
    await response.Body.FlushAsync(ct);

    using var subscription = broadcaster.Subscribe(room.Trim());

    try
    {
        while (!ct.IsCancellationRequested)
        {
            // Wait for a message, but never longer than the heartbeat interval: a comment line
            // keeps idle connections alive through proxies and reveals dead peers to us.
            using var heartbeat = CancellationTokenSource.CreateLinkedTokenSource(ct);
            heartbeat.CancelAfter(TimeSpan.FromSeconds(15));

            try
            {
                var message = await subscription.Reader.ReadAsync(heartbeat.Token);
                await response.WriteAsync(
                    $"data: {ChatMessageSerializer.Serialize(message)}\n\n", ct);
            }
            catch (OperationCanceledException) when (!ct.IsCancellationRequested)
            {
                await response.WriteAsync(": ping\n\n", ct);
            }

            await response.Body.FlushAsync(ct);
        }
    }
    catch (OperationCanceledException)
    {
        // The browser went away, or the process is shutting down. Both are normal.
    }
    catch (ChannelClosedException)
    {
        // Subscription was completed during shutdown.
    }
});

app.Run();

// Exposed so the test project can reference the entry-point assembly.
public partial class Program;
