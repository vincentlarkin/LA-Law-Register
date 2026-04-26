using System.Threading.RateLimiting;
using BayouLex.Api.Services;
using BayouLex.Shared;
using Microsoft.AspNetCore.Http.HttpResults;
using Microsoft.AspNetCore.RateLimiting;
using Microsoft.Net.Http.Headers;

var builder = WebApplication.CreateBuilder(args);

builder.Services.AddSingleton(BayouLexApiOptions.FromConfiguration(builder.Configuration));
builder.Services.AddSingleton<ContentStore>();
builder.Services.AddSingleton<OfflineArtifactStore>();

builder.Services.AddRateLimiter(options =>
{
    options.RejectionStatusCode = StatusCodes.Status429TooManyRequests;
    options.AddFixedWindowLimiter("search", limiter =>
    {
        limiter.Window = TimeSpan.FromMinutes(1);
        limiter.PermitLimit = 90;
        limiter.QueueLimit = 0;
    });
    options.AddFixedWindowLimiter("offline", limiter =>
    {
        limiter.Window = TimeSpan.FromHours(1);
        limiter.PermitLimit = 32;
        limiter.QueueLimit = 0;
    });
    options.GlobalLimiter = PartitionedRateLimiter.Create<HttpContext, string>(context =>
        RateLimitPartition.GetFixedWindowLimiter(
            context.Connection.RemoteIpAddress?.ToString() ?? "unknown",
            _ => new FixedWindowRateLimiterOptions
            {
                PermitLimit = 240,
                Window = TimeSpan.FromMinutes(1),
                QueueLimit = 0,
                AutoReplenishment = true,
            }));
});

var app = builder.Build();
app.UseRateLimiter();

app.MapGet("/", () => TypedResults.Ok(new
{
    app = "BayouLex",
    api = "/bayoulex/v1/",
    health = "/healthz",
    endpoints = new[]
    {
        "/bayoulex/v1/init",
        "/bayoulex/v1/catalog",
        "/bayoulex/v1/search?q=capital",
        "/bayoulex/v1/documents/{documentKey}",
        "/bayoulex/v1/offline/{version}/manifest",
    },
}));

var api = app.MapGroup("/bayoulex/v1");

api.MapGet("/", Results<Ok<BayouLexInitResponse>, ProblemHttpResult> (
    ContentStore content,
    BayouLexApiOptions options) =>
{
    var version = content.GetDatasetVersion();
    return TypedResults.Ok(new BayouLexInitResponse(
        AppName: "BayouLex",
        ApiVersion: "v1",
        DatasetVersion: version,
        MinimumClientVersion: "1.0.0",
        PublicBaseUrl: options.PublicBaseUrl,
        Capabilities: ["search", "catalog", "document-detail", "offline-snapshot"]));
});

api.MapGet("/init", Results<Ok<BayouLexInitResponse>, ProblemHttpResult> (
    ContentStore content,
    BayouLexApiOptions options) =>
{
    var version = content.GetDatasetVersion();
    return TypedResults.Ok(new BayouLexInitResponse(
        AppName: "BayouLex",
        ApiVersion: "v1",
        DatasetVersion: version,
        MinimumClientVersion: "1.0.0",
        PublicBaseUrl: options.PublicBaseUrl,
        Capabilities: ["search", "catalog", "document-detail", "offline-snapshot"]));
});

api.MapGet("/catalog", Results<Ok<CatalogResponse>, ProblemHttpResult> (ContentStore content) =>
    TypedResults.Ok(content.GetCatalog()));

api.MapGet("/search", Results<Ok<SearchResponse>, BadRequest<string>, ProblemHttpResult> (
    string q,
    string? category,
    string? bundle,
    int? limit,
    int? offset,
    ContentStore content,
    CancellationToken cancellationToken) =>
{
    if (string.IsNullOrWhiteSpace(q))
    {
        return TypedResults.BadRequest("Query is required.");
    }

    var safeLimit = Math.Clamp(limit.GetValueOrDefault(50), 1, 100);
    var safeOffset = Math.Max(0, offset.GetValueOrDefault(0));
    return TypedResults.Ok(content.Search(q, category, bundle, safeLimit, safeOffset, cancellationToken));
}).RequireRateLimiting("search");

api.MapGet("/documents/{documentKey}", Results<Ok<DocumentDto>, NotFound> (
    string documentKey,
    ContentStore content) =>
{
    var doc = content.GetDocument(documentKey);
    return doc is null ? TypedResults.NotFound() : TypedResults.Ok(doc);
});

api.MapGet("/offline/{version}/manifest", Results<Ok<OfflineManifestDto>, NotFound> (
    string version,
    OfflineArtifactStore artifacts) =>
{
    var manifest = artifacts.GetManifest(version);
    return manifest is null ? TypedResults.NotFound() : TypedResults.Ok(manifest);
}).RequireRateLimiting("offline");

api.MapGet("/offline/{version}/chunks/{chunkNumber:int}", Results<FileStreamHttpResult, NotFound> (
    string version,
    int chunkNumber,
    OfflineArtifactStore artifacts) =>
{
    var chunk = artifacts.OpenChunk(version, chunkNumber);
    if (chunk is null)
    {
        return TypedResults.NotFound();
    }

    var (stream, metadata) = chunk.Value;
    return TypedResults.File(
        fileStream: stream,
        contentType: "application/octet-stream",
        fileDownloadName: metadata.FileName,
        lastModified: metadata.LastModified,
        entityTag: new EntityTagHeaderValue($"\"{metadata.Sha256}\""),
        enableRangeProcessing: true);
}).RequireRateLimiting("offline");

app.MapGet("/healthz", () => TypedResults.Ok(new { status = "ok" }));

app.Run();
