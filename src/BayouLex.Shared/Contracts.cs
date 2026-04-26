namespace BayouLex.Shared;

public sealed record BayouLexInitResponse(
    string AppName,
    string ApiVersion,
    string DatasetVersion,
    string MinimumClientVersion,
    string PublicBaseUrl,
    IReadOnlyList<string> Capabilities);

public sealed record CatalogResponse(
    string DatasetVersion,
    int DocumentCount,
    IReadOnlyList<CategorySummary> Categories,
    IReadOnlyList<BundleSummary> Bundles);

public sealed record CategorySummary(string Name, int DocumentCount);

public sealed record BundleSummary(string Category, string Name, int DocumentCount);

public sealed record SearchResponse(
    string Query,
    int Limit,
    int Offset,
    int TotalReturned,
    IReadOnlyList<SearchResultDto> Results);

public sealed record SearchResultDto(
    string DocumentKey,
    string DocId,
    string Citation,
    string Title,
    string Category,
    string Bundle,
    string StatusLabel,
    string Url,
    string Snippet);

public sealed record DocumentDto(
    string DocumentKey,
    string DocId,
    string Citation,
    string Title,
    string Category,
    string Bundle,
    string SessionId,
    string Chamber,
    string StatusGroup,
    string StatusLabel,
    string Url,
    string LocalFile,
    string Text,
    string MetadataJson);

public sealed record OfflineManifestDto(
    string DatasetVersion,
    string FileName,
    long CompressedBytes,
    long UncompressedBytes,
    string Sha256,
    int ChunkSizeBytes,
    IReadOnlyList<OfflineChunkDto> Chunks);

public sealed record OfflineChunkDto(
    int Number,
    string FileName,
    long Offset,
    long Bytes,
    string Sha256);
