using System.Text.Json;
using BayouLex.Shared;

namespace BayouLex.Api.Services;

public sealed class OfflineArtifactStore
{
    private readonly string _offlineRoot;
    private readonly JsonSerializerOptions _jsonOptions = new(JsonSerializerDefaults.Web);

    public OfflineArtifactStore(BayouLexApiOptions options)
    {
        _offlineRoot = options.OfflineRoot;
    }

    public OfflineManifestDto? GetManifest(string version)
    {
        var path = ManifestPath(version);
        if (!File.Exists(path))
        {
            return null;
        }

        using var stream = File.OpenRead(path);
        return JsonSerializer.Deserialize<OfflineManifestDto>(stream, _jsonOptions);
    }

    public (FileStream Stream, ChunkMetadata Metadata)? OpenChunk(string version, int chunkNumber)
    {
        var manifest = GetManifest(version);
        var chunk = manifest?.Chunks.FirstOrDefault(item => item.Number == chunkNumber);
        if (chunk is null)
        {
            return null;
        }

        var path = Path.Combine(_offlineRoot, version, chunk.FileName);
        if (!File.Exists(path))
        {
            return null;
        }

        var info = new FileInfo(path);
        return (
            File.Open(path, FileMode.Open, FileAccess.Read, FileShare.Read),
            new ChunkMetadata(chunk.FileName, chunk.Sha256, info.LastWriteTimeUtc));
    }

    private string ManifestPath(string version) => Path.Combine(_offlineRoot, version, "manifest.json");

    public sealed record ChunkMetadata(string FileName, string Sha256, DateTimeOffset LastModified);
}
