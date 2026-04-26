using System.IO.Compression;
using System.IO;
using System.Net.Http;
using System.Net.Http.Json;
using System.Security.Cryptography;
using BayouLex.Shared;

namespace BayouLex.Client.Windows.Services;

public sealed class OfflineDownloadService
{
    private readonly HttpClient _http;

    public OfflineDownloadService(HttpClient http)
    {
        _http = http;
    }

    public static string DefaultDatabasePath()
        => Path.Combine(AppDataRoot(), "bayoulex-content.sqlite");

    public async Task<string> DownloadAsync(
        string datasetVersion,
        IProgress<double> progress,
        CancellationToken cancellationToken)
    {
        var manifest = await _http.GetFromJsonAsync<OfflineManifestDto>(
            $"offline/{Uri.EscapeDataString(datasetVersion)}/manifest",
            cancellationToken) ?? throw new InvalidOperationException("Offline manifest not found.");

        var root = AppDataRoot();
        Directory.CreateDirectory(root);
        var chunksDir = Path.Combine(root, "chunks", datasetVersion);
        Directory.CreateDirectory(chunksDir);
        var compressedPath = Path.Combine(root, manifest.FileName);
        var dbPath = DefaultDatabasePath();

        long downloaded = 0;
        foreach (var chunk in manifest.Chunks)
        {
            cancellationToken.ThrowIfCancellationRequested();
            var chunkPath = Path.Combine(chunksDir, chunk.FileName);
            if (!File.Exists(chunkPath) || !HashMatches(chunkPath, chunk.Sha256))
            {
                using var response = await _http.GetAsync(
                    $"offline/{Uri.EscapeDataString(datasetVersion)}/chunks/{chunk.Number}",
                    HttpCompletionOption.ResponseHeadersRead,
                    cancellationToken);
                response.EnsureSuccessStatusCode();
                await using var source = await response.Content.ReadAsStreamAsync(cancellationToken);
                await using var target = File.Create(chunkPath);
                await source.CopyToAsync(target, cancellationToken);
            }

            if (!HashMatches(chunkPath, chunk.Sha256))
            {
                throw new InvalidOperationException($"Chunk hash mismatch: {chunk.FileName}");
            }

            downloaded += chunk.Bytes;
            progress.Report((double)downloaded / Math.Max(1, manifest.CompressedBytes) * 60.0);
        }

        await using (var combined = File.Create(compressedPath))
        {
            foreach (var chunk in manifest.Chunks)
            {
                await using var source = File.OpenRead(Path.Combine(chunksDir, chunk.FileName));
                await source.CopyToAsync(combined, cancellationToken);
            }
        }
        if (!HashMatches(compressedPath, manifest.Sha256))
        {
            throw new InvalidOperationException("Compressed offline snapshot hash mismatch.");
        }
        progress.Report(75);

        await using (var source = File.OpenRead(compressedPath))
        await using (var brotli = new BrotliStream(source, CompressionMode.Decompress, leaveOpen: false))
        await using (var target = File.Create(dbPath))
        {
            await brotli.CopyToAsync(target, cancellationToken);
        }
        progress.Report(100);
        return dbPath;
    }

    private static string AppDataRoot()
        => Path.Combine(Environment.GetFolderPath(Environment.SpecialFolder.LocalApplicationData), "BayouLex");

    private static bool HashMatches(string path, string expectedSha256)
    {
        if (!File.Exists(path))
        {
            return false;
        }

        using var stream = File.OpenRead(path);
        var actual = Convert.ToHexString(SHA256.HashData(stream)).ToLowerInvariant();
        return string.Equals(actual, expectedSha256, StringComparison.OrdinalIgnoreCase);
    }
}
