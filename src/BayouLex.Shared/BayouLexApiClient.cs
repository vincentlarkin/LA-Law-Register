using System.Net.Http.Json;

namespace BayouLex.Shared;

public sealed class BayouLexApiClient
{
    private readonly HttpClient _http;

    public BayouLexApiClient(HttpClient http)
    {
        _http = http;
    }

    public Task<BayouLexInitResponse?> GetInitAsync(CancellationToken cancellationToken = default)
        => _http.GetFromJsonAsync<BayouLexInitResponse>("init", cancellationToken);

    public Task<CatalogResponse?> GetCatalogAsync(CancellationToken cancellationToken = default)
        => _http.GetFromJsonAsync<CatalogResponse>("catalog", cancellationToken);

    public Task<SearchResponse?> SearchAsync(
        string query,
        string? category = null,
        string? bundle = null,
        int limit = 50,
        int offset = 0,
        CancellationToken cancellationToken = default)
    {
        var args = new Dictionary<string, string?>
        {
            ["q"] = query,
            ["category"] = category,
            ["bundle"] = bundle,
            ["limit"] = limit.ToString(),
            ["offset"] = offset.ToString(),
        };
        var url = "search?" + string.Join("&", args
            .Where(pair => !string.IsNullOrWhiteSpace(pair.Value))
            .Select(pair => $"{Uri.EscapeDataString(pair.Key)}={Uri.EscapeDataString(pair.Value!)}"));
        return _http.GetFromJsonAsync<SearchResponse>(url, cancellationToken);
    }

    public Task<DocumentDto?> GetDocumentAsync(string documentKey, CancellationToken cancellationToken = default)
        => _http.GetFromJsonAsync<DocumentDto>($"documents/{Uri.EscapeDataString(documentKey)}", cancellationToken);

    public Task<OfflineManifestDto?> GetOfflineManifestAsync(string datasetVersion, CancellationToken cancellationToken = default)
        => _http.GetFromJsonAsync<OfflineManifestDto>($"offline/{Uri.EscapeDataString(datasetVersion)}/manifest", cancellationToken);
}
