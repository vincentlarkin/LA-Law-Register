namespace BayouLex.Api.Services;

public sealed record BayouLexApiOptions(
    string DatasetPath,
    string OfflineRoot,
    string PublicBaseUrl)
{
    public static BayouLexApiOptions FromConfiguration(IConfiguration configuration)
    {
        var datasetPath = configuration["BAYOULEX_DATASET_PATH"]
            ?? configuration["BayouLex:DatasetPath"]
            ?? "/data/bayoulex-content.sqlite";
        var offlineRoot = configuration["BAYOULEX_OFFLINE_ROOT"]
            ?? configuration["BayouLex:OfflineRoot"]
            ?? "/data/offline";
        var publicBaseUrl = configuration["BAYOULEX_PUBLIC_BASE_URL"]
            ?? configuration["BayouLex:PublicBaseUrl"]
            ?? "https://api.ladf.us/bayoulex/v1";
        return new BayouLexApiOptions(datasetPath, offlineRoot, publicBaseUrl.TrimEnd('/'));
    }
}
