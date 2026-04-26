using System.Text.RegularExpressions;
using BayouLex.Shared;
using Microsoft.Data.Sqlite;

namespace BayouLex.Api.Services;

public sealed partial class ContentStore
{
    private const int MaxSnippetTokens = 12;
    private readonly string _connectionString;

    public ContentStore(BayouLexApiOptions options)
    {
        if (!File.Exists(options.DatasetPath))
        {
            throw new FileNotFoundException("BayouLex dataset not found.", options.DatasetPath);
        }

        var builder = new SqliteConnectionStringBuilder
        {
            DataSource = options.DatasetPath,
            Mode = SqliteOpenMode.ReadOnly,
            Cache = SqliteCacheMode.Shared,
        };
        _connectionString = builder.ToString();
    }

    public string GetDatasetVersion()
    {
        using var con = Open();
        using var cmd = con.CreateCommand();
        cmd.CommandText = "SELECT value FROM dataset_metadata WHERE key = 'dataset_version' LIMIT 1;";
        return (cmd.ExecuteScalar() as string) ?? "unknown";
    }

    public CatalogResponse GetCatalog()
    {
        using var con = Open();
        var version = GetMetadataValue(con, "dataset_version") ?? "unknown";
        var documentCount = Convert.ToInt32(Scalar(con, "SELECT COUNT(*) FROM documents;"));
        var categories = Query(con, """
            SELECT category, COUNT(*) AS doc_count
            FROM documents
            GROUP BY category
            ORDER BY category;
            """, reader => new CategorySummary(
                reader.GetString(0),
                reader.GetInt32(1)));
        var bundles = Query(con, """
            SELECT category, bundle, COUNT(*) AS doc_count
            FROM documents
            GROUP BY category, bundle
            ORDER BY category, bundle;
            """, reader => new BundleSummary(
                reader.GetString(0),
                reader.GetString(1),
                reader.GetInt32(2)));
        return new CatalogResponse(version, documentCount, categories, bundles);
    }

    public SearchResponse Search(
        string rawQuery,
        string? category,
        string? bundle,
        int limit,
        int offset,
        CancellationToken cancellationToken)
    {
        var ftsQuery = ToFtsPrefixQuery(rawQuery);
        if (string.IsNullOrWhiteSpace(ftsQuery))
        {
            return new SearchResponse(rawQuery, limit, offset, 0, []);
        }

        using var con = Open();
        using var cmd = con.CreateCommand();
        var filters = new List<string>();
        cmd.Parameters.AddWithValue("$query", ftsQuery);
        cmd.Parameters.AddWithValue("$limit", limit);
        cmd.Parameters.AddWithValue("$offset", offset);
        if (!string.IsNullOrWhiteSpace(category))
        {
            filters.Add("d.category = $category");
            cmd.Parameters.AddWithValue("$category", category);
        }
        if (!string.IsNullOrWhiteSpace(bundle))
        {
            filters.Add("d.bundle = $bundle");
            cmd.Parameters.AddWithValue("$bundle", bundle);
        }
        var where = filters.Count == 0 ? "" : " AND " + string.Join(" AND ", filters);
        cmd.CommandText = $"""
            SELECT
              d.document_key,
              d.doc_id,
              d.citation,
              d.title,
              d.category,
              d.bundle,
              d.status_label,
              d.url,
              snippet(documents_fts, 2, '[', ']', ' ... ', {MaxSnippetTokens}) AS snippet
            FROM documents_fts
            JOIN documents d ON d.id = documents_fts.rowid
            WHERE documents_fts MATCH $query {where}
            ORDER BY bm25(documents_fts)
            LIMIT $limit OFFSET $offset;
            """;

        var results = new List<SearchResultDto>();
        using var reader = cmd.ExecuteReader();
        while (reader.Read())
        {
            cancellationToken.ThrowIfCancellationRequested();
            results.Add(new SearchResultDto(
                DocumentKey: reader.GetString(0),
                DocId: reader.GetString(1),
                Citation: reader.GetString(2),
                Title: reader.GetString(3),
                Category: reader.GetString(4),
                Bundle: reader.GetString(5),
                StatusLabel: reader.GetString(6),
                Url: reader.GetString(7),
                Snippet: reader.IsDBNull(8) ? "" : reader.GetString(8).Trim()));
        }
        return new SearchResponse(rawQuery, limit, offset, results.Count, results);
    }

    public DocumentDto? GetDocument(string documentKey)
    {
        using var con = Open();
        using var cmd = con.CreateCommand();
        cmd.CommandText = """
            SELECT document_key, doc_id, citation, title, category, bundle, session_id, chamber,
                   status_group, status_label, url, local_file, text, metadata_json
            FROM documents
            WHERE document_key = $document_key
            LIMIT 1;
            """;
        cmd.Parameters.AddWithValue("$document_key", documentKey);
        using var reader = cmd.ExecuteReader();
        if (!reader.Read())
        {
            return null;
        }

        return new DocumentDto(
            DocumentKey: reader.GetString(0),
            DocId: reader.GetString(1),
            Citation: reader.GetString(2),
            Title: reader.GetString(3),
            Category: reader.GetString(4),
            Bundle: reader.GetString(5),
            SessionId: reader.GetString(6),
            Chamber: reader.GetString(7),
            StatusGroup: reader.GetString(8),
            StatusLabel: reader.GetString(9),
            Url: reader.GetString(10),
            LocalFile: reader.GetString(11),
            Text: reader.GetString(12),
            MetadataJson: reader.GetString(13));
    }

    private SqliteConnection Open()
    {
        var con = new SqliteConnection(_connectionString);
        con.Open();
        using var cmd = con.CreateCommand();
        cmd.CommandText = "PRAGMA query_only = ON;";
        cmd.ExecuteNonQuery();
        return con;
    }

    private static object? Scalar(SqliteConnection con, string sql)
    {
        using var cmd = con.CreateCommand();
        cmd.CommandText = sql;
        return cmd.ExecuteScalar();
    }

    private static string? GetMetadataValue(SqliteConnection con, string key)
    {
        using var cmd = con.CreateCommand();
        cmd.CommandText = "SELECT value FROM dataset_metadata WHERE key = $key LIMIT 1;";
        cmd.Parameters.AddWithValue("$key", key);
        return cmd.ExecuteScalar() as string;
    }

    private static List<T> Query<T>(SqliteConnection con, string sql, Func<SqliteDataReader, T> map)
    {
        using var cmd = con.CreateCommand();
        cmd.CommandText = sql;
        using var reader = cmd.ExecuteReader();
        var results = new List<T>();
        while (reader.Read())
        {
            results.Add(map(reader));
        }
        return results;
    }

    private static string ToFtsPrefixQuery(string rawQuery)
    {
        var trimmed = rawQuery.Trim();
        if (trimmed.Length == 0)
        {
            return "";
        }

        if (trimmed.Any(ch => "\"*():{}".Contains(ch)) || OperatorRegex().IsMatch(trimmed))
        {
            return trimmed;
        }

        var tokens = TokenRegex()
            .Matches(trimmed)
            .Select(match => match.Value)
            .Where(token => token.Length >= 3)
            .Select(token => token + "*");
        return string.Join(" ", tokens);
    }

    [GeneratedRegex("[A-Za-z0-9_]+")]
    private static partial Regex TokenRegex();

    [GeneratedRegex("\\b(?:AND|OR|NOT|NEAR)\\b", RegexOptions.IgnoreCase)]
    private static partial Regex OperatorRegex();
}
