using System.IO;
using System.Text.RegularExpressions;
using BayouLex.Shared;
using Microsoft.Data.Sqlite;

namespace BayouLex.Client.Windows.Services;

public sealed partial class LocalSqliteSearchService
{
    private readonly string _connectionString;

    public LocalSqliteSearchService(string dbPath)
    {
        if (!File.Exists(dbPath))
        {
            throw new FileNotFoundException("Offline BayouLex dataset not found.", dbPath);
        }

        var builder = new SqliteConnectionStringBuilder
        {
            DataSource = dbPath,
            Mode = SqliteOpenMode.ReadOnly,
            Cache = SqliteCacheMode.Shared,
        };
        _connectionString = builder.ToString();
    }

    public Task<SearchResponse> SearchAsync(string rawQuery, string? category, int limit, CancellationToken cancellationToken)
        => Task.Run(() =>
        {
            var ftsQuery = ToFtsPrefixQuery(rawQuery);
            if (string.IsNullOrWhiteSpace(ftsQuery))
            {
                return new SearchResponse(rawQuery, limit, 0, 0, []);
            }

            using var con = Open();
            using var cmd = con.CreateCommand();
            cmd.Parameters.AddWithValue("$query", ftsQuery);
            cmd.Parameters.AddWithValue("$limit", limit);
            var where = "";
            if (!string.IsNullOrWhiteSpace(category))
            {
                where = " AND d.category = $category";
                cmd.Parameters.AddWithValue("$category", category);
            }
            cmd.CommandText = $"""
                SELECT d.document_key, d.doc_id, d.citation, d.title, d.category, d.bundle,
                       d.status_label, d.url, snippet(documents_fts, 2, '[', ']', ' ... ', 12) AS snippet
                FROM documents_fts
                JOIN documents d ON d.id = documents_fts.rowid
                WHERE documents_fts MATCH $query {where}
                ORDER BY bm25(documents_fts)
                LIMIT $limit;
                """;

            var rows = new List<SearchResultDto>();
            using var reader = cmd.ExecuteReader();
            while (reader.Read())
            {
                cancellationToken.ThrowIfCancellationRequested();
                rows.Add(new SearchResultDto(
                    reader.GetString(0),
                    reader.GetString(1),
                    reader.GetString(2),
                    reader.GetString(3),
                    reader.GetString(4),
                    reader.GetString(5),
                    reader.GetString(6),
                    reader.GetString(7),
                    reader.IsDBNull(8) ? "" : reader.GetString(8).Trim()));
            }
            return new SearchResponse(rawQuery, limit, 0, rows.Count, rows);
        }, cancellationToken);

    public Task<DocumentDto?> GetDocumentAsync(string documentKey, CancellationToken cancellationToken)
        => Task.Run(() =>
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

            cancellationToken.ThrowIfCancellationRequested();
            return new DocumentDto(
                reader.GetString(0),
                reader.GetString(1),
                reader.GetString(2),
                reader.GetString(3),
                reader.GetString(4),
                reader.GetString(5),
                reader.GetString(6),
                reader.GetString(7),
                reader.GetString(8),
                reader.GetString(9),
                reader.GetString(10),
                reader.GetString(11),
                reader.GetString(12),
                reader.GetString(13));
        }, cancellationToken);

    private SqliteConnection Open()
    {
        var con = new SqliteConnection(_connectionString);
        con.Open();
        using var cmd = con.CreateCommand();
        cmd.CommandText = "PRAGMA query_only = ON;";
        cmd.ExecuteNonQuery();
        return con;
    }

    private static string ToFtsPrefixQuery(string rawQuery)
    {
        var trimmed = rawQuery.Trim();
        if (trimmed.Any(ch => "\"*():{}".Contains(ch)) || OperatorRegex().IsMatch(trimmed))
        {
            return trimmed;
        }
        return string.Join(" ", TokenRegex()
            .Matches(trimmed)
            .Select(match => match.Value)
            .Where(token => token.Length >= 3)
            .Select(token => token + "*"));
    }

    [GeneratedRegex("[A-Za-z0-9_]+")]
    private static partial Regex TokenRegex();

    [GeneratedRegex("\\b(?:AND|OR|NOT|NEAR)\\b", RegexOptions.IgnoreCase)]
    private static partial Regex OperatorRegex();
}
