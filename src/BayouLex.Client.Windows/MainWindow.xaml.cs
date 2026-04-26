using System.Collections.ObjectModel;
using System.Net.Http;
using System.Windows;
using System.Windows.Controls;
using System.Windows.Threading;
using BayouLex.Client.Windows.Services;
using BayouLex.Shared;

namespace BayouLex.Client.Windows;

public partial class MainWindow : Window
{
    private const int MaxPreviewChars = 180_000;
    private readonly DispatcherTimer _searchDebounce;
    private HttpClient _http = new();
    private BayouLexApiClient _api;
    private LocalSqliteSearchService? _offlineSearch;
    private CancellationTokenSource? _searchCts;
    private CancellationTokenSource? _detailCts;
    private BayouLexInitResponse? _init;
    private bool _useOffline;
    private bool _settingsOpen;

    public ObservableCollection<SearchResultDto> Results { get; } = [];

    public MainWindow()
    {
        InitializeComponent();
        DataContext = this;
        _api = CreateApiClient();
        _searchDebounce = new DispatcherTimer { Interval = TimeSpan.FromMilliseconds(180) };
        _searchDebounce.Tick += async (_, _) =>
        {
            _searchDebounce.Stop();
            await RunSearchAsync();
        };
        LimitComboBox.ItemsSource = new[] { 25, 50, 100 };
        LimitComboBox.SelectedItem = 50;
        CategoryComboBox.ItemsSource = new[] { "All categories" };
        CategoryComboBox.SelectedIndex = 0;
    }

    private async void Window_Loaded(object sender, RoutedEventArgs e)
    {
        await RefreshApiAsync();
    }

    private BayouLexApiClient CreateApiClient()
    {
        var baseUri = ApiBaseTextBox.Text.Trim();
        if (!baseUri.EndsWith('/'))
        {
            baseUri += "/";
        }
        _http.Dispose();
        _http = new HttpClient
        {
            BaseAddress = new Uri(baseUri),
            Timeout = TimeSpan.FromSeconds(30),
        };
        return new BayouLexApiClient(_http);
    }

    private async Task RefreshApiAsync()
    {
        try
        {
            SetBusy("Connecting...");
            _api = CreateApiClient();
            _init = await _api.GetInitAsync();
            var catalog = await _api.GetCatalogAsync();
            var categories = new List<string> { "All categories" };
            if (catalog is not null)
            {
                categories.AddRange(catalog.Categories.Select(item => item.Name));
            }
            CategoryComboBox.ItemsSource = categories;
            CategoryComboBox.SelectedIndex = 0;
            _useOffline = false;
            ModeTextBlock.Text = "API mode";
            DatasetTextBlock.Text = $"Dataset {_init?.DatasetVersion ?? "unknown"}";
            SetReady("Ready");
        }
        catch (Exception ex)
        {
            DatasetTextBlock.Text = "Dataset unavailable";
            SetReady($"API unavailable: {ex.Message}");
        }
    }

    private async Task RunSearchAsync()
    {
        var query = SearchTextBox.Text.Trim();
        if (query.Length < 3)
        {
            Results.Clear();
            DetailTextBox.Text = "";
            DetailTitleTextBlock.Text = "";
            DetailUrlTextBox.Text = "";
            ResultCountTextBlock.Text = "";
            StatusTextBlock.Text = "Type at least 3 characters.";
            return;
        }

        _searchCts?.Cancel();
        _searchCts = new CancellationTokenSource();
        var token = _searchCts.Token;
        var category = CategoryComboBox.SelectedItem as string;
        if (string.Equals(category, "All categories", StringComparison.OrdinalIgnoreCase))
        {
            category = null;
        }
        var limit = LimitComboBox.SelectedItem is int selectedLimit ? selectedLimit : 50;

        try
        {
            SetBusy(_useOffline ? "Searching offline..." : "Searching API...");
            SearchResponse? response;
            if (_useOffline)
            {
                if (_offlineSearch is null)
                {
                    SetReady("Offline dataset is not loaded.");
                    return;
                }
                response = await _offlineSearch.SearchAsync(query, category, limit, token);
            }
            else
            {
                response = await _api.SearchAsync(query, category, null, limit, 0, token);
            }

            if (token.IsCancellationRequested)
            {
                return;
            }

            Results.Clear();
            foreach (var row in response?.Results ?? [])
            {
                Results.Add(row);
            }
            ResultCountTextBlock.Text = $"{Results.Count:n0}";
            SetReady($"{Results.Count} result(s)");
        }
        catch (OperationCanceledException)
        {
        }
        catch (Exception ex)
        {
            SetReady($"Search failed: {ex.Message}");
        }
    }

    private async Task LoadDetailAsync(SearchResultDto result)
    {
        _detailCts?.Cancel();
        _detailCts = new CancellationTokenSource();
        var token = _detailCts.Token;

        try
        {
            SetBusy("Loading detail...");
            DocumentDto? doc = _useOffline
                ? await (_offlineSearch?.GetDocumentAsync(result.DocumentKey, token) ?? Task.FromResult<DocumentDto?>(null))
                : await _api.GetDocumentAsync(result.DocumentKey, token);
            if (doc is null || token.IsCancellationRequested)
            {
                return;
            }

            DetailTitleTextBlock.Text = string.Join(" | ", new[] { doc.Citation, doc.Title }.Where(value => !string.IsNullOrWhiteSpace(value)));
            DetailUrlTextBox.Text = doc.Url;
            DetailTextBox.Text = doc.Text.Length <= MaxPreviewChars
                ? doc.Text
                : doc.Text[..MaxPreviewChars] + $"\n\n[... preview truncated at {MaxPreviewChars:n0} / {doc.Text.Length:n0} chars ...]";
            SetReady("Detail loaded");
        }
        catch (OperationCanceledException)
        {
        }
        catch (Exception ex)
        {
            SetReady($"Detail failed: {ex.Message}");
        }
    }

    private async void SearchTextBox_TextChanged(object sender, TextChangedEventArgs e)
    {
        _searchDebounce.Stop();
        _searchDebounce.Start();
        await Task.CompletedTask;
    }

    private async void CategoryComboBox_SelectionChanged(object sender, SelectionChangedEventArgs e)
    {
        await RunSearchAsync();
    }

    private async void LimitComboBox_SelectionChanged(object sender, SelectionChangedEventArgs e)
    {
        await RunSearchAsync();
    }

    private async void ResultsListView_SelectionChanged(object sender, SelectionChangedEventArgs e)
    {
        if (ResultsListView.SelectedItem is SearchResultDto result)
        {
            await LoadDetailAsync(result);
        }
    }

    private async void RefreshButton_Click(object sender, RoutedEventArgs e)
    {
        await RefreshApiAsync();
    }

    private async void OfflineButton_Click(object sender, RoutedEventArgs e)
    {
        try
        {
            _api = CreateApiClient();
            _init ??= await _api.GetInitAsync();
            if (_init is null)
            {
                SetReady("Cannot download offline data: API init failed.");
                return;
            }

            ProgressBar.Visibility = Visibility.Visible;
            ProgressBar.Value = 0;
            var downloader = new OfflineDownloadService(_http);
            var progress = new Progress<double>(value => ProgressBar.Value = Math.Clamp(value, 0, 100));
            var dbPath = await downloader.DownloadAsync(_init.DatasetVersion, progress, CancellationToken.None);
            _offlineSearch = new LocalSqliteSearchService(dbPath);
            _useOffline = true;
            ModeTextBlock.Text = "Offline mode";
            DatasetTextBlock.Text = $"Dataset {_init.DatasetVersion}";
            SetReady($"Offline dataset ready: {dbPath}");
            await RunSearchAsync();
        }
        catch (Exception ex)
        {
            SetReady($"Offline download failed: {ex.Message}");
        }
        finally
        {
            ProgressBar.Visibility = Visibility.Collapsed;
        }
    }

    private async void UseOfflineButton_Click(object sender, RoutedEventArgs e)
    {
        try
        {
            var dbPath = OfflineDownloadService.DefaultDatabasePath();
            _offlineSearch = new LocalSqliteSearchService(dbPath);
            _useOffline = true;
            ModeTextBlock.Text = "Offline mode";
            DatasetTextBlock.Text = "Local offline dataset";
            SetReady($"Using offline dataset: {dbPath}");
            await RunSearchAsync();
        }
        catch (Exception ex)
        {
            SetReady($"Offline dataset unavailable: {ex.Message}");
        }
    }

    private void SetBusy(string message)
    {
        StatusTextBlock.Text = message;
    }

    private void SetReady(string message)
    {
        StatusTextBlock.Text = message;
    }

    private void SettingsButton_Click(object sender, RoutedEventArgs e)
    {
        SetSettingsOpen(!_settingsOpen);
    }

    private void CloseSettingsButton_Click(object sender, RoutedEventArgs e)
    {
        SetSettingsOpen(false);
    }

    private async void ResetLocalApiButton_Click(object sender, RoutedEventArgs e)
    {
        ApiBaseTextBox.Text = "http://127.0.0.1:5087/bayoulex/v1/";
        await RefreshApiAsync();
    }

    private void SetSettingsOpen(bool open)
    {
        _settingsOpen = open;
        SettingsPanel.Visibility = open ? Visibility.Visible : Visibility.Collapsed;
        SettingsColumn.Width = open ? new GridLength(360) : new GridLength(0);
    }
}
