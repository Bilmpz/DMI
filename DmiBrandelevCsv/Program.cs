using System.Globalization;
using System.Text;
using System.Text.Json;

internal class Program
{
    private const string StationId = "06154";
    private const string StationName = "Brandelev";
    private const string ParameterHumidity = "humidity";
    private const string ParameterTempDry = "temp_dry";
    private const string BaseUrl = "https://opendataapi.dmi.dk/v2/metObs/collections/observation/items";
    private const int PageLimit = 250000;

    public static async Task Main()
    {
        var startUtc = DateTimeOffset.Parse(
            "2024-01-01T00:00:00Z",
            CultureInfo.InvariantCulture,
            DateTimeStyles.AssumeUniversal | DateTimeStyles.AdjustToUniversal);

        var endUtc = DateTimeOffset.UtcNow;

        var outputDirectory = Path.Combine(AppContext.BaseDirectory, "output");
        Directory.CreateDirectory(outputDirectory);

        var outputFile = Path.Combine(
            outputDirectory,
            $"brandelev_{StationId}_{startUtc:yyyy-MM-dd}_to_{endUtc:yyyy-MM-dd}.csv");

        var rows = new SortedDictionary<DateTimeOffset, WeatherRow>();

        using var httpClient = new HttpClient();
        httpClient.DefaultRequestHeaders.UserAgent.ParseAdd("DmiBrandelevCsv/1.0");

        Console.WriteLine($"Henter DMI-data for {StationName} ({StationId})");
        Console.WriteLine($"Fra: {startUtc:yyyy-MM-dd HH:mm:ss} UTC");
        Console.WriteLine($"Til: {endUtc:yyyy-MM-dd HH:mm:ss} UTC");
        Console.WriteLine();

        await FetchParameterIntoRowsAsync(httpClient, ParameterHumidity, startUtc, endUtc, rows);
        await FetchParameterIntoRowsAsync(httpClient, ParameterTempDry, startUtc, endUtc, rows);

        WriteCsv(outputFile, rows.Values);
        CopyLatestCsvToDocs(outputFile);

        Console.WriteLine();
        Console.WriteLine("Færdig.");
        Console.WriteLine($"CSV-fil gemt her:");
        Console.WriteLine(outputFile);
        Console.WriteLine($"Antal rækker: {rows.Count}");
    }

    private static async Task FetchParameterIntoRowsAsync(
        HttpClient httpClient,
        string parameterId,
        DateTimeOffset startUtc,
        DateTimeOffset endUtc,
        SortedDictionary<DateTimeOffset, WeatherRow> rows)
    {
        Console.WriteLine($"Henter parameter: {parameterId}");

        foreach (var (chunkStartUtc, chunkEndUtc) in SplitIntoMonthlyRanges(startUtc, endUtc))
        {
            string? nextUrl = BuildUrl(StationId, parameterId, chunkStartUtc, chunkEndUtc, PageLimit);

            while (!string.IsNullOrWhiteSpace(nextUrl))
            {
                using var response = await httpClient.GetAsync(nextUrl);
                var responseText = await response.Content.ReadAsStringAsync();

                if (!response.IsSuccessStatusCode)
                {
                    Console.WriteLine();
                    Console.WriteLine("DMI-kald fejlede.");
                    Console.WriteLine($"Parameter: {parameterId}");
                    Console.WriteLine($"URL: {nextUrl}");
                    Console.WriteLine($"HTTP status: {(int)response.StatusCode} {response.StatusCode}");
                    Console.WriteLine("Svar fra DMI:");
                    Console.WriteLine(responseText);

                    throw new InvalidOperationException("DMI API returnerede en fejl.");
                }

                using var document = JsonDocument.Parse(responseText);

                if (document.RootElement.TryGetProperty("features", out var features) &&
                    features.ValueKind == JsonValueKind.Array)
                {
                    foreach (var feature in features.EnumerateArray())
                    {
                        if (!feature.TryGetProperty("properties", out var properties))
                            continue;

                        if (!properties.TryGetProperty("observed", out var observedElement) ||
                            observedElement.ValueKind != JsonValueKind.String)
                            continue;

                        if (!properties.TryGetProperty("value", out var valueElement) ||
                            valueElement.ValueKind != JsonValueKind.Number)
                            continue;

                        var observedText = observedElement.GetString();
                        if (string.IsNullOrWhiteSpace(observedText))
                            continue;

                        if (!DateTimeOffset.TryParse(
                                observedText,
                                CultureInfo.InvariantCulture,
                                DateTimeStyles.AssumeUniversal | DateTimeStyles.AdjustToUniversal,
                                out var observedUtc))
                        {
                            continue;
                        }

                        var value = valueElement.GetDouble();

                        if (!rows.TryGetValue(observedUtc, out var row))
                        {
                            row = new WeatherRow(observedUtc);
                            rows.Add(observedUtc, row);
                        }

                        if (parameterId == ParameterHumidity)
                        {
                            row.Humidity = value;
                        }
                        else if (parameterId == ParameterTempDry)
                        {
                            row.TempDry = value;
                        }
                    }
                }

                nextUrl = GetNextLink(document.RootElement);
            }

            Console.WriteLine($"  OK måned: {chunkStartUtc:yyyy-MM-dd} -> {chunkEndUtc:yyyy-MM-dd}");
        }
    }

    private static IEnumerable<(DateTimeOffset StartUtc, DateTimeOffset EndUtc)> SplitIntoMonthlyRanges(
        DateTimeOffset startUtc,
        DateTimeOffset endUtc)
    {
        if (startUtc > endUtc)
            yield break;

        var currentStart = startUtc;

        while (currentStart <= endUtc)
        {
            var firstOfCurrentMonth = new DateTimeOffset(
                currentStart.Year,
                currentStart.Month,
                1,
                0,
                0,
                0,
                TimeSpan.Zero);

            var firstOfNextMonth = firstOfCurrentMonth.AddMonths(1);
            var currentEnd = firstOfNextMonth.AddSeconds(-1);

            if (currentEnd > endUtc)
                currentEnd = endUtc;

            yield return (currentStart, currentEnd);

            currentStart = currentEnd.AddSeconds(1);
        }
    }

    private static string BuildUrl(
        string stationId,
        string parameterId,
        DateTimeOffset startUtc,
        DateTimeOffset endUtc,
        int limit)
    {
        var startText = startUtc.ToString("yyyy-MM-dd'T'HH':'mm':'ss'Z'", CultureInfo.InvariantCulture);
        var endText = endUtc.ToString("yyyy-MM-dd'T'HH':'mm':'ss'Z'", CultureInfo.InvariantCulture);
        var datetimeRange = $"{startText}/{endText}";

        return $"{BaseUrl}" +
               $"?stationId={Uri.EscapeDataString(stationId)}" +
               $"&parameterId={Uri.EscapeDataString(parameterId)}" +
               $"&datetime={Uri.EscapeDataString(datetimeRange)}" +
               $"&limit={limit}";
    }

    private static string? GetNextLink(JsonElement root)
    {
        if (!root.TryGetProperty("links", out var links) || links.ValueKind != JsonValueKind.Array)
            return null;

        foreach (var link in links.EnumerateArray())
        {
            if (!link.TryGetProperty("rel", out var relElement) ||
                relElement.ValueKind != JsonValueKind.String)
                continue;

            var rel = relElement.GetString();
            if (!string.Equals(rel, "next", StringComparison.OrdinalIgnoreCase))
                continue;

            if (!link.TryGetProperty("href", out var hrefElement) ||
                hrefElement.ValueKind != JsonValueKind.String)
                continue;

            return hrefElement.GetString();
        }

        return null;
    }

  private static void WriteCsv(string filePath, IEnumerable<WeatherRow> rows)
{
    var danishCulture = CultureInfo.GetCultureInfo("da-DK");

    using var writer = new StreamWriter(
        filePath,
        false,
        new UTF8Encoding(encoderShouldEmitUTF8Identifier: false));

    writer.WriteLine("ObservedUtc;HumidityPct;TempDryC");

    foreach (var row in rows)
    {
        if (!ShouldIncludeRow(row.ObservedUtc))
            continue;

        var observedText = row.ObservedUtc.ToString("yyyy-MM-dd HH:mm:ss", CultureInfo.InvariantCulture);
        var humidityText = row.Humidity?.ToString(danishCulture) ?? "";
        var tempDryText = row.TempDry?.ToString(danishCulture) ?? "";

        writer.WriteLine($"{observedText};{humidityText};{tempDryText}");
    }
}

    private static void CopyLatestCsvToDocs(string outputFile)
    {
        var repoRoot = Path.GetFullPath(Path.Combine(
            AppContext.BaseDirectory, "..", "..", "..", ".."));

        var docsDataDirectory = Path.Combine(repoRoot, "docs", "data");
        Directory.CreateDirectory(docsDataDirectory);

        var latestCsvFile = Path.Combine(docsDataDirectory, "latest.csv");
        File.Copy(outputFile, latestCsvFile, overwrite: true);

        Console.WriteLine($"Seneste CSV kopieret til:");
        Console.WriteLine(latestCsvFile);
    }
    
    private static bool ShouldIncludeRow(DateTimeOffset observedUtc)
{
    return observedUtc.Second == 0 &&
           (observedUtc.Minute == 0 || observedUtc.Minute == 30);
}
}

internal sealed class WeatherRow
{
    public WeatherRow(DateTimeOffset observedUtc)
    {
        ObservedUtc = observedUtc;
    }

    public DateTimeOffset ObservedUtc { get; }
    public double? Humidity { get; set; }
    public double? TempDry { get; set; }
}