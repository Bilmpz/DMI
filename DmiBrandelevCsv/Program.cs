using System.Globalization;
using System.Text;
using System.Text.Json;

internal class Program
{
    private const string ParameterHumidity = "humidity";
    private const string ParameterTempDry = "temp_dry";
    private const string ObservationBaseUrl = "https://opendataapi.dmi.dk/v2/metObs/collections/observation/items";
    private const string StationsUrl = "https://opendataapi.dmi.dk/v2/metObs/collections/station/items?limit=10000";
    private const int PageLimit = 300000;

    public static async Task<int> Main(string[] args)
    {
        using var httpClient = new HttpClient();
        httpClient.DefaultRequestHeaders.UserAgent.ParseAdd("DmiBrandelevCsv/1.0");

        if (args.Length >= 2 && args[0] == "--station")
        {
            return await RunStationCsvModeAsync(httpClient, args[1]);
        }

        return await RunStationsListModeAsync(httpClient);
    }

    // Default mode: bygger docs/data/stations.json fra DMI's stationsendpoint.
    private static async Task<int> RunStationsListModeAsync(HttpClient httpClient)
    {
        Console.WriteLine("Henter stationsliste fra DMI ...");
        Console.WriteLine($"URL: {StationsUrl}");

        using var response = await httpClient.GetAsync(StationsUrl);
        var responseText = await response.Content.ReadAsStringAsync();

        if (!response.IsSuccessStatusCode)
        {
            Console.WriteLine();
            Console.WriteLine("DMI-kald fejlede.");
            Console.WriteLine($"HTTP status: {(int)response.StatusCode} {response.StatusCode}");
            Console.WriteLine(responseText);
            return 1;
        }

        using var document = JsonDocument.Parse(responseText);

        if (!document.RootElement.TryGetProperty("features", out var features) ||
            features.ValueKind != JsonValueKind.Array)
        {
            Console.WriteLine("Uventet JSON-svar (ingen features).");
            return 1;
        }

        var stations = new List<StationEntry>();

        foreach (var feature in features.EnumerateArray())
        {
            if (!feature.TryGetProperty("properties", out var properties))
                continue;

            if (!properties.TryGetProperty("status", out var statusElement) ||
                statusElement.ValueKind != JsonValueKind.String)
                continue;

            if (!string.Equals(statusElement.GetString(), "Active", StringComparison.OrdinalIgnoreCase))
                continue;

            if (!properties.TryGetProperty("parameterId", out var parameterIdElement) ||
                parameterIdElement.ValueKind != JsonValueKind.Array)
                continue;

            bool hasTempDry = false;
            bool hasHumidity = false;
            foreach (var p in parameterIdElement.EnumerateArray())
            {
                if (p.ValueKind != JsonValueKind.String) continue;
                var pid = p.GetString();
                if (string.Equals(pid, ParameterTempDry, StringComparison.OrdinalIgnoreCase)) hasTempDry = true;
                if (string.Equals(pid, ParameterHumidity, StringComparison.OrdinalIgnoreCase)) hasHumidity = true;
            }
            if (!hasTempDry || !hasHumidity) continue;

            if (!properties.TryGetProperty("stationId", out var stationIdElement) ||
                stationIdElement.ValueKind != JsonValueKind.String)
                continue;

            var stationId = stationIdElement.GetString();
            if (string.IsNullOrWhiteSpace(stationId)) continue;

            string name = stationId!;
            if (properties.TryGetProperty("name", out var nameElement) &&
                nameElement.ValueKind == JsonValueKind.String)
            {
                var n = nameElement.GetString();
                if (!string.IsNullOrWhiteSpace(n)) name = n!;
            }

            double? lon = null;
            double? lat = null;
            if (feature.TryGetProperty("geometry", out var geometry) &&
                geometry.TryGetProperty("coordinates", out var coords) &&
                coords.ValueKind == JsonValueKind.Array)
            {
                var arr = coords.EnumerateArray().ToArray();
                if (arr.Length >= 2 &&
                    arr[0].ValueKind == JsonValueKind.Number &&
                    arr[1].ValueKind == JsonValueKind.Number)
                {
                    lon = arr[0].GetDouble();
                    lat = arr[1].GetDouble();
                }
            }
            if (lon == null || lat == null) continue;

            stations.Add(new StationEntry
            {
                Id = stationId!,
                Name = name,
                Lat = Math.Round(lat.Value, 4),
                Lon = Math.Round(lon.Value, 4)
            });
        }

        var unique = stations
            .GroupBy(s => s.Id)
            .Select(g => g.First())
            .OrderBy(s => s.Name, StringComparer.Create(CultureInfo.GetCultureInfo("da-DK"), false))
            .ToList();

        Console.WriteLine($"Fundet {unique.Count} aktive stationer med både temp_dry og humidity.");

        var docsDataDirectory = ResolveDocsDataDirectory();
        Directory.CreateDirectory(docsDataDirectory);
        var stationsFile = Path.Combine(docsDataDirectory, "stations.json");

        var jsonOptions = new JsonSerializerOptions
        {
            WriteIndented = true,
            PropertyNamingPolicy = JsonNamingPolicy.CamelCase
        };

        await File.WriteAllTextAsync(
            stationsFile,
            JsonSerializer.Serialize(unique, jsonOptions),
            new UTF8Encoding(encoderShouldEmitUTF8Identifier: false));

        Console.WriteLine($"Skrevet: {stationsFile}");
        return 0;
    }

    // Valgfri mode: dotnet run -- --station <id>  (genererer CSV for én station).
    private static async Task<int> RunStationCsvModeAsync(HttpClient httpClient, string stationId)
    {
        var startUtc = DateTimeOffset.Parse(
            "2022-10-01T00:00:00Z",
            CultureInfo.InvariantCulture,
            DateTimeStyles.AssumeUniversal | DateTimeStyles.AdjustToUniversal);

        var endUtc = DateTimeOffset.UtcNow;

        var outputDirectory = Path.Combine(AppContext.BaseDirectory, "output");
        Directory.CreateDirectory(outputDirectory);

        var outputFile = Path.Combine(
            outputDirectory,
            $"station_{stationId}_{startUtc:yyyy-MM-dd}_to_{endUtc:yyyy-MM-dd}.csv");

        var rows = new SortedDictionary<DateTimeOffset, WeatherRow>();

        Console.WriteLine($"Henter DMI-data for station {stationId}");
        Console.WriteLine($"Fra: {startUtc:yyyy-MM-dd HH:mm:ss} UTC");
        Console.WriteLine($"Til: {endUtc:yyyy-MM-dd HH:mm:ss} UTC");
        Console.WriteLine();

        await FetchParameterIntoRowsAsync(httpClient, stationId, ParameterHumidity, startUtc, endUtc, rows);
        await FetchParameterIntoRowsAsync(httpClient, stationId, ParameterTempDry, startUtc, endUtc, rows);

        WriteCsv(outputFile, rows.Values);

        Console.WriteLine();
        Console.WriteLine("Færdig.");
        Console.WriteLine($"CSV-fil gemt her:");
        Console.WriteLine(outputFile);
        Console.WriteLine($"Antal rækker: {rows.Count}");
        return 0;
    }

    private static async Task FetchParameterIntoRowsAsync(
        HttpClient httpClient,
        string stationId,
        string parameterId,
        DateTimeOffset startUtc,
        DateTimeOffset endUtc,
        SortedDictionary<DateTimeOffset, WeatherRow> rows)
    {
        Console.WriteLine($"Henter parameter: {parameterId}");

        foreach (var (chunkStartUtc, chunkEndUtc) in SplitIntoMonthlyRanges(startUtc, endUtc))
        {
            string? nextUrl = BuildObservationUrl(stationId, parameterId, chunkStartUtc, chunkEndUtc, PageLimit);
            int pageCount = 0;
            int rowsInChunk = 0;

            while (!string.IsNullOrWhiteSpace(nextUrl))
            {
                pageCount++;
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
                            row.Humidity = value;
                        else if (parameterId == ParameterTempDry)
                            row.TempDry = value;

                        rowsInChunk++;
                    }
                }

                nextUrl = GetNextLink(document.RootElement);

                if (nextUrl != null)
                    Console.WriteLine($"  Side {pageCount} hentet, henter næste side...");
            }

            Console.WriteLine($"  OK: {chunkStartUtc:yyyy-MM-dd} -> {chunkEndUtc:yyyy-MM-dd} ({rowsInChunk} observationer)");
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
                1, 0, 0, 0, TimeSpan.Zero);

            var firstOfNextMonth = firstOfCurrentMonth.AddMonths(1);
            var currentEnd = firstOfNextMonth.AddSeconds(-1);

            if (currentEnd > endUtc)
                currentEnd = endUtc;

            yield return (currentStart, currentEnd);

            currentStart = currentEnd.AddSeconds(1);
        }
    }

    private static string BuildObservationUrl(
        string stationId,
        string parameterId,
        DateTimeOffset startUtc,
        DateTimeOffset endUtc,
        int limit)
    {
        var startText = startUtc.ToString("yyyy-MM-dd'T'HH':'mm':'ss'Z'", CultureInfo.InvariantCulture);
        var endText = endUtc.ToString("yyyy-MM-dd'T'HH':'mm':'ss'Z'", CultureInfo.InvariantCulture);
        var datetimeRange = $"{startText}/{endText}";

        return $"{ObservationBaseUrl}" +
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

    private static string ResolveDocsDataDirectory()
    {
        var candidate = Path.GetFullPath(Path.Combine(
            AppContext.BaseDirectory, "..", "..", "..", ".."));

        // Find nærmeste mappe der indeholder en "docs"-mappe.
        var dir = new DirectoryInfo(candidate);
        for (int i = 0; i < 6 && dir != null; i++)
        {
            var docs = Path.Combine(dir.FullName, "docs");
            if (Directory.Exists(docs))
                return Path.Combine(docs, "data");
            dir = dir.Parent;
        }

        return Path.Combine(candidate, "docs", "data");
    }

    private static bool ShouldIncludeRow(DateTimeOffset observedUtc)
    {
        return observedUtc.Second == 0 && observedUtc.Minute == 0;
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

internal sealed class StationEntry
{
    public string Id { get; set; } = "";
    public string Name { get; set; } = "";
    public double Lat { get; set; }
    public double Lon { get; set; }
}
