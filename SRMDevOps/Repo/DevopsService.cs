using Microsoft.EntityFrameworkCore;
using SRMDevOps.Controllers;
using SRMDevOps.DataAccess;
using SRMDevOps.Dto;
using System.Net.Http.Headers;
using System.Text;
using System.Text.Json;

namespace SRMDevOps.Repo
{
    public class DevopsService : IADO
    {
        private readonly IConfiguration _configuration;
        private readonly string _baseUrl = "https://dev.azure.com/Indusvalleypartners";
        private readonly IvpadodashboardContext _context;

        public DevopsService(IConfiguration configuration,IvpadodashboardContext context)
        {
            _configuration = configuration;
            _context = context;
        }


        private AuthenticationHeaderValue GetAuthHeader()
        {
            var pat = _configuration["AzureDevOps:PAT"];
            var credentials = Convert.ToBase64String(Encoding.ASCII.GetBytes($":{pat}"));
            return new AuthenticationHeaderValue("Basic", credentials);
        }

        public async Task<List<TeamDto>> GetTeamsByProjectIdAsync(string projectId)
        {
            // Retrieve PAT from configuration securely
            var pat = _configuration["AzureDevOps:PAT"];
            var credentials = Convert.ToBase64String(Encoding.ASCII.GetBytes($":{pat}"));

            using (HttpClient client = new HttpClient())
            {
                client.DefaultRequestHeaders.Authorization = new AuthenticationHeaderValue("Basic", credentials);
                client.DefaultRequestHeaders.Accept.Add(new MediaTypeWithQualityHeaderValue("application/json"));

                // ID-based URL: {projectId} is now a GUID
                string url = $"https://dev.azure.com/Indusvalleypartners/_apis/projects/{projectId}/teams?api-version=7.1";

                var response = await client.GetAsync(url);

                if (response.IsSuccessStatusCode)
                {
                    var rawJson = await response.Content.ReadAsStringAsync();

                    // Standard ADO response wrapper containing the 'value' array
                    var options = new JsonSerializerOptions { PropertyNameCaseInsensitive = true };
                    var result = JsonSerializer.Deserialize<AzureDevOpsResponse<TeamDto>>(rawJson, options);

                    return result?.Value ?? new List<TeamDto>();
                }

                throw new Exception($"ADO API Error: {response.StatusCode} for Project ID: {projectId}");
            }
        }

        // 1. Fetch exact Area Paths owned by a team
        public async Task<List<string>> GetTeamAreaPathsAsync(string projectId, string teamId)
        {
            using var client = new HttpClient();
            client.DefaultRequestHeaders.Authorization = GetAuthHeader();

            string url = $"{_baseUrl}/{projectId}/{teamId}/_apis/work/teamsettings/teamfieldvalues?api-version=7.1";

            var response = await client.GetAsync(url);
            if (response.IsSuccessStatusCode)
            {
                var data = await response.Content.ReadFromJsonAsync<JsonElement>();
                return data.GetProperty("values")
                           .EnumerateArray()
                           .Select(v => v.GetProperty("value").GetString())
                           .ToList();
            }
            return new List<string>();
        }

        // 2. Fetch official Sprint Dates from ADO
        public async Task<List<SprintDto>> GetRecentSprintsAsync(string projectId, string teamId, int lastNSprints)
        {
            using var client = new HttpClient();
            client.DefaultRequestHeaders.Authorization = GetAuthHeader();

            string url = $"{_baseUrl}/{projectId}/{teamId}/_apis/work/teamsettings/iterations?api-version=7.1";
            var response = await client.GetAsync(url);

            if (!response.IsSuccessStatusCode) return new List<SprintDto>();

            var data = await response.Content.ReadFromJsonAsync<AzureDevOpsResponse<SprintDto>>(new JsonSerializerOptions { PropertyNameCaseInsensitive = true });

            return data.Value
                .Where(s => s.Attributes.StartDate.HasValue && s.Attributes.FinishDate.HasValue)
                .OrderByDescending(s => s.Attributes.StartDate)
                .Take(lastNSprints)
                .ToList();
        }

        // 3. Combined method to prepare data for your DB query
        public async Task<List<SprintProgressDto>> GetSprintDataByAreaPathAsync(
            string projectId,
            string teamId,
            int lastNSprints)
        {
            // First, get the "Source of Truth" from ADO API
            var teamPaths = await GetTeamAreaPathsAsync(projectId, teamId);
            var sprints = await GetRecentSprintsAsync(projectId, teamId, lastNSprints);

            // This list will be passed to your SpillageService or DB Repository
            // For now, returning the Sprint list to be processed by your DB logic
            var results = new List<SprintProgressDto>();

            foreach (var sprint in sprints)
            {
                // Here is where you integrate the DB calculation we discussed
                // results.Add(await _spillageService.CalculateFromDb(teamPaths, sprint.Attributes.StartDate, sprint.Attributes.FinishDate));
            }

            return results;
        }

        private static (string unit, int bucketMonths, int defaultN) NormalizePeriodUnit(string? unit)
        {
            var u = unit?.Trim().ToLowerInvariant();
            return u switch
            {
                "quarter" or "quarterly" => ("quarterly", 3, 4),
                "year" or "yearly" => ("yearly", 12, 1),
                _ => ("monthly", 1, 6)
            };
        }

        private static DateTime ComputeWindowStart(string unit, int nPeriods)
        {
            var now = DateTime.Now;
            var bucketMonths = unit == "quarterly" ? 3 : unit == "yearly" ? 12 : 1;
            return new DateTime(now.Year, now.Month, 1).AddMonths(-((nPeriods * bucketMonths) - 1));
        }

        // Helper to perform the DB calculation based on Sprint End Date
        private async Task<(double Assigned, double Completed)> GetSprintDataFromDbAsync(string iterationPath, string areaPath, DateTime sprintEnd)
        {
            var doneStates = new[] { "Closed", "Done", "Verified", "Completed", "Live" };

            var dbStories = await _context.IvpUserStoryIterations
                .Join(_context.IvpUserStoryDetails,
                    usi => usi.UserStoryId,
                    usd => usd.UserStoryId,
                    (usi, usd) => new { usi, usd })
                .Where(c => c.usi.IterationPath == iterationPath && c.usd.AreaPath == areaPath)
                .Select(x => new { x.usd.StoryPoints, x.usd.ClosedDate, x.usd.State })
                .ToListAsync();

            double assigned = dbStories.Sum(x => x.StoryPoints ?? 0);
            double completed = dbStories
                .Where(x => !string.IsNullOrEmpty(x.State) &&
                            doneStates.Contains(x.State, StringComparer.OrdinalIgnoreCase) &&
                            x.ClosedDate.HasValue && x.ClosedDate.Value.Date <= sprintEnd.Date)
                .Sum(x => x.StoryPoints ?? 0);

            return (assigned, completed);
        }

        // --- RESTORED ADO FUNCTIONALITIES ---

        public async Task<List<SprintDto>> GetRecentSprintsAsync(string projectId, string teamId)
        {
            using var client = new HttpClient();
            client.DefaultRequestHeaders.Authorization = GetAuthHeader();

            string url = $"{_baseUrl}/{projectId}/{teamId}/_apis/work/teamsettings/iterations?api-version=7.1";
            var response = await client.GetAsync(url);

            if (!response.IsSuccessStatusCode) return new List<SprintDto>();

            var data = await response.Content.ReadFromJsonAsync<AzureDevOpsResponse<SprintDto>>(new JsonSerializerOptions { PropertyNameCaseInsensitive = true });
            return data?.Value ?? new List<SprintDto>();
        }


        public async Task<CombinedSprintDataDto> GetAggregatedTeamStatsAsync(
    string projectId,
    string teamId,
    string? timeframe,
    int n)
        {
            // 1. Setup timeframe boundaries (Monthly/Quarterly/Yearly)
            var (unit, bucketMonths, defaultN) = NormalizePeriodUnit(timeframe);
            var periods = n > 0 ? n : defaultN;
            var windowStart = ComputeWindowStart(unit, periods);

            // 2. Fetch Team Metadata from ADO
            var teamAreaPaths = await GetTeamAreaPathsAsync(projectId, teamId);
            var allSprints = await GetRecentSprintsAsync(projectId, teamId);

            var result = new CombinedSprintDataDto
            {
                Stats = new List<SprintProgressDto>(),
                Spillage = new List<SpillageTrendDto>()
            };

            // 3. Process each period bucket
            for (int p = 0; p < periods; p++)
            {
                var periodStart = windowStart.AddMonths(p * bucketMonths);
                var periodEnd = periodStart.AddMonths(bucketMonths);

                // Find sprints that fall into this timeframe bucket
                var sprintsInBucket = allSprints
                    .Where(s => s.Attributes.StartDate >= periodStart && s.Attributes.StartDate < periodEnd)
                    .ToList();

                double bucketAssigned = 0;
                double bucketCompleted = 0;

                // 4. AGGREGATE: Loop through every Area Path and every Sprint in this bucket
                foreach (var areaPath in teamAreaPaths)
                {
                    foreach (var sprint in sprintsInBucket)
                    {
                        if (sprint.Attributes.FinishDate.HasValue)
                        {
                            // Use the helper to query your DB
                            var dbData = await GetSprintDataFromDbAsync(sprint.Path, areaPath, sprint.Attributes.FinishDate.Value);
                            bucketAssigned += dbData.Assigned;
                            bucketCompleted += dbData.Completed;
                        }
                    }
                }

                // 5. Generate Label (e.g., "Jan 2026")
                string label = unit switch
                {
                    "quarterly" => $"Q{((periodStart.Month - 1) / 3) + 1} {periodStart:yyyy}",
                    "yearly" => periodStart.ToString("yyyy"),
                    _ => periodStart.ToString("MMM yyyy")
                };

                result.Stats.Add(new SprintProgressDto { IterationPath = label, TotalPointsAssigned = bucketAssigned, TotalPointsCompleted = bucketCompleted, SortDate = periodStart });
                result.Spillage.Add(new SpillageTrendDto { IterationPath = label, SpillagePoints = bucketAssigned - bucketCompleted, SortDate = periodStart });
            }

            return result;
        }
    }

    public class AzureDevOpsResponse<T>
    {
        public int Count { get; set; }
        public List<T> Value { get; set; }
    }

    public class SprintDto
    {
        public string Name { get; set; }
        public string Path { get; set; }
        public Attributes Attributes { get; set; }
    }

    public class Attributes
    {
        public DateTime? StartDate { get; set; }
        public DateTime? FinishDate { get; set; }
    }

    public class CombinedSprintDataDto
    {
        public List<SprintProgressDto> Stats { get; set; }
        public List<SpillageTrendDto> Spillage { get; set; }
    }
}