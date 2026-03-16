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
        private readonly IvpadodashboardContext _context;

        public DevopsService(IConfiguration configuration, IvpadodashboardContext context)
        {
            _configuration = configuration;
            _context = context;
        }

        public async Task<string> GetTeamsInProject(string projectId)
        {
            // Retrieve PAT from configuration securely
            var pat = _configuration["AzureDevOps:PAT"];
            var credentials = Convert.ToBase64String(Encoding.ASCII.GetBytes($":{pat}"));

            using (HttpClient client = new HttpClient())
            {
                client.DefaultRequestHeaders.Accept.Add(
                    new MediaTypeWithQualityHeaderValue("application/json"));

                client.DefaultRequestHeaders.Authorization = new AuthenticationHeaderValue("Basic", credentials);

                // URL to fetch all teams for a specific project
                string url = $"https://dev.azure.com/Indusvalleypartners/_apis/projects/{projectId}/teams?api-version=7.1";

                using (HttpResponseMessage response = await client.GetAsync(url))
                {
                    if (response.IsSuccessStatusCode)
                    {
                        return await response.Content.ReadAsStringAsync();
                    }
                    throw new Exception($"Failed to fetch teams. Status: {response.StatusCode}");
                }
            }
        }

        public async Task<TeamFieldValuesDto> GetTeamAreaPaths(string projectId, string teamId)
        {
            var pat = _configuration["AzureDevOps:PAT"];
            var credentials = Convert.ToBase64String(Encoding.ASCII.GetBytes($":{pat}"));

            using (HttpClient client = new HttpClient())
            {
                client.DefaultRequestHeaders.Authorization = new AuthenticationHeaderValue("Basic", credentials);

                // API call to get Team Field Values (which are Area Paths by default)
                string url = $"https://dev.azure.com/Indusvalleypartners/{projectId}/{teamId}/_apis/work/teamsettings/teamfieldvalues?api-version=7.1";

                var response = await client.GetAsync(url);
                if (response.IsSuccessStatusCode)
                {
                    var rawJson = await response.Content.ReadAsStringAsync();
                    var options = new JsonSerializerOptions { PropertyNameCaseInsensitive = true };
                    return JsonSerializer.Deserialize<TeamFieldValuesDto>(rawJson, options);
                }
                throw new Exception($"Error: {response.StatusCode}");
            }
        }

        //public async Task<SprintProgressDto> GetStatsForSpecificAreaPath(
        //    string projectId,
        //    string iterationPath,
        //    string selectedAreaPath)
        //{
        //    var pat = _configuration["AzureDevOps:PAT"];
        //    var credentials = Convert.ToBase64String(Encoding.ASCII.GetBytes($":{pat}"));

        //    using (HttpClient client = new HttpClient())
        //    {
        //        client.DefaultRequestHeaders.Authorization = new AuthenticationHeaderValue("Basic", credentials);

        //        // WIQL Query: Filter by Area Path AND Iteration Path
        //        var query = new
        //        {
        //            query = $@"SELECT [System.Id], [Microsoft.VSTS.Scheduling.StoryPoints], [System.State] 
        //               FROM WorkItems 
        //               WHERE [System.TeamProject] = '{projectId}' 
        //               AND [System.WorkItemType] = 'User Story'
        //               AND [System.IterationPath] = '{iterationPath}'
        //               AND [System.AreaPath] UNDER '{selectedAreaPath}'"
        //        };

        //        string url = $"https://dev.azure.com/Indusvalleypartners/{projectId}/_apis/wit/wiql?api-version=7.1";

        //        var response = await client.PostAsJsonAsync(url, query);
        //        if (response.IsSuccessStatusCode)
        //        {
        //            // This returns a list of Work Item IDs. 
        //            // You then need to fetch the 'Story Points' details for these IDs in a batch.
        //            var result = await response.Content.ReadAsStringAsync();
        //            return ParseAndAggregate(result); // Helper to sum points
        //        }
        //        throw new Exception("WIQL Query Failed");
        //    }
        //}

        public async Task<CombinedSprintDataDto> GetSprintAndSpillageDataAsync(
        string projectId, // Incoming GUID
        string teamId,    // Incoming GUID
        string selectedAreaPath,
        int lastNSprints)
        {
            var pat = _configuration["AzureDevOps:PAT"];
            var credentials = Convert.ToBase64String(Encoding.ASCII.GetBytes($":{pat}"));

            var response = new CombinedSprintDataDto();

            using var client = new HttpClient();
            client.DefaultRequestHeaders.Authorization = new AuthenticationHeaderValue("Basic", credentials);

            // 1. Fetch Sprint Metadata from ADO using IDs
            string iterationsUrl = $"https://dev.azure.com/Indusvalleypartners/{projectId}/{teamId}/_apis/work/teamsettings/iterations?api-version=7.1";
            var iterationsResponse = await client.GetAsync(iterationsUrl);

            if (!iterationsResponse.IsSuccessStatusCode) return response;

            var iterationsRaw = await iterationsResponse.Content.ReadAsStringAsync();
            var allSprints = JsonSerializer.Deserialize<AzureDevOpsResponse<SprintDto>>(iterationsRaw, new JsonSerializerOptions { PropertyNameCaseInsensitive = true });

            // Filter and take last N
            var recentSprints = allSprints.Value
                .Where(s => s.Attributes.StartDate.HasValue && s.Attributes.FinishDate.HasValue)
                .OrderByDescending(s => s.Attributes.StartDate)
                .Take(lastNSprints)
                .ToList();

            foreach (var sprint in recentSprints)
            {
                DateTime sprintStart = sprint.Attributes.StartDate.Value;
                DateTime sprintEnd = sprint.Attributes.FinishDate.Value;
                string iterationPath = sprint.Path;

                // 2. Database LINQ Query
                var dbStories = await _context.IvpUserStoryIterations
                    .Join(_context.IvpUserStoryDetails,
                        usi => usi.UserStoryId,
                        usd => usd.UserStoryId,
                        (usi, usd) => new { usi, usd })
                    .Where(c => c.usi.IterationPath == iterationPath && c.usd.AreaPath == selectedAreaPath)
                    .Select(x => new { x.usd.StoryPoints, x.usd.ClosedDate, x.usd.State })
                    .ToListAsync();

                double totalAssigned = dbStories.Sum(x => x.StoryPoints ?? 0);
                var doneStates = new[] { "Closed", "Done", "Verified", "Completed", "Live" };

                // Logic: Done state AND Closed on or before the Sprint End Date
                double completedOnTime = dbStories
                    .Where(x => !string.IsNullOrEmpty(x.State) &&
                                doneStates.Contains(x.State, StringComparer.OrdinalIgnoreCase) &&
                                x.ClosedDate.HasValue && x.ClosedDate.Value.Date <= sprintEnd.Date)
                    .Sum(x => x.StoryPoints ?? 0);

                // 3. Populate both Stats and Spillage lists
                response.Stats.Add(new SprintProgressDto
                {
                    IterationPath = sprint.Name,
                    TotalPointsAssigned = totalAssigned,
                    TotalPointsCompleted = completedOnTime,
                    SortDate = sprintStart
                });

                response.Spillage.Add(new SpillageTrendDto
                {
                    IterationPath = sprint.Name,
                    SpillagePoints = totalAssigned - completedOnTime, // Items that leaked out of the sprint
                    SortDate = sprintStart
                });
            }

            // Sort ascending for the frontend charts
            response.Stats = response.Stats.OrderBy(r => r.SortDate).ToList();
            response.Spillage = response.Spillage.OrderBy(r => r.SortDate).ToList();

            return response;
        }

        public async Task<CombinedSprintDataDto> GetSprintStatsByTimeframeAsync(
        string projectId,
        string teamId,
        string selectedAreaPath,
        string timeframe,
        int? n)
        {
            // 1. Normalize period and compute window (Mirroring SpillageService logic)
            var (unit, bucketMonths, defaultN) = NormalizePeriodUnit(timeframe);
            var periods = n ?? defaultN;
            var windowStart = ComputeWindowStart(unit, periods);

            var finalStats = new List<SprintProgressDto>();
            var finalSpillage = new List<SpillageTrendDto>();

            // 2. Fetch ALL Iterations from ADO to find which ones fall in our timeframe
            var pat = _configuration["AzureDevOps:PAT"];
            var credentials = Convert.ToBase64String(Encoding.ASCII.GetBytes($":{pat}"));

            var response = new CombinedSprintDataDto();

            using var client = new HttpClient();
            client.DefaultRequestHeaders.Authorization = new AuthenticationHeaderValue("Basic", credentials);

            // 1. Fetch Sprint Metadata from ADO using IDs
            string iterationsUrl = $"https://dev.azure.com/Indusvalleypartners/{projectId}/{teamId}/_apis/work/teamsettings/iterations?api-version=7.1";
            var iterationsResponse = await client.GetAsync(iterationsUrl);

            if (!iterationsResponse.IsSuccessStatusCode) return response;

            var iterationsRaw = await iterationsResponse.Content.ReadAsStringAsync();
            var allSprints = JsonSerializer.Deserialize<AzureDevOpsResponse<SprintDto>>(iterationsRaw, new JsonSerializerOptions { PropertyNameCaseInsensitive = true });

            // 3. Process each period bucket (Jan, Feb, etc.)
            for (int p = 0; p < periods; p++)
            {
                var periodStart = windowStart.AddMonths(p * bucketMonths);
                var periodEnd = periodStart.AddMonths(bucketMonths);

                // Identify iterations that START within this bucket
                var sprintsInBucket = allSprints.Value
                    .Where(s => s.Attributes.StartDate >= periodStart && s.Attributes.StartDate < periodEnd)
                    .ToList();

                double bucketAssigned = 0;
                double bucketCompleted = 0;

                foreach (var sprint in sprintsInBucket)
                {
                    // Query DB for this specific sprint + area path
                    var dbData = await GetSprintDataFromDbAsync(sprint.Path, selectedAreaPath, sprint.Attributes.FinishDate.Value);

                    bucketAssigned += dbData.Assigned;
                    bucketCompleted += dbData.Completed;
                }

                // Generate Label (e.g., "Jan 2026" or "Q1 2026")
                string label = unit switch
                {
                    "quarterly" => $"Q{((periodStart.Month - 1) / 3) + 1} {periodStart:yyyy}",
                    "yearly" => periodStart.ToString("yyyy"),
                    _ => periodStart.ToString("MMM yyyy")
                };

                finalStats.Add(new SprintProgressDto
                {
                    IterationPath = label,
                    TotalPointsAssigned = bucketAssigned,
                    TotalPointsCompleted = bucketCompleted,
                    SortDate = periodStart
                });

                finalSpillage.Add(new SpillageTrendDto
                {
                    IterationPath = label,
                    SpillagePoints = bucketAssigned - bucketCompleted,
                    SortDate = periodStart
                });
            }

            return new CombinedSprintDataDto
            {
                Stats = finalStats,
                Spillage = finalSpillage
            };
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

        // Helper to encapsulate the Database Join logic
        private async Task<(double Assigned, double Completed)> GetSprintDataFromDbAsync(string iterationPath, string areaPath, DateTime sprintEnd)
        {
            var doneStates = new[] { "Closed", "Done", "Verified", "Completed", "Live" };

            var dbStories = await _context.IvpUserStoryIterations
                .Join(_context.IvpUserStoryDetails, usi => usi.UserStoryId, usd => usd.UserStoryId, (usi, usd) => new { usi, usd })
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
    }

    public class SprintDto
    {
        public Guid Id { get; set; }
        public string Name { get; set; }
        public string Path { get; set; }
        public Attributes Attributes { get; set; } // Contains StartDate and FinishDate
    }

    public class Attributes
    {
        public DateTime? StartDate { get; set; }
        public DateTime? FinishDate { get; set; }
        public string TimeFrame { get; set; } // "past", "current", or "future"
    }

    public class CombinedSprintDataDto
    {
        public List<SprintProgressDto> Stats { get; set; } = new();
        public List<SpillageTrendDto> Spillage { get; set; } = new();
    }
}
