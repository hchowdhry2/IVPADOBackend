using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using Microsoft.EntityFrameworkCore;
using SRMDevOps.DataAccess;
using SRMDevOps.Dto;

namespace SRMDevOps.Repo
{
    public class SpillageService : ISpillage
    {
        private readonly IvpadodashboardContext _context;

        public SpillageService(IvpadodashboardContext context)
        {
            _context = context;
        }

        // Private aggregate holder to return from in-memory mapping
        private sealed record AggregatedStat(string FullPath, double Total, double Closed, DateTime SortDate);

        // Helper: compute start date based on timeframe
        private static DateTime GetStartDate(string timeframe) =>
            timeframe?.ToLower() == "yearly" ? DateTime.Now.AddYears(-1) : DateTime.Now.AddMonths(-6);

        // Helper: get recent sprint paths (same criteria as original methods)
        private async Task<List<(string Path, DateTime SortDate)>> GetRecentSprintsAsync(string projectName, int lastNSprints)
        {
            var raw = await _context.IvpUserStoryIterations
                .Where(usi =>
                    usi.IterationPath != null &&
                    usi.IterationPath.StartsWith(projectName) &&
                    usi.IterationPath.Contains("\\") &&
                    !usi.IterationPath.Contains("Rearch"))
                .GroupBy(usi => usi.IterationPath)
                .Select(g => new
                {
                    Path = g.Key,
                    SortDate = g.Max(x => x.AssignedDate)
                })
                .OrderByDescending(x => x.SortDate)
                .Take(lastNSprints)
                .ToListAsync();

            return raw
                .Select(x => (x.Path ?? string.Empty, x.SortDate))
                .ToList();
        }

        // Core aggregator that centralizes the repeated Join / Where / GroupBy / Select logic
        private async Task<List<AggregatedStat>> GetAggregatedStatsAsync(
            string? projectName = null,
            IEnumerable<string>? iterationPaths = null,
            string? parentType = null,
            DateTime? minFirstInprogress = null,
            bool requireFirstInprogressNotNull = false,
            bool excludeRearch = true)
        {
            var query = _context.IvpUserStoryIterations
                .Join(_context.IvpUserStoryDetails,
                    usi => usi.UserStoryId,
                    usd => usd.UserStoryId,
                    (usi, usd) => new { usi, usd })
                .AsQueryable();

            if (iterationPaths != null)
            {
                var pathsList = iterationPaths.ToList();
                query = query.Where(c => c.usi.IterationPath != null && pathsList.Contains(c.usi.IterationPath));
            }
            else if (!string.IsNullOrEmpty(projectName))
            {
                query = query.Where(c =>
                    c.usi.IterationPath != null &&
                    c.usi.IterationPath.StartsWith(projectName) &&
                    c.usi.IterationPath.Contains("\\") &&
                    (!excludeRearch || !c.usi.IterationPath.Contains("Rearch")));
            }

            if (!string.IsNullOrEmpty(parentType))
                query = query.Where(c => c.usd.ParentType == parentType);

            if (minFirstInprogress.HasValue)
                query = query.Where(c => c.usd.FirstInprogressTime >= minFirstInprogress.Value);

            if (requireFirstInprogressNotNull)
                query = query.Where(c => c.usd.FirstInprogressTime != null);

            var rawGrouped = await query
                .GroupBy(c => c.usi.IterationPath)
                .Select(g => new
                {
                    FullPath = g.Key,
                    Total = g.Sum(x => x.usd.StoryPoints ?? 0),
                    Closed = g.Sum(x => x.usd.State == "Closed" ? (x.usd.StoryPoints ?? 0) : 0),
                    SortDate = g.Max(x => x.usi.AssignedDate)
                })
                .ToListAsync();

            var grouped = rawGrouped
                .Select(g => new AggregatedStat(g.FullPath ?? string.Empty, g.Total, g.Closed, g.SortDate))
                .ToList();

            return grouped;
        }

        /// <summary>
        /// Returns monthly-bucketed spillage (month label like "Jan 2026") for the given timeframe.
        /// timeframe: "monthly" or "yearly". 
        /// - If timeframe == "monthly", <paramref name="n"/> is number of months (defaults to 6).
        /// - If timeframe == "yearly", <paramref name="n"/> is number of years (defaults to 1) and will be converted to months.
        /// parentType: null for all, "Feature" or "Client Issue".
        /// </summary>
        public async Task<List<SpillageTrendDto>> GetSpillageByMonthsAsync(string projectName, string timeframe, int? n = null, string? parentType = null)
        {
            try
            {
                // 1. Determine months window
                int monthsBack;
                if (string.Equals(timeframe, "monthly", StringComparison.OrdinalIgnoreCase))
                    monthsBack = n ?? 6;
                else if (string.Equals(timeframe, "yearly", StringComparison.OrdinalIgnoreCase))
                    monthsBack = (n ?? 1) * 12;
                else
                    monthsBack = n ?? 6;

                if (monthsBack <= 0) return new List<SpillageTrendDto>();

                // 2. Set Time Window
                var now = DateTime.Now;
                var windowStart = new DateTime(now.Year, now.Month, 1).AddMonths(-(monthsBack - 1));

                // 3. Get raw data (fetch all relevant data first)
                var aggregated = await GetAggregatedStatsAsync(
                    projectName: projectName,
                    parentType: parentType,
                    minFirstInprogress: null,
                    requireFirstInprogressNotNull: false
                );

                var result = new List<SpillageTrendDto>();

                // 4. Loop through every expected month to fill gaps
                for (int i = 0; i < monthsBack; i++)
                {
                    var currentMonthStart = windowStart.AddMonths(i);
                    var currentMonthEnd = currentMonthStart.AddMonths(1).AddTicks(-1);

                    // Filter data for this specific month
                    var inMonth = aggregated
                        .Where(a => a.SortDate >= currentMonthStart && a.SortDate <= currentMonthEnd)
                        .ToList();

                    var total = inMonth.Sum(x => x.Total);
                    var closed = inMonth.Sum(x => x.Closed);

                    // 5. Map to SpillageTrendDto (Spillage = Total - Closed)
                    result.Add(new SpillageTrendDto
                    {
                        IterationPath = currentMonthStart.ToString("MMM yyyy"), // e.g. "Oct 2023"
                        SpillagePoints = total - closed,
                        SortDate = currentMonthStart
                    });
                }

                return result;
            }
            catch (Exception e)
            {
                Console.WriteLine(e.Message);
                return new List<SpillageTrendDto>();
            }
        }

        /// <summary>
        /// Returns monthly-bucketed sprint stats (month label like "Jan 2026") for the given timeframe.
        /// Uses same timeframe semantics as GetSpillageByMonthsAsync.
        /// </summary>
        /// 
        public async Task<List<SprintProgressDto>> GetSprintStatsByMonthsAsync(string projectName, string timeframe, int? n = null, string? parentType = null)
        {
            try
            {
                int monthsBack;
                if (string.Equals(timeframe, "monthly", StringComparison.OrdinalIgnoreCase))
                    monthsBack = n ?? 6;
                else if (string.Equals(timeframe, "yearly", StringComparison.OrdinalIgnoreCase))
                    monthsBack = (n ?? 1) * 12;
                else if (string.Equals(timeframe, "quarterly", StringComparison.OrdinalIgnoreCase))
                    monthsBack = (n ?? 1) * 3;
                else
                    monthsBack = n ?? 6;

                if (monthsBack <= 0) return new List<SprintProgressDto>();

                var now = DateTime.Now;
                var windowStart = new DateTime(now.Year, now.Month, 1).AddMonths(-(monthsBack - 1));

                // Get raw data
                var aggregated = await GetAggregatedStatsAsync(projectName: projectName, parentType: parentType, minFirstInprogress: null, requireFirstInprogressNotNull: false);

                // Correct List Type
                var result = new List<SprintProgressDto>();

                for (int i = 0; i < monthsBack; i++)
                {
                    var currentMonthStart = windowStart.AddMonths(i);
                    var currentMonthEnd = currentMonthStart.AddMonths(1).AddTicks(-1);

                    var inMonth = aggregated
                        .Where(a => a.SortDate >= currentMonthStart && a.SortDate <= currentMonthEnd)
                        .ToList();

                    var total = inMonth.Sum(x => x.Total);
                    var closed = inMonth.Sum(x => x.Closed);

                    // Correct DTO and Property Mapping
                    result.Add(new SprintProgressDto
                    {
                        IterationPath = currentMonthStart.ToString("MMM yyyy"),
                        TotalPointsAssigned = total,      // Store Total
                        TotalPointsCompleted = closed,    // Store Closed
                        SortDate = currentMonthStart
                    });
                }

                return result;
            }
            catch (Exception e)
            {
                Console.WriteLine(e.Message);
                return new List<SprintProgressDto>();
            }
        }

        // Mapping helpers
        private static List<SpillageTrendDto> ToSpillageTrendDto(List<AggregatedStat> stats)
        {
            return stats
                .OrderBy(s => s.SortDate)
                .Select(s => new SpillageTrendDto
                {
                    IterationPath = s.FullPath.Split('\\').Last(),
                    SpillagePoints = s.Total - s.Closed,
                    SortDate = s.SortDate
                })
                .ToList();
        }

        private static List<SprintProgressDto> ToSprintProgressDto(List<AggregatedStat> stats)
        {
            return stats
                .Select(s => new SprintProgressDto
                {
                    IterationPath = s.FullPath,
                    TotalPointsAssigned = s.Total,
                    TotalPointsCompleted = s.Closed,
                    SortDate = s.SortDate
                })
                .ToList();
        }

        // Existing public methods (unchanged behavior, return empty lists on errors)
        public async Task<List<SpillageTrendDto>> GetAllSpillageTimeline(string projectName, string timeframe)
        {
            try
            {
                var startDate = GetStartDate(timeframe);
                var stats = await GetAggregatedStatsAsync(projectName: projectName, minFirstInprogress: startDate, requireFirstInprogressNotNull: false);
                return ToSpillageTrendDto(stats);
            }
            catch (Exception e)
            {
                Console.WriteLine(e.Message);
                return new List<SpillageTrendDto>();
            }
        }

        public async Task<List<SpillageTrendDto>> GetAllSpillageTrend(string projectName, int lastNSprints)
        {
            try
            {
                var recent = await GetRecentSprintsAsync(projectName, lastNSprints);
                if (!recent.Any()) return new List<SpillageTrendDto>();

                var sprintPaths = recent.Select(s => s.Path).ToList();
                var stats = await GetAggregatedStatsAsync(iterationPaths: sprintPaths, requireFirstInprogressNotNull: true);
                return ToSpillageTrendDto(stats);
            }
            catch (Exception e)
            {
                Console.WriteLine(e.Message);
                return new List<SpillageTrendDto>();
            }
        }

        public async Task<List<SprintProgressDto>> GetAllSprintStats(string projectName, int lastNSprints)
        {
            try
            {
                var recent = await GetRecentSprintsAsync(projectName, lastNSprints);
                if (!recent.Any()) return new List<SprintProgressDto>();

                var sprintPaths = recent.Select(s => s.Path).ToList();
                var aggregated = await GetAggregatedStatsAsync(iterationPaths: sprintPaths, requireFirstInprogressNotNull: true);

                var stats = ToSprintProgressDto(aggregated);

                var finalResult = recent
                    .OrderBy(s => s.SortDate)
                    .Select(s => stats.FirstOrDefault(st => st.IterationPath == s.Path) ?? new SprintProgressDto
                    {
                        IterationPath = s.Path,
                        TotalPointsAssigned = 0,
                        TotalPointsCompleted = 0
                    })
                    .ToList();

                return finalResult;
            }
            catch (Exception e)
            {
                Console.WriteLine(e.Message);
                return new List<SprintProgressDto>();
            }
        }

        public async Task<List<SprintProgressDto>> GetAllSprintStatsByTime(string projectName, string timeframe)
        {
            try
            {
                var startDate = GetStartDate(timeframe);
                var aggregated = await GetAggregatedStatsAsync(projectName: projectName, minFirstInprogress: startDate, requireFirstInprogressNotNull: false);
                var stats = ToSprintProgressDto(aggregated).OrderBy(x => x.SortDate).ToList();
                return stats;
            }
            catch (Exception e)
            {
                Console.WriteLine(e.Message);
                return new List<SprintProgressDto>();
            }
        }

        // Feature services
        public async Task<List<SpillageTrendDto>> GetFeatureSpillageTimeline(string projectName, string timeframe)
        {
            try
            {
                var startDate = GetStartDate(timeframe);
                var stats = await GetAggregatedStatsAsync(projectName: projectName, parentType: "Feature", minFirstInprogress: startDate, requireFirstInprogressNotNull: false);
                return ToSpillageTrendDto(stats);
            }
            catch (Exception e)
            {
                Console.WriteLine(e.Message);
                return new List<SpillageTrendDto>();
            }
        }

        public async Task<List<SpillageTrendDto>> GetFeatureSpillageTrend(string projectName, int lastNSprints)
        {
            try
            {
                var recent = await GetRecentSprintsAsync(projectName, lastNSprints);
                if (!recent.Any()) return new List<SpillageTrendDto>();

                var sprintPaths = recent.Select(s => s.Path).ToList();
                var stats = await GetAggregatedStatsAsync(iterationPaths: sprintPaths, parentType: "Feature", requireFirstInprogressNotNull: true);
                return ToSpillageTrendDto(stats);
            }
            catch (Exception e)
            {
                Console.WriteLine(e.Message);
                return new List<SpillageTrendDto>();
            }
        }

        public async Task<List<SprintProgressDto>> GetFeatureSprintStats(string projectName, int lastNSprints)
        {
            try
            {
                var recent = await GetRecentSprintsAsync(projectName, lastNSprints);
                if (!recent.Any()) return new List<SprintProgressDto>();

                var sprintPaths = recent.Select(s => s.Path).ToList();
                var aggregated = await GetAggregatedStatsAsync(iterationPaths: sprintPaths, parentType: "Feature", requireFirstInprogressNotNull: true);
                var stats = ToSprintProgressDto(aggregated);

                var finalResult = recent
                    .OrderBy(s => s.SortDate)
                    .Select(s => stats.FirstOrDefault(st => st.IterationPath == s.Path) ?? new SprintProgressDto
                    {
                        IterationPath = s.Path,
                        TotalPointsAssigned = 0,
                        TotalPointsCompleted = 0
                    })
                    .ToList();

                return finalResult;
            }
            catch (Exception e)
            {
                Console.WriteLine(e.Message);
                return new List<SprintProgressDto>();
            }
        }

        public async Task<List<SprintProgressDto>> GetFeatureSprintStatsByTime(string projectName, string timeframe)
        {
            try
            {
                var startDate = GetStartDate(timeframe);
                var aggregated = await GetAggregatedStatsAsync(projectName: projectName, parentType: "Feature", minFirstInprogress: startDate, requireFirstInprogressNotNull: false);
                var stats = ToSprintProgressDto(aggregated).OrderBy(x => x.SortDate).ToList();
                return stats;
            }
            catch (Exception e)
            {
                Console.WriteLine(e.Message);
                return new List<SprintProgressDto>();
            }
        }

        // Client services
        public async Task<List<SpillageTrendDto>> GetClientSpillageTimeline(string projectName, string timeframe)
        {
            try
            {
                var startDate = GetStartDate(timeframe);
                var stats = await GetAggregatedStatsAsync(projectName: projectName, parentType: "Client Issue", minFirstInprogress: startDate, requireFirstInprogressNotNull: false);
                return ToSpillageTrendDto(stats);
            }
            catch (Exception e)
            {
                Console.WriteLine(e.Message);
                return new List<SpillageTrendDto>();
            }
        }

        public async Task<List<SpillageTrendDto>> GetClientSpillageTrend(string projectName, int lastNSprints)
        {
            try
            {
                var recent = await GetRecentSprintsAsync(projectName, lastNSprints);
                if (!recent.Any()) return new List<SpillageTrendDto>();

                var sprintPaths = recent.Select(s => s.Path).ToList();
                var stats = await GetAggregatedStatsAsync(iterationPaths: sprintPaths, parentType: "Client Issue", requireFirstInprogressNotNull: true);
                return ToSpillageTrendDto(stats);
            }
            catch (Exception e)
            {
                Console.WriteLine(e.Message);
                return new List<SpillageTrendDto>();
            }
        }

        public async Task<List<SprintProgressDto>> GetClientSprintStats(string projectName, int lastNSprints)
        {
            try
            {
                var recent = await GetRecentSprintsAsync(projectName, lastNSprints);
                if (!recent.Any()) return new List<SprintProgressDto>();

                var sprintPaths = recent.Select(s => s.Path).ToList();
                var aggregated = await GetAggregatedStatsAsync(iterationPaths: sprintPaths, parentType: "Client Issue", requireFirstInprogressNotNull: true);
                // Note: keep behavior consistent with other methods (mapping & ordering)
                var stats = ToSprintProgressDto(aggregated);

                var finalResult = recent
                    .OrderBy(s => s.SortDate)
                    .Select(s => stats.FirstOrDefault(st => st.IterationPath == s.Path) ?? new SprintProgressDto
                    {
                        IterationPath = s.Path,
                        TotalPointsAssigned = 0,
                        TotalPointsCompleted = 0
                    })
                    .ToList();

                return finalResult;
            }
            catch (Exception e)
            {
                Console.WriteLine(e.Message);
                return new List<SprintProgressDto>();
            }
        }

        public async Task<List<SprintProgressDto>> GetClientSprintStatsByTime(string projectName, string timeframe)
        {
            try
            {
                var startDate = GetStartDate(timeframe);
                var aggregated = await GetAggregatedStatsAsync(projectName: projectName, parentType: "Client Issue", minFirstInprogress: startDate, requireFirstInprogressNotNull: false);
                var stats = ToSprintProgressDto(aggregated).OrderBy(x => x.SortDate).ToList();
                return stats;
            }
            catch (Exception e)
            {
                Console.WriteLine(e.Message);
                return new List<SprintProgressDto>();
            }
        }

        // New: aggregate summary for last N sprints (moved from controller)
        public async Task<SpillageSummaryDto> GetSpillageSummaryLast(string projectName, int lastNSprints)
        {
            try
            {
                var statsAll = await GetAllSprintStats(projectName, lastNSprints);
                var statsFeature = await GetFeatureSprintStats(projectName, lastNSprints);
                var statsClient = await GetClientSprintStats(projectName, lastNSprints);

                var trendAll = await GetAllSpillageTrend(projectName, lastNSprints);
                var trendFeature = await GetFeatureSpillageTrend(projectName, lastNSprints);
                var trendClient = await GetClientSpillageTrend(projectName, lastNSprints);

                // Fetch histories per section (filtered by parentType for feature/client)
                var historyAll = await GetStoryHistoryLastNSprints(projectName, lastNSprints, null);
                var historyFeature = await GetStoryHistoryLastNSprints(projectName, lastNSprints, "Feature");
                var historyClient = await GetStoryHistoryLastNSprints(projectName, lastNSprints, "Client Issue");

                var summary = new SpillageSummaryDto
                {
                    All = new SectionDto { Stats = statsAll, Spillage = trendAll, History = historyAll },
                    Feature = new SectionDto { Stats = statsFeature, Spillage = trendFeature, History = historyFeature },
                    Client = new SectionDto { Stats = statsClient, Spillage = trendClient, History = historyClient }
                };

                return summary;
            }
            catch (Exception e)
            {
                Console.WriteLine(e.Message);
                return new SpillageSummaryDto();
            }
        }

        // New: aggregate summary for timeframe-based queries (moved from controller)
        public async Task<SpillageSummaryDto> GetSpillageSummaryTime(string projectName, string timeframe)
        {
            try
            {
                var statsAll = await GetAllSprintStatsByTime(projectName, timeframe);
                var statsFeature = await GetFeatureSprintStatsByTime(projectName, timeframe);
                var statsClient = await GetClientSprintStatsByTime(projectName, timeframe);

                var timelineAll = await GetAllSpillageTimeline(projectName, timeframe);
                var timelineFeature = await GetFeatureSpillageTimeline(projectName, timeframe);
                var timelineClient = await GetClientSpillageTimeline(projectName, timeframe);

                // Fetch histories per section filtered by parentType for feature/client
                var historyAll = await GetStoryHistoryByTimeframe(projectName, timeframe, null);
                var historyFeature = await GetStoryHistoryByTimeframe(projectName, timeframe, "Feature");
                var historyClient = await GetStoryHistoryByTimeframe(projectName, timeframe, "Client Issue");

                var summary = new SpillageSummaryDto
                {
                    All = new SectionDto { Stats = statsAll, Spillage = timelineAll, History = historyAll },
                    Feature = new SectionDto { Stats = statsFeature, Spillage = timelineFeature, History = historyFeature },
                    Client = new SectionDto { Stats = statsClient, Spillage = timelineClient, History = historyClient }
                };

                return summary;
            }
            catch (Exception e)
            {
                Console.WriteLine(e.Message);
                return new SpillageSummaryDto();
            }
        }

        // New methods implementing the "spillage history per user story" logic.

        public async Task<List<StoryHistoryDto>> GetStoryHistoryLastNSprints(string projectName, int lastNSprints, string? parentType = null)
        {
            try
            {
                var recent = await GetRecentSprintsAsync(projectName, lastNSprints);
                if (!recent.Any()) return new List<StoryHistoryDto>();

                var sprintPaths = recent.Select(s => s.Path).ToList();

                var rowsQuery = _context.IvpUserStoryIterations
                    .Join(_context.IvpUserStoryDetails,
                          usi => usi.UserStoryId,
                          usd => usd.UserStoryId,
                          (usi, usd) => new { usi, usd })
                    .Where(x => x.usi.IterationPath != null && sprintPaths.Contains(x.usi.IterationPath) &&
                                x.usd.FirstInprogressTime != null);

                if (!string.IsNullOrEmpty(parentType))
                    rowsQuery = rowsQuery.Where(x => x.usd.ParentType == parentType);

                var rows = await rowsQuery
                    .Select(x => new
                    {
                        x.usd.UserStoryId,
                        Title = x.usd.Title,
                        State = x.usd.State,
                        FirstInprogressTime = x.usd.FirstInprogressTime,
                        ClosedDate = x.usd.ClosedDate,
                        AssignedDate = x.usi.AssignedDate,
                        IterationPath = x.usi.IterationPath
                    })
                    .ToListAsync();

                if (!rows.Any()) return new List<StoryHistoryDto>();

                var storyIds = rows.Select(r => r.UserStoryId).Distinct().ToList();

                var countsQuery = _context.IvpUserStoryIterations
                    .Where(i => storyIds.Contains(i.UserStoryId));

                var counts = await countsQuery
                    .GroupBy(i => i.UserStoryId)
                    .Select(g => new { UserStoryId = g.Key, Total = g.Count() })
                    .ToDictionaryAsync(x => x.UserStoryId, x => x.Total);

                var result = rows
                    .Select(r => new StoryHistoryDto
                    {
                        UserStoryId = r.UserStoryId,
                        Title = r.Title ?? string.Empty,
                        State = r.State ?? string.Empty,
                        FirstInprogressTime = r.FirstInprogressTime,
                        ClosedDate = r.ClosedDate,
                        AssignedDate = r.AssignedDate,
                        IterationPath = r.IterationPath ?? string.Empty,
                        TotalHistoryCount = counts.TryGetValue(r.UserStoryId, out var c) ? c : 0
                    })
                    .OrderBy(r => r.AssignedDate)
                    .ToList();

                return result;
            }
            catch (Exception e)
            {
                Console.WriteLine(e.Message);
                return new List<StoryHistoryDto>();
            }
        }

        public async Task<List<StoryHistoryDto>> GetStoryHistoryByTimeframe(string projectName, string timeframe, string? parentType = null)
        {
            try
            {
                var startDate = GetStartDate(timeframe);

                var rowsQuery = _context.IvpUserStoryIterations
                    .Join(_context.IvpUserStoryDetails,
                          usi => usi.UserStoryId,
                          usd => usd.UserStoryId,
                          (usi, usd) => new { usi, usd })
                    .Where(x =>
                        x.usi.IterationPath != null &&
                        x.usi.IterationPath.StartsWith(projectName) &&
                        x.usi.IterationPath.Contains("\\") &&
                        !x.usi.IterationPath.Contains("Rearch") &&
                        x.usd.FirstInprogressTime >= startDate);

                if (!string.IsNullOrEmpty(parentType))
                    rowsQuery = rowsQuery.Where(x => x.usd.ParentType == parentType);

                var rows = await rowsQuery
                    .Select(x => new
                    {
                        x.usd.UserStoryId,
                        Title = x.usd.Title,
                        State = x.usd.State,
                        FirstInprogressTime = x.usd.FirstInprogressTime,
                        ClosedDate = x.usd.ClosedDate,
                        AssignedDate = x.usi.AssignedDate,
                        IterationPath = x.usi.IterationPath
                    })
                    .ToListAsync();

                if (!rows.Any()) return new List<StoryHistoryDto>();

                var storyIds = rows.Select(r => r.UserStoryId).Distinct().ToList();

                var counts = await _context.IvpUserStoryIterations
                    .Where(i => storyIds.Contains(i.UserStoryId))
                    .GroupBy(i => i.UserStoryId)
                    .Select(g => new { UserStoryId = g.Key, Total = g.Count() })
                    .ToDictionaryAsync(x => x.UserStoryId, x => x.Total);

                var result = rows
                    .Select(r => new StoryHistoryDto
                    {
                        UserStoryId = r.UserStoryId,
                        Title = r.Title ?? string.Empty,
                        State = r.State ?? string.Empty,
                        FirstInprogressTime = r.FirstInprogressTime,
                        ClosedDate = r.ClosedDate,
                        AssignedDate = r.AssignedDate,
                        IterationPath = r.IterationPath ?? string.Empty,
                        TotalHistoryCount = counts.TryGetValue(r.UserStoryId, out var c) ? c : 0
                    })
                    .OrderBy(r => r.AssignedDate)
                    .ToList();

                return result;
            }
            catch (Exception e)
            {
                Console.WriteLine(e.Message);
                return new List<StoryHistoryDto>();
            }
        }
    }
}
