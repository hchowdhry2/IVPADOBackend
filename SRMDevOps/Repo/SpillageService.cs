using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Text.Json;
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
        // Make SortDate nullable because EF Max(...) can be null for some groups.
        private sealed record AggregatedStat(string FullPath, double Total, double Closed, DateTime? SortDate);

        // Helper: compute default start date for legacy timeframe strings
        private static DateTime GetStartDate(string timeframe) =>
            timeframe?.ToLower() == "yearly" ? DateTime.Now.AddYears(-1) : DateTime.Now.AddMonths(-6);

        // Helper: get recent sprint paths (same criteria as original methods)
        private async Task<List<(string Path, DateTime? SortDate)>> GetRecentSprintsAsync(string projectName, int lastNSprints)
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
                    // Calculate the Mode Date (the date with the most assignments)
                    SortDate = g.GroupBy(x => x.AssignedDate.Date)
                                .OrderByDescending(dg => dg.Count())
                                .Select(dg => dg.Key)
                                .FirstOrDefault()
                })
                .OrderByDescending(x => x.SortDate) // This now sorts by the 'real' sprint timeframe
                .Take(lastNSprints)
                .ToListAsync();

            return raw
                .Select(x => (Path: x.Path ?? string.Empty, SortDate: (DateTime?)x.SortDate))
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
                // Group internal dates to find the most frequent day
                DateFrequencies = g.GroupBy(x => x.usi.AssignedDate.Date)
                                   .Select(dg => new { Date = dg.Key, Count = dg.Count() })
                                   .OrderByDescending(dg => dg.Count)
                                   .FirstOrDefault()
            })
            .ToListAsync();

            var grouped = rawGrouped
                .Select(g => new AggregatedStat(
                    g.FullPath ?? string.Empty,
                    g.Total,
                    g.Closed,
                    g.DateFrequencies?.Date ?? DateTime.MinValue // This is now your "SortDate"
                ))
                .ToList();

            return grouped;
        }

        // Helper: normalize period unit and get bucket size (months) and default n
        private static (string unit, int bucketMonths, int defaultN) NormalizePeriodUnit(string? unit)
        {
            var u = unit?.Trim().ToLowerInvariant();
            return u switch
            {
                "quarter" or "quarterly" => ( "quarterly", 3, 4 ), // default 4 quarters = 1 year
                "year" or "yearly" => ( "yearly", 12, 1 ),
                _ => ( "monthly", 1, 6 ) // default monthly, 6 months
            };
        }

        // Helper: compute window start from period unit and number of periods (nPeriods)
        private static DateTime ComputeWindowStart(string unit, int nPeriods)
        {
            var now = DateTime.Now;
            var bucketMonths = unit == "quarterly" ? 3 : unit == "yearly" ? 12 : 1;
            var monthsBack = nPeriods * bucketMonths;
            var windowStart = new DateTime(now.Year, now.Month, 1).AddMonths(-(monthsBack - 1));
            return windowStart;
        }

        // New: generalized period aggregator for spillage (monthly/quarterly/yearly)
        public async Task<List<SpillageTrendDto>> GetSpillageByPeriodAsync(string projectName, string? periodUnit, int? n, string? parentType = null)
        {
            try
            {
                var (unit, bucketMonths, defaultN) = NormalizePeriodUnit(periodUnit);
                var periods = n.HasValue && n.Value > 0 ? n.Value : defaultN;

                if (periods <= 0) return new List<SpillageTrendDto>();

                var windowStart = ComputeWindowStart(unit, periods);
                // Fetch aggregated data across iterations (per sprint) for the project/parentType
                // NOTE: do not pre-filter by FirstInprogressTime here — we want to aggregate per iteration
                // then bucket by the iteration SortDate below. Pre-filtering by FirstInprogressTime caused
                // mismatches with GetSprintStatsByPeriodAsync.
                
                var aggregated = await GetAggregatedStatsAsync(projectName: projectName, parentType: parentType, minFirstInprogress: null, requireFirstInprogressNotNull: true);

                

                var result = new List<SpillageTrendDto>();

                for (int p = 0; p < periods; p++)
                {
                    var periodStart = windowStart.AddMonths(p * bucketMonths);
                    var periodEnd = periodStart.AddMonths(bucketMonths).AddTicks(-1);

                    var inPeriod = aggregated
                        .Where(a => a.SortDate >= periodStart && a.SortDate <= periodEnd)
                        .ToList();

                    var total = inPeriod.Sum(x => x.Total);
                    var closed = inPeriod.Sum(x => x.Closed);

                    var label = unit switch
                    {
                        "quarterly" => $"Q{((periodStart.Month - 1) / 3) + 1} {periodStart:yyyy}",
                        "yearly" => periodStart.ToString("yyyy"),
                        _ => periodStart.ToString("MMM yyyy")
                    };

                    result.Add(new SpillageTrendDto
                    {
                        IterationPath = label,
                        SpillagePoints = total - closed,
                        SortDate = periodStart
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

        // New: generalized period aggregator for sprint stats (monthly/quarterly/yearly)
        public async Task<List<SprintProgressDto>> GetSprintStatsByPeriodAsync(string projectName, string? periodUnit, int? n, string? parentType = null)
        {
            try
            {
                var (unit, bucketMonths, defaultN) = NormalizePeriodUnit(periodUnit);
                var periods = n.HasValue && n.Value > 0 ? n.Value : defaultN;

                if (periods <= 0) return new List<SprintProgressDto>();

                // 1. Calculate the start of the timeframe
                var windowStart = ComputeWindowStart(unit, periods);

                // 2. IMPORTANT: We fetch ALL data for the project. 
                // We will filter by the Mid-Date of the Sprints in-memory to ensure accuracy.
                // I've removed minFirstInprogress here because we want to filter by SPRINT date, not story date.
                var aggregated = await GetAggregatedStatsAsync(
                    projectName: projectName,
                    parentType: parentType,
                    minFirstInprogress: null, // Pass null to get the "Timeline" of sprints
                    requireFirstInprogressNotNull: true);

                DebugLogAggregated(aggregated, $"GetSpillageByPeriodAsync project={projectName} parentType={parentType} windowStart={windowStart:yyyy-MM-dd}");

                var result = new List<SprintProgressDto>();

                // 3. Loop through each period (e.g., Nov, Dec, Jan)
                for (int p = 0; p < periods; p++)
                {
                    var periodStart = windowStart.AddMonths(p * bucketMonths);
                    // Use exclusive end date for clean bucketing
                    var periodEnd = periodStart.AddMonths(bucketMonths);

                    // 4. Find all sprints whose "Center of Gravity" (SortDate) falls in this month
                    // This ensures a sprint is counted EXACTLY once in the most relevant month.
                    var inPeriod = aggregated
                        .Where(a => a.SortDate >= periodStart && a.SortDate < periodEnd)
                        .ToList();

                    var total = inPeriod.Sum(x => x.Total);
                    var closed = inPeriod.Sum(x => x.Closed);

                    var label = unit switch
                    {
                        "quarterly" => $"Q{((periodStart.Month - 1) / 3) + 1} {periodStart:yyyy}",
                        "yearly" => periodStart.ToString("yyyy"),
                        _ => periodStart.ToString("MMM yyyy")
                    };

                    result.Add(new SprintProgressDto
                    {
                        IterationPath = label,
                        TotalPointsAssigned = total,
                        TotalPointsCompleted = closed,
                        SortDate = periodStart
                    });
                }

                return result;
            }
            catch (Exception e)
            {
                Console.WriteLine($"Error in GetSprintStatsByPeriodAsync: {e.Message}");
                return new List<SprintProgressDto>();
            }
        }

        // Mapping helpers (unchanged)
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

        // Existing public methods (unchanged behavior for last-N and legacy timeframe)
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

        // New: aggregate summary for last N sprints (unchanged)
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

        // Updated: aggregate summary for timeframe-based queries — now supports periodUnit and n
        public async Task<SpillageSummaryDto> GetSpillageSummaryTime(string projectName, string? periodUnit = null, int? n = null)
        {
            try
            {
                // Use new period aggregators for each section (All/Feature/Client)
                var statsAll = await GetSprintStatsByPeriodAsync(projectName, periodUnit, n, null);
                var statsFeature = await GetSprintStatsByPeriodAsync(projectName, periodUnit, n, "Feature");
                var statsClient = await GetSprintStatsByPeriodAsync(projectName, periodUnit, n, "Client Issue");

                var spillageAll = await GetSpillageByPeriodAsync(projectName, periodUnit, n, null);
                var spillageFeature = await GetSpillageByPeriodAsync(projectName, periodUnit, n, "Feature");
                var spillageClient = await GetSpillageByPeriodAsync(projectName, periodUnit, n, "Client Issue");

                // For histories use same period window; to avoid extra DB calls you may choose to omit history here or make it conditional.
                // We'll compute history using period->startDate (legacy GetStoryHistoryByTimeframe still exists; to avoid breaking signatures we will compute a startDate and call the timeframe-based history)
                var (unit, _, defaultN) = NormalizePeriodUnit(periodUnit);
                var periods = n.HasValue && n.Value > 0 ? n.Value : defaultN;
                var windowStart = ComputeWindowStart(unit, periods);

                var historyAll = await GetStoryHistoryByStartDate(projectName, windowStart, null);
                var historyFeature = await GetStoryHistoryByStartDate(projectName, windowStart, "Feature");
                var historyClient = await GetStoryHistoryByStartDate(projectName, windowStart, "Client Issue");

                var summary = new SpillageSummaryDto
                {
                    All = new SectionDto { Stats = statsAll, Spillage = spillageAll, History = historyAll },
                    Feature = new SectionDto { Stats = statsFeature, Spillage = spillageFeature, History = historyFeature },
                    Client = new SectionDto { Stats = statsClient, Spillage = spillageClient, History = historyClient }
                };

                return summary;
            }
            catch (Exception e)
            {
                Console.WriteLine(e.Message);
                return new SpillageSummaryDto();
            }
        }

        // ---------- History helpers (new overload used above) ----------
        // Existing methods GetStoryHistoryLastNSprints and GetStoryHistoryByTimeframe remain; add small helper that filters by computed startDate.

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

        public async Task<List<StoryHistoryDto>> GetStoryHistoryByTimeframe(string projectName, string timeframe, string? parentType = null)
        {
            try
            {
                var startDate = GetStartDate(timeframe);

                return await GetStoryHistoryByStartDate(projectName, startDate, parentType);
            }
            catch (Exception e)
            {
                Console.WriteLine(e.Message);
                return new List<StoryHistoryDto>();
            }
        }

        // New: history fetch based on explicit startDate (used by period-based summary)
        private async Task<List<StoryHistoryDto>> GetStoryHistoryByStartDate(string projectName, DateTime startDate, string? parentType = null)
        {
            try
            {
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

        // Debug helper: serialize aggregated contents to Output / Console for inspection
        private void DebugLogAggregated(IEnumerable<AggregatedStat> aggregated, string context)
        {
            try
            {
                var items = aggregated
                    .Select(a => new
                    {
                        FullPath = a.FullPath,
                        Total = a.Total,
                        Closed = a.Closed,
                        SortDate = a.SortDate.HasValue ? a.SortDate.Value.ToString("o") : null
                    })
                    .ToList();

                var payload = new { Context = context, Count = items.Count, Items = items };
                var json = JsonSerializer.Serialize(payload, new JsonSerializerOptions { WriteIndented = true });

                // Visible in Visual Studio Output window when running under debugger
                Debug.WriteLine(json);

                // Also write to Console in non-debug runs (API logs / container stdout)
                Console.WriteLine(json);
            }
            catch
            {
                // Swallow any logging errors to avoid breaking behavior
            }
        }
    }
}
