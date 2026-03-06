using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using Microsoft.AspNetCore.Mvc;
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

                var summary = new SpillageSummaryDto
                {
                    All = new SectionDto { Stats = statsAll, Spillage = trendAll },
                    Feature = new SectionDto { Stats = statsFeature, Spillage = trendFeature },
                    Client = new SectionDto { Stats = statsClient, Spillage = trendClient }
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

                var summary = new SpillageSummaryDto
                {
                    All = new SectionDto { Stats = statsAll, Spillage = timelineAll },
                    Feature = new SectionDto { Stats = statsFeature, Spillage = timelineFeature },
                    Client = new SectionDto { Stats = statsClient, Spillage = timelineClient }
                };

                return summary;
            }
            catch (Exception e)
            {
                Console.WriteLine(e.Message);
                return new SpillageSummaryDto();
            }
        }
    }
}
