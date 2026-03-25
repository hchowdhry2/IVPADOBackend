using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using Microsoft.EntityFrameworkCore;
using SRMDevOps.DataAccess;
using SRMDevOps.Dto;
using static SRMDevOps.Repo.DevopsService;

namespace SRMDevOps.Repo
{
    public class SpillageService : ISpillage
    {
        private readonly IvpadodashboardContext _context;

        public SpillageService(IvpadodashboardContext context)
        {
            _context = context;
        }

        private sealed record AggregatedStat(string FullPath, double Total, double AddedLater, double Closed, 
                 DateTime? SortDate);

        /// <summary>
        /// Internal DB Aggregator: Only returns data for iterations that exist in the DB.
        /// </summary>
        /// 

        private async Task<List<AggregatedStat>> GetAggregatedStatsAsync(
    List<string> adoAreaPaths,
    Dictionary<string, (DateTime Start, DateTime End)> sprintDateMap,
    string? parentType = null)
        {
            var iterationPaths = sprintDateMap.Keys.ToList();

            var query = _context.IvpUserStoryIterations
                .Join(_context.IvpUserStoryDetails,
                    usi => usi.UserStoryId,
                    usd => usd.UserStoryId,
                    (usi, usd) => new { usi, usd })
                .Where(c => iterationPaths.Contains(c.usi.IterationPath) &&
                            adoAreaPaths.Contains(c.usd.AreaPath));

            if (!string.IsNullOrEmpty(parentType) && !string.Equals(parentType, "all", StringComparison.OrdinalIgnoreCase))
                query = query.Where(c => c.usd.ParentType == parentType);

            // FIX: Include AssignedDate in the select
            var rawData = await query.Select(x => new {
                x.usi.IterationPath,
                x.usi.AssignedDate, // Needed to check if it was added late
                x.usd.StoryPoints,
                x.usd.ClosedDate
            }).ToListAsync();

            return rawData
                .GroupBy(x => x.IterationPath)
                .Select(g => {
                    var path = g.Key ?? string.Empty;
                    if (!sprintDateMap.TryGetValue(path, out var dates))
                        return new AggregatedStat(path, 0, 0, 0, null);

                    var startLimit = dates.Start.Date;
                    var endLimit = dates.End.Date;

                    // 1. Completed Points
                    var completedPoints = g.Where(x => x.ClosedDate.HasValue &&
                                                       x.ClosedDate.Value.Date <= endLimit)
                                           .Sum(x => x.StoryPoints ?? 0);

                    // 2. Mid-Sprint Added Points (Logic: AssignedDate > Sprint Start Date)
                    var addedLaterPoints = g.Where(x => x.AssignedDate.Date > startLimit)
                                            .Sum(x => x.StoryPoints ?? 0);

                    // 3. Initial Points (Logic: Total minus what was added late)
                    var totalPoints = g.Sum(x => x.StoryPoints ?? 0);
                    var initialPoints = totalPoints - addedLaterPoints;

                    return new AggregatedStat(
                        path,
                        totalPoints, // We now treat "Total" as "Initially Assigned"
                        addedLaterPoints, // Pass the new metric
                        completedPoints,
                        dates.Start
                    );
                })
                .ToList();
        }
        //private async Task<List<AggregatedStat>> GetAggregatedStatsAsync(
        //    List<string> adoAreaPaths,
        //    Dictionary<string, (DateTime Start, DateTime End)> sprintDateMap,
        //    string? parentType = null)
        //{
        //    var iterationPaths = sprintDateMap.Keys.ToList();

        //    var query = _context.IvpUserStoryIterations
        //        .Join(_context.IvpUserStoryDetails,
        //            usi => usi.UserStoryId,
        //            usd => usd.UserStoryId,
        //            (usi, usd) => new { usi, usd })
        //        .Where(c => iterationPaths.Contains(c.usi.IterationPath) &&
        //                    adoAreaPaths.Contains(c.usd.AreaPath));

        //    if (!string.IsNullOrEmpty(parentType) && !string.Equals(parentType, "all", StringComparison.OrdinalIgnoreCase))
        //        query = query.Where(c => c.usd.ParentType == parentType);

        //    var rawData = await query.Select(x => new {
        //        x.usi.IterationPath,
        //        x.usd.StoryPoints,
        //        x.usd.ClosedDate
        //    }).ToListAsync();

        //    return rawData
        //        .GroupBy(x => x.IterationPath)
        //        .Select(g => {
        //            var path = g.Key ?? string.Empty;
        //            if (!sprintDateMap.TryGetValue(path, out var dates))
        //                return new AggregatedStat(path, 0, 0, null);

        //            var endLimit = dates.End.Date;
        //            var completedPoints = g.Where(x => x.ClosedDate.HasValue &&
        //                                               x.ClosedDate.Value.Date <= endLimit)
        //                                   .Sum(x => x.StoryPoints ?? 0);

        //            return new AggregatedStat(path, g.Sum(x => x.StoryPoints ?? 0), completedPoints, dates.Start);
        //        })
        //        .ToList();
        //}

        public async Task<List<SprintProgressDto>> GetSprintStatsAsync(List<string> adoAreaPaths, List<SprintDto> adoSprints, string? parentType = null)
        {
            try
            {
                if (adoSprints == null || !adoSprints.Any()) return new List<SprintProgressDto>();

                var dateMap = adoSprints.ToDictionary(
                    s => s.Path,
                    s => (s.Attributes.StartDate.Value, s.Attributes.FinishDate.Value),
                    StringComparer.OrdinalIgnoreCase
                );

                var aggregated = await GetAggregatedStatsAsync(adoAreaPaths, dateMap, parentType);

                // FIX: Map against the FULL list of ADO Sprints so empty ones show up as 0
                return adoSprints.Select(s => {
                    var dbMatch = aggregated.FirstOrDefault(a => a.FullPath.Equals(s.Path, StringComparison.OrdinalIgnoreCase));
                    return new SprintProgressDto
                    {
                        IterationPath = s.Name,
                        TotalPointsAssigned = dbMatch?.Total ?? 0,
                        MidSprintAddedPoints = dbMatch?.AddedLater ?? 0,
                        TotalPointsCompleted = dbMatch?.Closed ?? 0,
                        
                        SortDate = s.Attributes.StartDate
                    };
                })
                .OrderBy(x => x.SortDate)
                .ToList();
            }
            catch (Exception e)
            {
                Console.WriteLine($"Stats Error: {e.Message}");
                return new List<SprintProgressDto>();
            }
        }

        public async Task<List<SpillageTrendDto>> GetSpillageTrendAsync(List<string> adoAreaPaths, List<SprintDto> adoSprints, string? parentType = null)
        {
            try
            {
                if (adoSprints == null || !adoSprints.Any()) return new List<SpillageTrendDto>();

                var dateMap = adoSprints.ToDictionary(
                    s => s.Path,
                    s => (s.Attributes.StartDate.Value, s.Attributes.FinishDate.Value),
                    StringComparer.OrdinalIgnoreCase
                );

                var aggregated = await GetAggregatedStatsAsync(adoAreaPaths, dateMap, parentType);

                // FIX: Map against the FULL list of ADO Sprints
                return adoSprints.Select(s => {
                    var dbMatch = aggregated.FirstOrDefault(a => a.FullPath.Equals(s.Path, StringComparison.OrdinalIgnoreCase));
                    return new SpillageTrendDto
                    {
                        IterationPath = s.Name,
                        SpillagePoints = (dbMatch?.Total ?? 0) - (dbMatch?.Closed ?? 0),
                        SortDate = s.Attributes.StartDate
                    };
                })
                .OrderBy(x => x.SortDate)
                .ToList();
            }
            catch (Exception e)
            {
                Console.WriteLine($"Trend Error: {e.Message}");
                return new List<SpillageTrendDto>();
            }
        }

        public async Task<List<StoryHistoryDto>> GetStoryHistoryAsync(List<string> adoAreaPaths, List<SprintDto> adoSprints, string? parentType = null)
        {
            try
            {
                if (adoSprints == null || !adoSprints.Any()) return new List<StoryHistoryDto>();

                var sprintPaths = adoSprints.Select(s => s.Path).ToList();

                var rowsQuery = _context.IvpUserStoryIterations
                    .Join(_context.IvpUserStoryDetails,
                        usi => usi.UserStoryId,
                        usd => usd.UserStoryId,
                        (usi, usd) => new { usi, usd })
                    .Where(x => adoAreaPaths.Contains(x.usd.AreaPath) &&
                                sprintPaths.Contains(x.usi.IterationPath));

                if (!string.IsNullOrEmpty(parentType) && !string.Equals(parentType, "all", StringComparison.OrdinalIgnoreCase))
                    rowsQuery = rowsQuery.Where(x => x.usd.ParentType == parentType);

                var rows = await rowsQuery
                    .Select(x => new {
                        x.usd.UserStoryId,
                        x.usd.Title,
                        x.usd.State,
                        x.usd.FirstInprogressTime,
                        x.usd.ClosedDate,
                        x.usi.AssignedDate,
                        x.usi.IterationPath
                    })
                    .ToListAsync();

                if (!rows.Any()) return new List<StoryHistoryDto>();

                var storyIds = rows.Select(r => r.UserStoryId).Distinct().ToList();
                var counts = await _context.IvpUserStoryIterations
                    .Where(i => storyIds.Contains(i.UserStoryId))
                    .GroupBy(i => i.UserStoryId)
                    .Select(g => new { UserStoryId = g.Key, Total = g.Count() })
                    .ToDictionaryAsync(x => x.UserStoryId, x => x.Total);

                return rows.Select(r => new StoryHistoryDto
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
                .OrderByDescending(r => r.AssignedDate)
                .ToList();
            }
            catch (Exception e)
            {
                Console.WriteLine($"History Error: {e.Message}");
                return new List<StoryHistoryDto>();
            }
        }

        public async Task<SpillageSummaryDto> GetFullSummaryAsync(List<string> areaPaths, List<SprintDto> adoSprints)
        {
            return new SpillageSummaryDto
            {
                All = new SectionDto
                {
                    Stats = await GetSprintStatsAsync(areaPaths, adoSprints, "all"),
                    Spillage = await GetSpillageTrendAsync(areaPaths, adoSprints, "all"),
                    History = await GetStoryHistoryAsync(areaPaths, adoSprints, "all")
                },
                Feature = new SectionDto
                {
                    Stats = await GetSprintStatsAsync(areaPaths, adoSprints, "Feature"),
                    Spillage = await GetSpillageTrendAsync(areaPaths, adoSprints, "Feature"),
                    History = await GetStoryHistoryAsync(areaPaths, adoSprints, "Feature")
                },
                Client = new SectionDto
                {
                    Stats = await GetSprintStatsAsync(areaPaths, adoSprints, "Client Issue"),
                    Spillage = await GetSpillageTrendAsync(areaPaths, adoSprints, "Client Issue"),
                    History = await GetStoryHistoryAsync(areaPaths, adoSprints, "Client Issue")
                }
            };
        }
    }
}

//using System;
//using System.Collections.Generic;
//using System.Diagnostics;
//using System.Linq;
//using System.Text.Json;
//using System.Threading.Tasks;
//using Microsoft.EntityFrameworkCore;
//using SRMDevOps.DataAccess;
//using SRMDevOps.Dto;

//namespace SRMDevOps.Repo
//{
//    public class SpillageService : ISpillage
//    {
//        private readonly IvpadodashboardContext _context;

//        public SpillageService(IvpadodashboardContext context)
//        {
//            _context = context;
//        }

//        // Private aggregate holder to return from in-memory mapping
//        private sealed record AggregatedStat(string FullPath, double Total, double Closed, DateTime? SortDate);

//        // Helper: compute default start date for legacy timeframe strings
//        private static DateTime GetStartDate(string timeframe) =>
//            timeframe?.ToLower() == "yearly" ? DateTime.Now.AddYears(-1) : DateTime.Now.AddMonths(-6);

//        // Helper: get recent sprint paths (same criteria as original methods)
//        private async Task<List<(string Path, DateTime? SortDate)>> GetRecentSprintsAsync(string projectName, int lastNSprints)
//        {
//            var raw = await _context.IvpUserStoryIterations
//                .Where(usi =>
//                    usi.IterationPath != null &&
//                    usi.IterationPath.StartsWith(projectName) &&
//                    usi.IterationPath.Contains("\\") &&
//                    !usi.IterationPath.Contains("Rearch"))
//                .GroupBy(usi => usi.IterationPath)
//                .Select(g => new
//                {
//                    Path = g.Key,
//                    SortDate = g.GroupBy(x => x.AssignedDate.Date)
//                                .OrderByDescending(dg => dg.Count())
//                                .Select(dg => dg.Key)
//                                .FirstOrDefault()
//                })
//                .OrderByDescending(x => x.SortDate) 
//                .Take(lastNSprints)
//                .ToListAsync();

//            return raw
//                .Select(x => (Path: x.Path ?? string.Empty, SortDate: (DateTime?)x.SortDate))
//                .ToList();
//        }


//        private static (DateTime? Start, DateTime? End) ExtractSprintDates(string iterationPath)
//        {
//            if (string.IsNullOrEmpty(iterationPath)) return (null, null);

//            // Matches: "15 Dec - 02 Jan 2026" or "15 Dec 2025 - 02 Jan 2026"
//            // This pattern is more flexible for the year location
//            var pattern = @"(\d{1,2})\s+([a-zA-Z]{3}).*?(\d{1,2})\s+([a-zA-Z]{3})\s+(\d{4})";
//            var match = System.Text.RegularExpressions.Regex.Match(iterationPath, pattern);

//            if (match.Success)
//            {
//                var year = match.Groups[5].Value;
//                var startStr = $"{match.Groups[1].Value} {match.Groups[2].Value} {year}";
//                var endStr = $"{match.Groups[3].Value} {match.Groups[4].Value} {year}";

//                if (DateTime.TryParse(startStr, out var startDate) &&
//                    DateTime.TryParse(endStr, out var endDate))
//                {
//                    // If the sprint starts in Dec and ends in Jan, but only one year is provided,
//                    // the start year might actually be the previous year.
//                    if (startDate > endDate) startDate = startDate.AddYears(-1);

//                    return (startDate, endDate);
//                }
//            }
//            return (null, null);
//        }


//        // Core aggregator that centralizes the repeated Join / Where / GroupBy / Select logic

//        private async Task<List<AggregatedStat>> GetAggregatedStatsAsync(
//    List<string> adoAreaPaths, // Exact paths owned by the team from API
//    Dictionary<string, (DateTime Start, DateTime End)> sprintDateMap, // Dates from API
//    string? parentType = null)
//        {
//            var iterationPaths = sprintDateMap.Keys.ToList();

//            var query = _context.IvpUserStoryIterations
//                .Join(_context.IvpUserStoryDetails,
//                    usi => usi.UserStoryId,
//                    usd => usd.UserStoryId,
//                    (usi, usd) => new { usi, usd })
//                .Where(c => iterationPaths.Contains(c.usi.IterationPath) &&
//                            adoAreaPaths.Contains(c.usd.AreaPath)); // API-driven filtering

//            if (!string.IsNullOrEmpty(parentType))
//                query = query.Where(c => c.usd.ParentType == parentType);

//            var rawData = await query.Select(x => new {
//                x.usi.IterationPath,
//                x.usd.StoryPoints,
//                x.usd.ClosedDate
//            }).ToListAsync();

//            return rawData
//                .GroupBy(x => x.IterationPath)
//                .Select(g => {
//                    var path = g.Key ?? string.Empty;
//                    var dates = sprintDateMap[path];

//                    // Use the API End Date for the completion cutoff
//                    var endLimit = dates.End.Date;
//                    var completedPoints = g.Where(x => x.ClosedDate.HasValue &&
//                                                       x.ClosedDate.Value.Date <= endLimit)
//                                           .Sum(x => x.StoryPoints ?? 0);

//                    return new AggregatedStat(path, g.Sum(x => x.StoryPoints ?? 0), completedPoints, dates.Start);
//                })
//                .ToList();
//        }
//        //private async Task<List<AggregatedStat>> GetAggregatedStatsAsync(
//        //    string? projectName = null,
//        //    IEnumerable<string>? iterationPaths = null,
//        //    string? parentType = null,
//        //    bool excludeRearch = true)
//        //{
//        //    var query = _context.IvpUserStoryIterations
//        //        .Join(_context.IvpUserStoryDetails,
//        //            usi => usi.UserStoryId,
//        //            usd => usd.UserStoryId,
//        //            (usi, usd) => new { usi, usd })
//        //        .Where(c => c.usd.AreaPath == null || !c.usd.AreaPath.Contains(@"\QA")); // Exclude QA

//        //    if (iterationPaths != null)
//        //    {
//        //        var pathsList = iterationPaths.ToList();
//        //        query = query.Where(c => c.usi.IterationPath != null && pathsList.Contains(c.usi.IterationPath));
//        //    }
//        //    else if (!string.IsNullOrEmpty(projectName))
//        //    {
//        //        query = query.Where(c =>
//        //            c.usi.IterationPath != null &&
//        //            c.usi.IterationPath.StartsWith(projectName) &&
//        //            (!excludeRearch || !c.usi.IterationPath.Contains("Rearch")));
//        //    }

//        //    if (!string.IsNullOrEmpty(parentType))
//        //        query = query.Where(c => c.usd.ParentType == parentType);

//        //    // We fetch the raw data to handle the Regex and Date comparison in memory
//        //    var rawData = await query.Select(x => new {
//        //        x.usi.IterationPath,
//        //        x.usd.StoryPoints,
//        //        x.usd.ClosedDate,
//        //        x.usi.AssignedDate
//        //    }).ToListAsync();

//        //    var grouped = rawData
//        //    .GroupBy(x => x.IterationPath)
//        //    .Select(g => {
//        //        var path = g.Key ?? string.Empty;
//        //        var (sprintStart, sprintEnd) = ExtractSprintDates(path);

//        //        double completedPoints = 0;
//        //        if (sprintEnd.HasValue)
//        //        {
//        //            // Explicitly compare the Date part to avoid time-of-day discrepancies
//        //            var endLimit = sprintEnd.Value.Date;
//        //            completedPoints = g.Where(x => x.ClosedDate.HasValue && x.ClosedDate.Value.Date <= endLimit)
//        //                               .Sum(x => x.StoryPoints ?? 0);
//        //        }

//        //        return new AggregatedStat(
//        //            path,
//        //            g.Sum(x => x.StoryPoints ?? 0),
//        //            completedPoints,
//        //            sprintStart // Used for monthly aggregation
//        //        );
//        //    })
//        //    .ToList();

//        //    return grouped;
//        //}

//        // Helper: normalize period unit and get bucket size (months) and default n
//        private static (string unit, int bucketMonths, int defaultN) NormalizePeriodUnit(string? unit)
//        {
//            var u = unit?.Trim().ToLowerInvariant();
//            return u switch
//            {
//                "quarter" or "quarterly" => ( "quarterly", 3, 4 ), 
//                "year" or "yearly" => ( "yearly", 12, 1 ),
//                _ => ( "monthly", 1, 6 ) 
//            };
//        }

//        // Helper: compute window start from period unit and number of periods (nPeriods)
//        private static DateTime ComputeWindowStart(string unit, int nPeriods)
//        {
//            var now = DateTime.Now;
//            var bucketMonths = unit == "quarterly" ? 3 : unit == "yearly" ? 12 : 1;
//            var monthsBack = nPeriods * bucketMonths;
//            var windowStart = new DateTime(now.Year, now.Month, 1).AddMonths(-(monthsBack - 1));
//            return windowStart;
//        }

//        //*****
//        // Generalized period aggregator for spillage (monthly/quarterly/yearly)
//        public async Task<List<SpillageTrendDto>> GetSpillageByPeriodAsync(string projectName, string? periodUnit, int? n, string? parentType = null)
//        {
//            try
//            {
//                var (unit, bucketMonths, defaultN) = NormalizePeriodUnit(periodUnit);
//                var periods = n.HasValue && n.Value > 0 ? n.Value : defaultN;

//                if (periods <= 0) return new List<SpillageTrendDto>();

//                var windowStart = ComputeWindowStart(unit, periods);

//                var aggregated = await GetAggregatedStatsAsync(projectName: projectName, parentType: parentType);



//                var result = new List<SpillageTrendDto>();

//                for (int p = 0; p < periods; p++)
//                {
//                    var periodStart = windowStart.AddMonths(p * bucketMonths);
//                    var periodEnd = periodStart.AddMonths(bucketMonths); // No .AddTicks(-1)

//                    // Inside the for loop of GetSprintStatsByPeriodAsync
//                    var inPeriod = aggregated
//                        .Where(a => a.SortDate.HasValue &&
//                                    a.SortDate.Value >= periodStart &&
//                                    a.SortDate.Value < periodEnd)
//                        .ToList();

//                    var total = inPeriod.Sum(x => x.Total);
//                    var closed = inPeriod.Sum(x => x.Closed);

//                    var label = unit switch
//                    {
//                        "quarterly" => $"Q{((periodStart.Month - 1) / 3) + 1} {periodStart:yyyy}",
//                        "yearly" => periodStart.ToString("yyyy"),
//                        _ => periodStart.ToString("MMM yyyy")
//                    };

//                    result.Add(new SpillageTrendDto
//                    {
//                        IterationPath = label,
//                        SpillagePoints = total - closed,
//                        SortDate = periodStart
//                    });
//                }

//                return result;
//            }
//            catch (Exception e)
//            {
//                Console.WriteLine(e.Message);
//                return new List<SpillageTrendDto>();
//            }
//        }

//        //*****
//        // Generalized period aggregator for sprint stats (monthly/quarterly/yearly)
//        public async Task<List<SprintProgressDto>> GetSprintStatsByPeriodAsync(string projectName, string? periodUnit, int? n, string? parentType = null)
//        {
//            try
//            {
//                var (unit, bucketMonths, defaultN) = NormalizePeriodUnit(periodUnit);
//                var periods = n ?? defaultN;
//                var windowStart = ComputeWindowStart(unit, periods);

//                // Fetch aggregated data with the new Regex-based SortDate
//                var aggregated = await GetAggregatedStatsAsync(projectName : projectName, parentType : parentType);

//                var result = new List<SprintProgressDto>();

//                for (int p = 0; p < periods; p++)
//                {
//                    var periodStart = windowStart.AddMonths(p * bucketMonths);
//                    var periodEnd = periodStart.AddMonths(bucketMonths);

//                    // Bucketing now uses the ExtractSprintEndDate (SortDate)
//                    var inPeriod = aggregated
//                        .Where(a => a.SortDate >= periodStart && a.SortDate < periodEnd)
//                        .ToList();

//                    result.Add(new SprintProgressDto
//                    {
//                        IterationPath = unit == "quarterly" ? $"Q{((periodStart.Month - 1) / 3) + 1} {periodStart:yyyy}"
//                                        : periodStart.ToString("MMM yyyy"),
//                        TotalPointsAssigned = inPeriod.Sum(x => x.Total),
//                        TotalPointsCompleted = inPeriod.Sum(x => x.Closed),
//                        SortDate = periodStart
//                    });
//                }
//                return result;
//            }
//            catch (Exception e)
//            {
//                Console.WriteLine($"Error: {e.Message}");
//                return new List<SprintProgressDto>();
//            }
//        }

//        // Mapping helpers
//        private static List<SpillageTrendDto> ToSpillageTrendDto(List<AggregatedStat> stats)
//        {
//            return stats
//                .OrderBy(s => s.SortDate)
//                .Select(s => new SpillageTrendDto
//                {
//                    IterationPath = s.FullPath.Split('\\').Last(),
//                    SpillagePoints = s.Total - s.Closed,
//                    SortDate = s.SortDate
//                })
//                .ToList();
//        }

//        private static List<SprintProgressDto> ToSprintProgressDto(List<AggregatedStat> stats)
//        {
//            return stats
//                .Select(s => new SprintProgressDto
//                {
//                    IterationPath = s.FullPath,
//                    TotalPointsAssigned = s.Total,
//                    TotalPointsCompleted = s.Closed,
//                    SortDate = s.SortDate
//                })
//                .ToList();
//        }

//        // All Service

//        //*****
//        public async Task<List<SpillageTrendDto>> GetAllSpillageTrend(string projectName, int lastNSprints)
//        {
//            try
//            {
//                var recent = await GetRecentSprintsAsync(projectName, lastNSprints);
//                if (!recent.Any()) return new List<SpillageTrendDto>();

//                var sprintPaths = recent.Select(s => s.Path).ToList();
//                var stats = await GetAggregatedStatsAsync(iterationPaths: sprintPaths);
//                return ToSpillageTrendDto(stats);
//            }
//            catch (Exception e)
//            {
//                Console.WriteLine(e.Message);
//                return new List<SpillageTrendDto>();
//            }
//        }

//        //*******
//        public async Task<List<SprintProgressDto>> GetAllSprintStats(string projectName, int lastNSprints)
//        {
//            try
//            {
//                var recent = await GetRecentSprintsAsync(projectName, lastNSprints);
//                if (!recent.Any()) return new List<SprintProgressDto>();

//                var sprintPaths = recent.Select(s => s.Path).ToList();
//                var aggregated = await GetAggregatedStatsAsync(iterationPaths: sprintPaths);

//                var stats = ToSprintProgressDto(aggregated);

//                var finalResult = recent
//                    .OrderBy(s => s.SortDate)
//                    .Select(s => stats.FirstOrDefault(st => st.IterationPath == s.Path) ?? new SprintProgressDto
//                    {
//                        IterationPath = s.Path,
//                        TotalPointsAssigned = 0,
//                        TotalPointsCompleted = 0
//                    })
//                    .ToList();

//                return finalResult;
//            }
//            catch (Exception e)
//            {
//                Console.WriteLine(e.Message);
//                return new List<SprintProgressDto>();
//            }
//        }


//        // Feature services

//        //*****
//        public async Task<List<SpillageTrendDto>> GetFeatureSpillageTrend(string projectName, int lastNSprints)
//        {
//            try
//            {
//                var recent = await GetRecentSprintsAsync(projectName, lastNSprints);
//                if (!recent.Any()) return new List<SpillageTrendDto>();

//                var sprintPaths = recent.Select(s => s.Path).ToList();
//                var stats = await GetAggregatedStatsAsync(iterationPaths: sprintPaths, parentType: "Feature");
//                return ToSpillageTrendDto(stats);
//            }
//            catch (Exception e)
//            {
//                Console.WriteLine(e.Message);
//                return new List<SpillageTrendDto>();
//            }
//        }

//        //*****
//        public async Task<List<SprintProgressDto>> GetFeatureSprintStats(string projectName, int lastNSprints)
//        {
//            try
//            {
//                var recent = await GetRecentSprintsAsync(projectName, lastNSprints);
//                if (!recent.Any()) return new List<SprintProgressDto>();

//                var sprintPaths = recent.Select(s => s.Path).ToList();
//                var aggregated = await GetAggregatedStatsAsync(iterationPaths: sprintPaths, parentType: "Feature");
//                var stats = ToSprintProgressDto(aggregated);

//                var finalResult = recent
//                    .OrderBy(s => s.SortDate)
//                    .Select(s => stats.FirstOrDefault(st => st.IterationPath == s.Path) ?? new SprintProgressDto
//                    {
//                        IterationPath = s.Path,
//                        TotalPointsAssigned = 0,
//                        TotalPointsCompleted = 0
//                    })
//                    .ToList();

//                return finalResult;
//            }
//            catch (Exception e)
//            {
//                Console.WriteLine(e.Message);
//                return new List<SprintProgressDto>();
//            }
//        }

//        // Client services

//        //*****
//        public async Task<List<SpillageTrendDto>> GetClientSpillageTrend(string projectName, int lastNSprints)
//        {
//            try
//            {
//                var recent = await GetRecentSprintsAsync(projectName, lastNSprints);
//                if (!recent.Any()) return new List<SpillageTrendDto>();

//                var sprintPaths = recent.Select(s => s.Path).ToList();
//                var stats = await GetAggregatedStatsAsync(iterationPaths: sprintPaths, parentType: "Client Issue");
//                return ToSpillageTrendDto(stats);
//            }
//            catch (Exception e)
//            {
//                Console.WriteLine(e.Message);
//                return new List<SpillageTrendDto>();
//            }
//        }

//        //*****
//        public async Task<List<SprintProgressDto>> GetClientSprintStats(string projectName, int lastNSprints)
//        {
//            try
//            {
//                var recent = await GetRecentSprintsAsync(projectName, lastNSprints);
//                if (!recent.Any()) return new List<SprintProgressDto>();

//                var sprintPaths = recent.Select(s => s.Path).ToList();
//                var aggregated = await GetAggregatedStatsAsync(iterationPaths: sprintPaths, parentType: "Client Issue");
//                var stats = ToSprintProgressDto(aggregated);

//                var finalResult = recent
//                    .OrderBy(s => s.SortDate)
//                    .Select(s => stats.FirstOrDefault(st => st.IterationPath == s.Path) ?? new SprintProgressDto
//                    {
//                        IterationPath = s.Path,
//                        TotalPointsAssigned = 0,
//                        TotalPointsCompleted = 0
//                    })
//                    .ToList();

//                return finalResult;
//            }
//            catch (Exception e)
//            {
//                Console.WriteLine(e.Message);
//                return new List<SprintProgressDto>();
//            }
//        }

//        // Aggregate summary for last N sprints (unchanged)
//        public async Task<SpillageSummaryDto> GetSpillageSummaryLast(string projectName, int lastNSprints)
//        {
//            try
//            {
//                var statsAll = await GetAllSprintStats(projectName, lastNSprints);
//                var statsFeature = await GetFeatureSprintStats(projectName, lastNSprints);
//                var statsClient = await GetClientSprintStats(projectName, lastNSprints);

//                var trendAll = await GetAllSpillageTrend(projectName, lastNSprints);
//                var trendFeature = await GetFeatureSpillageTrend(projectName, lastNSprints);
//                var trendClient = await GetClientSpillageTrend(projectName, lastNSprints);

//                // Fetch histories per section 
//                var historyAll = await GetStoryHistoryLastNSprints(projectName, lastNSprints, null);
//                var historyFeature = await GetStoryHistoryLastNSprints(projectName, lastNSprints, "Feature");
//                var historyClient = await GetStoryHistoryLastNSprints(projectName, lastNSprints, "Client Issue");

//                var summary = new SpillageSummaryDto
//                {
//                    All = new SectionDto { Stats = statsAll, Spillage = trendAll, History = historyAll },
//                    Feature = new SectionDto { Stats = statsFeature, Spillage = trendFeature, History = historyFeature },
//                    Client = new SectionDto { Stats = statsClient, Spillage = trendClient, History = historyClient }
//                };

//                return summary;
//            }
//            catch (Exception e)
//            {
//                Console.WriteLine(e.Message);
//                return new SpillageSummaryDto();
//            }
//        }

//        // Aggregate summary for timeframe-based queries
//        public async Task<SpillageSummaryDto> GetSpillageSummaryTime(string projectName, string? periodUnit = null, int? n = null)
//        {
//            try
//            {
//                // Use new period aggregators for each section (All/Feature/Client)
//                var statsAll = await GetSprintStatsByPeriodAsync(projectName, periodUnit, n, null);
//                var statsFeature = await GetSprintStatsByPeriodAsync(projectName, periodUnit, n, "Feature");
//                var statsClient = await GetSprintStatsByPeriodAsync(projectName, periodUnit, n, "Client Issue");

//                var spillageAll = await GetSpillageByPeriodAsync(projectName, periodUnit, n, null);
//                var spillageFeature = await GetSpillageByPeriodAsync(projectName, periodUnit, n, "Feature");
//                var spillageClient = await GetSpillageByPeriodAsync(projectName, periodUnit, n, "Client Issue");


//                var (unit, _, defaultN) = NormalizePeriodUnit(periodUnit);
//                var periods = n.HasValue && n.Value > 0 ? n.Value : defaultN;
//                var windowStart = ComputeWindowStart(unit, periods);

//                var historyAll = await GetStoryHistoryByStartDate(projectName, windowStart, null);
//                var historyFeature = await GetStoryHistoryByStartDate(projectName, windowStart, "Feature");
//                var historyClient = await GetStoryHistoryByStartDate(projectName, windowStart, "Client Issue");

//                var summary = new SpillageSummaryDto
//                {
//                    All = new SectionDto { Stats = statsAll, Spillage = spillageAll, History = historyAll },
//                    Feature = new SectionDto { Stats = statsFeature, Spillage = spillageFeature, History = historyFeature },
//                    Client = new SectionDto { Stats = statsClient, Spillage = spillageClient, History = historyClient }
//                };

//                return summary;
//            }
//            catch (Exception e)
//            {
//                Console.WriteLine(e.Message);
//                return new SpillageSummaryDto();
//            }
//        }

//        // ---------- History helpers (new overload used above) ----------


//        public async Task<List<StoryHistoryDto>> GetStoryHistoryLastNSprints(string projectName, int lastNSprints, string? parentType = null)
//        {
//            try
//            {
//                var recent = await GetRecentSprintsAsync(projectName, lastNSprints);
//                if (!recent.Any()) return new List<StoryHistoryDto>();

//                var sprintPaths = recent.Select(s => s.Path).ToList();

//                var rowsQuery = _context.IvpUserStoryIterations
//                    .Join(_context.IvpUserStoryDetails,
//                          usi => usi.UserStoryId,
//                          usd => usd.UserStoryId,
//                          (usi, usd) => new { usi, usd })
//                    .Where(x => x.usi.IterationPath != null && sprintPaths.Contains(x.usi.IterationPath) &&
//                                x.usd.FirstInprogressTime != null);

//                if (!string.IsNullOrEmpty(parentType))
//                    rowsQuery = rowsQuery.Where(x => x.usd.ParentType == parentType);

//                var rows = await rowsQuery
//                    .Select(x => new
//                    {
//                        x.usd.UserStoryId,
//                        Title = x.usd.Title,
//                        State = x.usd.State,
//                        FirstInprogressTime = x.usd.FirstInprogressTime,
//                        ClosedDate = x.usd.ClosedDate,
//                        AssignedDate = x.usi.AssignedDate,
//                        IterationPath = x.usi.IterationPath
//                    })
//                    .ToListAsync();

//                if (!rows.Any()) return new List<StoryHistoryDto>();

//                var storyIds = rows.Select(r => r.UserStoryId).Distinct().ToList();

//                var counts = await _context.IvpUserStoryIterations
//                    .Where(i => storyIds.Contains(i.UserStoryId))
//                    .GroupBy(i => i.UserStoryId)
//                    .Select(g => new { UserStoryId = g.Key, Total = g.Count() })
//                    .ToDictionaryAsync(x => x.UserStoryId, x => x.Total);

//                var result = rows
//                    .Select(r => new StoryHistoryDto
//                    {
//                        UserStoryId = r.UserStoryId,
//                        Title = r.Title ?? string.Empty,
//                        State = r.State ?? string.Empty,
//                        FirstInprogressTime = r.FirstInprogressTime,
//                        ClosedDate = r.ClosedDate,
//                        AssignedDate = r.AssignedDate,
//                        IterationPath = r.IterationPath ?? string.Empty,
//                        TotalHistoryCount = counts.TryGetValue(r.UserStoryId, out var c) ? c : 0
//                    })
//                    .OrderBy(r => r.AssignedDate)
//                    .ToList();

//                return result;
//            }
//            catch (Exception e)
//            {
//                Console.WriteLine(e.Message);
//                return new List<StoryHistoryDto>();
//            }
//        }

//        public async Task<List<StoryHistoryDto>> GetStoryHistoryByTimeframe(string projectName, string timeframe, string? parentType = null)
//        {
//            try
//            {
//                var startDate = GetStartDate(timeframe);

//                return await GetStoryHistoryByStartDate(projectName, startDate, parentType);
//            }
//            catch (Exception e)
//            {
//                Console.WriteLine(e.Message);
//                return new List<StoryHistoryDto>();
//            }
//        }

//        // New: history fetch based on explicit startDate (used by period-based summary)
//        private async Task<List<StoryHistoryDto>> GetStoryHistoryByStartDate(string projectName, DateTime startDate, string? parentType = null)
//        {
//            try
//            {
//                var rowsQuery = _context.IvpUserStoryIterations
//                    .Join(_context.IvpUserStoryDetails,
//                          usi => usi.UserStoryId,
//                          usd => usd.UserStoryId,
//                          (usi, usd) => new { usi, usd })
//                    .Where(x =>
//                        x.usi.IterationPath != null &&
//                        x.usi.IterationPath.StartsWith(projectName) &&
//                        (x.usd.AreaPath == null || !x.usd.AreaPath.Contains(@"\QA")) && // Add QA Exclusion
//                        !x.usi.IterationPath.Contains("Rearch") &&
//                        x.usd.FirstInprogressTime >= startDate);

//                if (!string.IsNullOrEmpty(parentType))
//                    rowsQuery = rowsQuery.Where(x => x.usd.ParentType == parentType);

//                var rows = await rowsQuery
//                    .Select(x => new
//                    {
//                        x.usd.UserStoryId,
//                        Title = x.usd.Title,
//                        State = x.usd.State,
//                        FirstInprogressTime = x.usd.FirstInprogressTime,
//                        ClosedDate = x.usd.ClosedDate,
//                        AssignedDate = x.usi.AssignedDate,
//                        IterationPath = x.usi.IterationPath
//                    })
//                    .ToListAsync();

//                if (!rows.Any()) return new List<StoryHistoryDto>();

//                var storyIds = rows.Select(r => r.UserStoryId).Distinct().ToList();

//                var counts = await _context.IvpUserStoryIterations
//                    .Where(i => storyIds.Contains(i.UserStoryId))
//                    .GroupBy(i => i.UserStoryId)
//                    .Select(g => new { UserStoryId = g.Key, Total = g.Count() })
//                    .ToDictionaryAsync(x => x.UserStoryId, x => x.Total);

//                var result = rows
//                    .Select(r => new StoryHistoryDto
//                    {
//                        UserStoryId = r.UserStoryId,
//                        Title = r.Title ?? string.Empty,
//                        State = r.State ?? string.Empty,
//                        FirstInprogressTime = r.FirstInprogressTime,
//                        ClosedDate = r.ClosedDate,
//                        AssignedDate = r.AssignedDate,
//                        IterationPath = r.IterationPath ?? string.Empty,
//                        TotalHistoryCount = counts.TryGetValue(r.UserStoryId, out var c) ? c : 0
//                    })
//                    .OrderBy(r => r.AssignedDate)
//                    .ToList();

//                return result;
//            }
//            catch (Exception e)
//            {
//                Console.WriteLine(e.Message);
//                return new List<StoryHistoryDto>();
//            }
//        }
//    }
//}
