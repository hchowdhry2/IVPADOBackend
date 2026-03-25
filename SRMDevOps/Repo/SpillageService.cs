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
        private readonly IADO _devops;

        public SpillageService(IvpadodashboardContext context, IADO devops)
        {
            _context = context;
            _devops = devops;
        }

        // Private aggregate holder to return from in-memory mapping
        private sealed record AggregatedStat(string FullPath, double Total, double AddedLater, double Closed,
                 DateTime? SortDate);

        /// <summary>
        /// Internal DB Aggregator: Only returns data for iterations that exist in the DB.
        /// </summary>
        private async Task<List<AggregatedStat>> GetAggregatedStatsAsync(
            List<string> adoAreaPaths,
            Dictionary<string, (DateTime Start, DateTime End)> sprintDateMap,
            string? parentType = null)
        {
            var iterationPaths = sprintDateMap.Keys.ToList();

            var validTypes = new[] { "Feature", "Client Issue" };

            var query = _context.IvpUserStoryIterations
                .Join(_context.IvpUserStoryDetails,
                    usi => usi.UserStoryId,
                    usd => usd.UserStoryId,
                    (usi, usd) => new { usi, usd })
                .Where(c => iterationPaths.Contains(c.usi.IterationPath) &&
                            adoAreaPaths.Contains(c.usd.AreaPath) &&
                            c.usd.ParentType != null);

            // 2. If parentType is "all", restrict it to ONLY your two categories
            if (string.IsNullOrEmpty(parentType) || string.Equals(parentType, "all", StringComparison.OrdinalIgnoreCase))
            {
                query = query.Where(c => validTypes.Contains(c.usd.ParentType));
            }
            else
            {
                // 3. Otherwise, filter by the specific type requested (Feature or Client Issue)
                query = query.Where(c => c.usd.ParentType.ToLower() == parentType.ToLower());
            }

            // FIX: Include AssignedDate in the select
            var rawData = await query.Select(x => new {
                x.usi.IterationPath,
                x.usi.AssignedDate, // Needed to check if it was added late
                x.usd.StoryPoints,
                x.usd.ClosedDate
            }).ToListAsync();

            //return rawData
                //.GroupBy(x => x.IterationPath)
                //.Select(g =>
                //{
                //    var path = g.Key ?? string.Empty;
                //    if (!sprintDateMap.TryGetValue(path, out var dates))
                //        return new AggregatedStat(path, 0, 0, 0, null);

                //    // 1. FIX: Set the cutoff to the very beginning of the DAY AFTER the sprint ends
                //    // If Sprint ends March 27, this becomes March 28, 00:00:00
                //    var startLimit = dates.Start.Date;
                //    var endLimitThreshold = dates.End.Date.AddDays(1);

                //    // 2. Completed Points: Capture everything up to the last millisecond of the end date
                //    var completedPoints = g.Where(x => x.ClosedDate.HasValue &&
                //                                       x.ClosedDate.Value < endLimitThreshold) // < March 28
                //                           .Sum(x => x.StoryPoints ?? 0);

                //    // 3. Mid-Sprint Added: Capture everything that happened AFTER the start day began
                //    //var addedLaterPoints = g.Where(x => x.AssignedDate.Date > startLimit)
                //    //                        .Sum(x => x.StoryPoints ?? 0);

                //    // Capture stories assigned even one second after the sprint start
                //    var addedLaterPoints = g.Where(x => x.AssignedDate > dates.Start)
                //                            .Sum(x => x.StoryPoints ?? 0);

                //    var totalPoints = g.Sum(x => x.StoryPoints ?? 0);

                //    return new AggregatedStat(
                //        path,
                //        totalPoints,
                //        addedLaterPoints,
                //        completedPoints,
                //        dates.Start
                //    );
                //})
                //.ToList();

            return rawData
                .GroupBy(x => x.IterationPath)
                .Select(g =>
                {
                    var path = g.Key ?? string.Empty;
                    if (!sprintDateMap.TryGetValue(path, out var dates))
                        return new AggregatedStat(path, 0, 0, 0, null);

                    // Using Local Date parts for comparison
                    var startLimit = dates.Start.Date;
                    var endLimit = dates.End.Date;

                    // 1. Completed Points: Capture everything closed on or before the end date (Local)
                    var completedPoints = g.Where(x => x.ClosedDate.HasValue &&
                                                       x.ClosedDate.Value.Date <= endLimit)
                                           .Sum(x => x.StoryPoints ?? 0);

                    // 2. Mid-Sprint Added Points (Logic: AssignedDate > Sprint Start Date - Local)
                    var addedLaterPoints = g.Where(x => x.AssignedDate.Date > startLimit)
                                            .Sum(x => x.StoryPoints ?? 0);

                    // 3. Initial Points (Logic: Total minus what was added late)
                    var totalPoints = g.Sum(x => x.StoryPoints ?? 0);
                    var initialPoints = totalPoints - addedLaterPoints;

                    return new AggregatedStat(
                        path,
                        totalPoints, // Total points assigned to the sprint
                        addedLaterPoints, // Points added after the start date
                        completedPoints,
                        dates.Start
                    );
                })
                .ToList();
        }

        public async Task<List<SprintProgressDto>> GetSprintStatsAsync(List<string> adoAreaPaths, List<SprintDto> adoSprints, string? parentType = null)
        {
            try
            {
                if (adoSprints == null || !adoSprints.Any()) return new List<SprintProgressDto>();

                // Convert API Sprint list to a dictionary (Local times)
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
                                sprintPaths.Contains(x.usi.IterationPath) &&
                                x.usd.ParentType != null);

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

                // 3. Spillage Count: How many times has this story appeared in ANY iteration?
                var storyIds = rows.Select(r => r.UserStoryId).Distinct().ToList();
                var counts = await _context.IvpUserStoryIterations
                    .Where(i => storyIds.Contains(i.UserStoryId))
                    .GroupBy(i => i.UserStoryId)
                    .Select(g => new { UserStoryId = g.Key, Total = g.Count() })
                    .ToDictionaryAsync(x => x.UserStoryId, x => x.Total);

                // 4. Map to DTO
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

        // for monthly/quarterly timeframe
        public async Task<SpillageSummaryDto> GetAggregatedTeamStatsAsync(
            string? timeframe,
            int n,
            List<string> adoAreaPaths,
            List<SprintDto> adoSprints)
        {
            // 1. Get the timeframe boundaries
            var (unit, bucketMonths, defaultN) = NormalizePeriodUnit(timeframe);
            var periods = n > 0 ? n : defaultN;
            var windowStart = ComputeWindowStart(unit, periods);

            // 2. CRITICAL: Get the "Working" Sprint-wise data first
            var rawSummary = await GetFullSummaryAsync(adoAreaPaths, adoSprints);

            var result = new SpillageSummaryDto();

            // 3. Helper to group the existing rawSummary sections into months
            SectionDto AggregateSection(SectionDto rawSection)
            {
                var groupedSection = new SectionDto
                {
                    Stats = new List<SprintProgressDto>(),
                    Spillage = new List<SpillageTrendDto>(),
                    History = rawSection.History
                };

                for (int p = 0; p < periods; p++)
                {
                    // 1. Force the bucket to start at exactly 00:00:00 Local
                    var periodStart = windowStart.AddMonths(p * bucketMonths).Date;
                    var periodEnd = periodStart.AddMonths(bucketMonths);

                    string label = unit switch
                    {
                        "quarterly" => $"Q{((periodStart.Month - 1) / 3) + 1} {periodStart:yyyy}",
                        "yearly" => periodStart.ToString("yyyy"),
                        _ => periodStart.ToString("MMM yyyy")
                    };

                    // 2. IMPORTANT: Normalize the sprint date to Local BEFORE comparing
                    var inBucket = rawSection.Stats
                    .Where(s => {
                        if (!s.SortDate.HasValue) return false;

                        // Treat date as Local
                        var sDate = s.SortDate.Value.Date;
                        return sDate >= periodStart && sDate < periodEnd;
                    })
                    .ToList();

                    var inBucketSpillage = rawSection.Spillage
                        .Where(s => {
                            if (!s.SortDate.HasValue) return false;
                            var sDate = s.SortDate.Value.Date;
                            return sDate >= periodStart && sDate < periodEnd;
                        })
                        .ToList();

                    groupedSection.Stats.Add(new SprintProgressDto
                    {
                        IterationPath = label,
                        TotalPointsAssigned = inBucket.Sum(x => x.TotalPointsAssigned),
                        MidSprintAddedPoints = inBucket.Sum(x => x.MidSprintAddedPoints),
                        TotalPointsCompleted = inBucket.Sum(x => x.TotalPointsCompleted),
                        SortDate = periodStart
                    });

                    groupedSection.Spillage.Add(new SpillageTrendDto
                    {
                        IterationPath = label,
                        SpillagePoints = inBucketSpillage.Sum(x => x.SpillagePoints),
                        SortDate = periodStart
                    });
                }
                return groupedSection;
            }

            // 4. Map the sections inside the initializer
            return new SpillageSummaryDto
            {
                All = AggregateSection(rawSummary.All),
                Feature = AggregateSection(rawSummary.Feature),
                Client = AggregateSection(rawSummary.Client)
            };
        }

        private (string unit, int bucketMonths, int defaultN) NormalizePeriodUnit(string? unit)
        {
            var u = unit?.Trim().ToLowerInvariant();
            return u switch
            {
                "quarter" or "quarterly" => ("quarterly", 3, 4),
                "year" or "yearly" => ("yearly", 12, 1),
                _ => ("monthly", 1, 6)
            };
        }

        private DateTime ComputeWindowStart(string unit, int nPeriods)
        {
            var now = DateTime.Now; // Use Local machine time
            var bucketMonths = unit == "quarterly" ? 3 : unit == "yearly" ? 12 : 1;

            // Create as Local
            return new DateTime(now.Year, now.Month, 1, 0, 0, 0, DateTimeKind.Local)
                        .AddMonths(-((nPeriods * bucketMonths) - 1));
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

        public async Task<List<SprintDto>> GetSprintsForTimeframeAsync(string projectId, string teamId, string? timeframe, int n)
        {
            var t = timeframe?.ToLower() ?? "";

            // 1. Calculate how many sprints we need
            int multiplier = t switch
            {
                var x when x.Contains("year") => 30,    // ~30 sprints per year
                var x when x.Contains("quarter") => 8,  // ~8 sprints per quarter
                _ => 3                                  // ~3 sprints per month
            };

            int fetchCount = n * multiplier;

            // 2. Fetch a larger pool
            var allSprints = await _devops.GetRecentSprintsAsync(projectId, teamId, lastNSprints: fetchCount + 10);

            // 3. Filter using Local Time
            return allSprints
                .Where(s => s.Attributes.StartDate.HasValue &&
                            s.Attributes.StartDate.Value.Date <= DateTime.Now.Date)
                .OrderByDescending(s => s.Attributes.StartDate)
                .Take(fetchCount)
                .ToList();
        }
    }
}