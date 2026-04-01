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
        private sealed record AggregatedStat(
            string FullPath,
            double Total,
            double InitialPoints,
            double AddedLater,
            double TotalClosed,
            double ClosedTimely,
            double ClosedLate,
            DateTime? SortDate);

        /// <summary>
        /// Internal DB Aggregator: Only returns data for iterations that exist in the DB.
        /// </summary>
        //    private async Task<List<AggregatedStat>> GetAggregatedStatsAsync(
        //        List<string> adoAreaPaths,
        //        Dictionary<string, (DateTime Start, DateTime End)> sprintDateMap,
        //        string? parentType = null)
        //    {
        //        var iterationPaths = sprintDateMap.Keys.ToList();

        //        var validTypes = new[] { "Feature", "Client Issue" };

        //        var query = _context.IvpUserStoryIterations
        //            .Join(_context.IvpUserStoryDetails,
        //                usi => usi.UserStoryId,
        //                usd => usd.UserStoryId,
        //                (usi, usd) => new { usi, usd })
        //            .Where(c => iterationPaths.Contains(c.usi.IterationPath) &&
        //                        adoAreaPaths.Contains(c.usd.AreaPath) &&

        //                        c.usd.ParentType != null);

        //        // 2. If parentType is "all", restrict it to ONLY your two categories
        //        if (string.IsNullOrEmpty(parentType) || string.Equals(parentType, "all", StringComparison.OrdinalIgnoreCase))
        //        {
        //            query = query.Where(c => validTypes.Contains(c.usd.ParentType));
        //        }
        //        else
        //        {
        //            // 3. Otherwise, filter by the specific type requested (Feature or Client Issue)
        //            query = query.Where(c => c.usd.ParentType.ToLower() == parentType.ToLower());
        //        }

        //        // FIX: Include AssignedDate in the select
        //        var rawData = await query.Select(x => new {
        //            x.usi.IterationPath,
        //            x.usi.AssignedDate, // Needed to check if it was added late
        //            x.usd.StoryPoints,
        //            x.usd.ClosedDate
        //        }).ToListAsync();



        //        return rawData
        //.GroupBy(x => x.IterationPath)
        //.Select(g =>
        //{
        //    var path = g.Key ?? string.Empty;
        //    if (!sprintDateMap.TryGetValue(path, out var dates))
        //        return new AggregatedStat(path, 0, 0, 0, 0, null);

        //    // 1. Get the local Start Date (Day 1)
        //    var localStart = dates.Start.ToLocalTime();
        //    var startDay = localStart.Date;

        //    var localEnd = dates.End.ToLocalTime();
        //    var nextDayThreshold = localEnd.Date.AddDays(1);

        //    // 2. INITIAL POINTS: Only stories assigned EXACTLY on the start date
        //    var initialPoints = g.Where(x => x.AssignedDate.Date == startDay)
        //                         .Sum(x => x.StoryPoints ?? 0);

        //    // 3. ADDED LATER: Stories assigned STRICTLY AFTER the start date
        //    var addedLaterPoints = g.Where(x => x.AssignedDate.Date > startDay)
        //                            .Sum(x => x.StoryPoints ?? 0);

        //    // 4. TOTAL: Initial + Added Later 
        //    // Note: This effectively ignores stories assigned BEFORE the start date.
        //    var totalPoints = initialPoints + addedLaterPoints;

        //    // 5. COMPLETED: Catching everything up to the end of the final day
        //    var completedPoints = g.Where(x => x.ClosedDate.HasValue &&
        //                                       x.ClosedDate.Value < nextDayThreshold)
        //                           .Sum(x => x.StoryPoints ?? 0);

        //    return new AggregatedStat(
        //        path,
        //        totalPoints,
        //        initialPoints,
        //        addedLaterPoints,
        //        completedPoints,
        //        dates.Start
        //    );
        //})
        //.ToList();
        //    }

        private async Task<List<AggregatedStat>> GetAggregatedStatsAsync(
    List<string> adoAreaPaths,
    Dictionary<string, (DateTime Start, DateTime End)> sprintDateMap,
    string? parentType = null)
        {
            var iterationPaths = sprintDateMap.Keys.ToList();
            var validTypes = new[] { "Feature", "Client Issue" };

            // 1. Fetch data
            var allData = await _context.IvpUserStoryIterations
                .Join(_context.IvpUserStoryDetails,
                    usi => usi.UserStoryId,
                    usd => usd.UserStoryId,
                    (usi, usd) => new { usi, usd })
                .Where(c => iterationPaths.Contains(c.usi.IterationPath) &&
                            adoAreaPaths.Contains(c.usd.AreaPath))
                .Where(c => (string.IsNullOrEmpty(parentType) || parentType.ToLower() == "all")
                            ? (c.usd.ParentType != null && validTypes.Contains(c.usd.ParentType))
                            : (c.usd.ParentType != null && c.usd.ParentType.ToLower() == parentType.ToLower()))
                .Select(x => new
                {
                    x.usi.UserStoryId,
                    x.usi.IterationPath,
                    x.usi.AssignedDate,
                    x.usd.StoryPoints,
                    x.usd.ClosedDate
                })
                .OrderBy(x => x.UserStoryId)
                .ThenBy(x => x.AssignedDate)
                .ToListAsync();

            // Updated dictionary to hold Timely vs Late
            var sprintAggregates = iterationPaths.ToDictionary(
                path => path,
                path => new { Initial = 0.0, Added = 0.0, CompletedTimely = 0.0, CompletedLate = 0.0 }
            );

            foreach (var storyGroup in allData.GroupBy(x => x.UserStoryId))
            {
                var storyHistory = storyGroup.OrderBy(x => x.AssignedDate).ToList();

                foreach (var sprintPath in iterationPaths)
                {
                    var dates = sprintDateMap[sprintPath];
                    var sStart = dates.Start.ToLocalTime().Date; // 12:00 AM Cutoff
                    var sEndMax = dates.End.ToLocalTime().Date.AddDays(1).AddTicks(-1);

                    // Membership Check
                    var stateAtPlanningEnd = storyHistory
                        .Where(us => us.AssignedDate.ToLocalTime() <= sStart)
                        .OrderByDescending(us => us.AssignedDate)
                        .FirstOrDefault();

                    bool isInitial = stateAtPlanningEnd != null &&
                                     stateAtPlanningEnd.IterationPath.Equals(sprintPath, StringComparison.OrdinalIgnoreCase);

                    bool addedMidSprint = storyHistory.Any(us =>
                        us.IterationPath.Equals(sprintPath, StringComparison.OrdinalIgnoreCase) &&
                        us.AssignedDate.ToLocalTime() > sStart &&
                        us.AssignedDate.ToLocalTime() <= sEndMax);

                    if (!(isInitial || addedMidSprint)) continue;

                    // --- COMPLETION LOGIC ---
                    // Get the absolute latest record for this story across all history to check current status
                    var absoluteLatest = storyHistory.LastOrDefault();

                    // Get the latest record WITHIN this specific sprint window
                    var latestInWindow = storyHistory
                        .Where(us => us.AssignedDate.ToLocalTime() <= sEndMax)
                        .OrderByDescending(us => us.AssignedDate)
                        .FirstOrDefault();

                    double points = latestInWindow?.StoryPoints ?? 0;
                    bool isClosedTimely = false;
                    bool isClosedLate = false;

                    if (latestInWindow != null && latestInWindow.ClosedDate.HasValue)
                    {
                        var closedTime = latestInWindow.ClosedDate.Value.ToLocalTime();

                        // 1. Completed Timely: Closed within the sprint dates
                        if (closedTime >= sStart && closedTime <= sEndMax)
                        {
                            isClosedTimely = true;
                        }
                        // 2. Completed Late: Closed after sprint end, but the story was NEVER moved to a new sprint
                        else if (closedTime > sEndMax && absoluteLatest != null &&
                                 absoluteLatest.IterationPath.Equals(sprintPath, StringComparison.OrdinalIgnoreCase))
                        {
                            isClosedLate = true;
                        }
                    }

                    var current = sprintAggregates[sprintPath];
                    sprintAggregates[sprintPath] = new
                    {
                        Initial = current.Initial + (isInitial ? points : 0),
                        Added = current.Added + (!isInitial ? points : 0),
                        CompletedTimely = current.CompletedTimely + (isClosedTimely ? points : 0),
                        CompletedLate = current.CompletedLate + (isClosedLate ? points : 0)
                    };
                }
            }

            // Map to your Final DTO
            return iterationPaths.Select(path =>
            {
                var data = sprintAggregates[path];
                return new AggregatedStat(
                    path,
                    data.Initial + data.Added,           // Total
                    data.Initial,                        // initialPoints
                    data.Added,                          // AddedLater
                    data.CompletedTimely + data.CompletedLate, // TotalClosed (Sum of both)
                    data.CompletedTimely,                // ClosedTimely
                    data.CompletedLate,                  // ClosedLate
                    sprintDateMap[path].Start            // SortDate
                );
            }).ToList();
        }

        //    private async Task<List<AggregatedStat>> GetAggregatedStatsAsync(
        //List<string> adoAreaPaths,
        //Dictionary<string, (DateTime Start, DateTime End)> sprintDateMap,
        //string? parentType = null)
        //    {
        //        var iterationPaths = sprintDateMap.Keys.ToList();

        //        // 1. IGNORE DB - Create only your test cases
        //        string s1Path = @"IVP-SRM\SPRINT 05 Jan - 23 Jan 2026";
        //        string s2Path = @"IVP-SRM\SPRINT 26 Jan - 13 Feb 2026";
        //        string backlogPath = @"IVP-SRM";

        //        var allData = new List<StoryUpdateRow>();

        //        // CASE 1: U1 ends up in S1 (Planned)
        //        allData.Add(new StoryUpdateRow { UserStoryId = 9991, IterationPath = s1Path, AssignedDate = new DateTime(2026, 1, 2), StoryPoints = 5, ClosedDate = null });
        //        allData.Add(new StoryUpdateRow { UserStoryId = 9991, IterationPath = backlogPath, AssignedDate = new DateTime(2026, 1, 3), StoryPoints = 5, ClosedDate = null });
        //        allData.Add(new StoryUpdateRow { UserStoryId = 9991, IterationPath = s1Path, AssignedDate = new DateTime(2026, 1, 4), StoryPoints = 5, ClosedDate = null });

        //        // CASE 2: U1 ends up in Backlog (Should be ignored for S1)
        //        allData.Add(new StoryUpdateRow { UserStoryId = 9992, IterationPath = s1Path, AssignedDate = new DateTime(2026, 1, 2), StoryPoints = 5, ClosedDate = null });
        //        allData.Add(new StoryUpdateRow { UserStoryId = 9992, IterationPath = backlogPath, AssignedDate = new DateTime(2026, 1, 3), StoryPoints = 5, ClosedDate = null });
        //        allData.Add(new StoryUpdateRow { UserStoryId = 9992, IterationPath = s1Path, AssignedDate = new DateTime(2026, 1, 4, 10, 0, 0), StoryPoints = 5, ClosedDate = null });
        //        allData.Add(new StoryUpdateRow { UserStoryId = 9992, IterationPath = s2Path, AssignedDate = new DateTime(2026, 1, 4, 15, 0, 0), StoryPoints = 5, ClosedDate = null });
        //        allData.Add(new StoryUpdateRow { UserStoryId = 9992, IterationPath = backlogPath, AssignedDate = new DateTime(2026, 1, 15), StoryPoints = 5, ClosedDate = null });

        //        // 2. Group the manual data
        //        var groupedByStory = allData.GroupBy(x => x.UserStoryId);

        //        var sprintAggregates = iterationPaths.ToDictionary(
        //            path => path,
        //            path => new { Initial = 0.0, Added = 0.0, Closed = 0.0 }
        //        );

        //        foreach (var storyGroup in groupedByStory)
        //        {
        //            var storyHistory = storyGroup.OrderBy(x => x.AssignedDate).ToList();

        //            foreach (var sprintPath in iterationPaths)
        //            {
        //                var dates = sprintDateMap[sprintPath];
        //                var sStart = dates.Start.ToLocalTime().Date;
        //                var sPlanningGraceEnd = sStart.AddDays(1).AddTicks(-1);

        //                // BOUNDARY: Define the exact end of the sprint (No grace period added)
        //                var sEndMax = dates.End.ToLocalTime().Date.AddDays(1).AddTicks(-1);

        //                var latestInSprintWindow = storyHistory
        //                    .Where(us => us.AssignedDate.ToLocalTime() <= sEndMax)
        //                    .OrderByDescending(us => us.AssignedDate)
        //                    .FirstOrDefault();

        //                if (latestInSprintWindow == null) continue;

        //                bool isYes = latestInSprintWindow.IterationPath.Equals(sprintPath, StringComparison.OrdinalIgnoreCase);
        //                if (!isYes) {
        //                    Console.WriteLine($"[REJECTED] ID: {storyGroup.Key} is NOT in {sprintPath} (Last known path: {latestInSprintWindow.IterationPath})");
        //                    continue;
        //                }


        //                var firstAssignedToThisSprint = storyHistory
        //                    .FirstOrDefault(us => us.IterationPath.Equals(sprintPath, StringComparison.OrdinalIgnoreCase));

        //                bool isInitial = firstAssignedToThisSprint != null &&
        //                                 firstAssignedToThisSprint.AssignedDate.ToLocalTime() <= sPlanningGraceEnd;

        //                if (isInitial)
        //                    Console.WriteLine($"[PLANNED] ID: {storyGroup.Key} in {sprintPath}");
        //                else
        //                    Console.WriteLine($"[ADDED] ID: {storyGroup.Key} in {sprintPath}");

        //                var points = latestInSprintWindow.StoryPoints ?? 0;
        //                var current = sprintAggregates[sprintPath];

        //                // STRICT COMPLETION: Check only against the end of the sprint
        //                bool isClosed = latestInSprintWindow.ClosedDate.HasValue &&
        //                                latestInSprintWindow.ClosedDate.Value.ToLocalTime() <= sEndMax;

        //                sprintAggregates[sprintPath] = new
        //                {
        //                    Initial = current.Initial + (isInitial ? points : 0),
        //                    Added = current.Added + (!isInitial ? points : 0),
        //                    Closed = current.Closed + (isClosed ? points : 0)
        //                };
        //            }
        //        }

        //        // 5. Final Mapping
        //        return iterationPaths.Select(path =>
        //        {
        //            var data = sprintAggregates[path];
        //            return new AggregatedStat(
        //                path,
        //                data.Initial + data.Added,
        //                data.Initial,
        //                data.Added,
        //                data.Closed,
        //                sprintDateMap[path].Start
        //            );
        //        }).ToList();
        //    }
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
                    // dbMatch is an 'AggregatedStat' record
                    var dbMatch = aggregated.FirstOrDefault(a => a.FullPath.Equals(s.Path, StringComparison.OrdinalIgnoreCase));

                    return new SprintProgressDto
                    {
                        IterationPath = s.Name,
                        TotalPointsAssigned = dbMatch?.Total ?? 0,

                        InitialPoints = dbMatch?.InitialPoints ?? 0,

                        MidSprintAddedPoints = dbMatch?.AddedLater ?? 0,

                        TotalPointsCompleted = dbMatch?.TotalClosed ?? 0,

                        // NEW: You can now pass these to your DTO if you updated it
                        ClosedTimely = dbMatch?.ClosedTimely ?? 0,
                        ClosedLate = dbMatch?.ClosedLate ?? 0,

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
                        SpillagePoints = (dbMatch?.Total ?? 0) - (dbMatch?.TotalClosed ?? 0),
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
        public async Task<List<ParentImpactDto>> GetImpactedParentHistoryAsync(
            List<string> adoAreaPaths,
            List<SprintDto> adoSprints,
            string? parentType = null)
        {
            try
            {
                if (adoSprints == null || !adoSprints.Any()) return new List<ParentImpactDto>();

                var sprintPaths = adoSprints.Select(s => s.Path).ToList();

                // 1. Join Iterations and Details to find spilled stories
                var rowsQuery = _context.IvpUserStoryIterations
                    .Join(_context.IvpUserStoryDetails,
                        usi => usi.UserStoryId,
                        usd => usd.UserStoryId,
                        (usi, usd) => new { usi, usd })
                    .Where(x => adoAreaPaths.Contains(x.usd.AreaPath) &&
                                sprintPaths.Contains(x.usi.IterationPath) &&
                                x.usd.ParentId != null); // Ensure there is a parent ID to group by

                // Filter by Parent Type (e.g., "Feature" or "Client Issue")
                if (!string.IsNullOrEmpty(parentType) && !string.Equals(parentType, "all", StringComparison.OrdinalIgnoreCase))
                    rowsQuery = rowsQuery.Where(x => x.usd.ParentType == parentType);

                var rows = await rowsQuery
                    .Select(x => new {
                        x.usd.UserStoryId,
                        x.usd.ParentId,
                        x.usd.State,
                        x.usi.AssignedDate,
                        x.usd.StoryPoints
                    })
                    .ToListAsync();

                if (!rows.Any()) return new List<ParentImpactDto>();

                // 2. Calculate "Spillage Hops" for each unique story
                var storyIds = rows.Select(r => r.UserStoryId).Distinct().ToList();
                var spillageCounts = await _context.IvpUserStoryIterations
                    .Where(i => storyIds.Contains(i.UserStoryId))
                    .GroupBy(i => i.UserStoryId)
                    .Select(g => new { UserStoryId = g.Key, Total = g.Count() })
                    .ToDictionaryAsync(x => x.UserStoryId, x => x.Total);

                // 3. Group by ParentId to show impacted details per Feature/Client Issue
                // 3. Group by ParentId with fixed aggregation logic
                var result = rows
                    .GroupBy(r => r.ParentId)
                    .Select(g => {
                        // Fix 1: Get unique stories to sum points correctly
                        var uniqueStories = g.GroupBy(s => s.UserStoryId)
                                             .Select(group => group.First())
                                             .ToList();

                        // Fix 2: Determine parent status by "most advanced" child state
                        var states = g.Select(x => x.State).ToList();
                        string calculatedStatus = states.Contains("In Progress") ? "In Progress" :
                                                  states.Contains("New") && states.Contains("Closed") ? "In Progress" :
                                                  states.OrderByDescending(s => s == "Closed").First();

                        return new ParentImpactDto
                        {
                            ParentId = g.Key,
                            ImpactedStoriesCount = uniqueStories.Count,
                            // Sum points only once per story ID
                            TotalPointsImpacted = uniqueStories.Sum(x => x.StoryPoints ?? 0),
                            MaxSpillageHops = g.Max(x => spillageCounts.ContainsKey(x.UserStoryId) ? spillageCounts[x.UserStoryId] : 0),
                            ParentStatus = calculatedStatus,
                            LatestAssignedDate = g.Max(x => x.AssignedDate)
                        };
                    })
                    .OrderByDescending(r => r.LatestAssignedDate)
                    .ToList();

                return result;
            }
            catch (Exception e)
            {
                Console.WriteLine($"Parent Impact Error: {e.Message}");
                return new List<ParentImpactDto>();
            }
        }
        private async Task<List<SprintDailyTrendDto>> GetDailyTrendStatsAsync(
    List<string> adoAreaPaths,
    Dictionary<string, (DateTime Start, DateTime End)> sprintDateMap,
    string? parentType = null)
        {
            var iterationPaths = sprintDateMap.Keys.ToList();
            var validTypes = new[] { "Feature", "Client Issue" };

            // --- LINE MATCH: Exact data fetch from GetAggregatedStatsAsync ---
            var allData = await _context.IvpUserStoryIterations
                .Join(_context.IvpUserStoryDetails,
                    usi => usi.UserStoryId,
                    usd => usd.UserStoryId,
                    (usi, usd) => new { usi, usd })
                .Where(c => iterationPaths.Contains(c.usi.IterationPath) &&
                            adoAreaPaths.Contains(c.usd.AreaPath))
                .Where(c => (string.IsNullOrEmpty(parentType) || parentType.ToLower() == "all")
                            ? (c.usd.ParentType != null && validTypes.Contains(c.usd.ParentType))
                            : (c.usd.ParentType != null && c.usd.ParentType.ToLower() == parentType.ToLower()))
                .Select(x => new
                {
                    x.usi.UserStoryId,
                    x.usi.IterationPath,
                    x.usi.AssignedDate,
                    x.usd.StoryPoints,
                    x.usd.ClosedDate
                })
                .OrderBy(x => x.UserStoryId)
                .ThenBy(x => x.AssignedDate)
                .ToListAsync();

            // Grouping to replicate the Membership Check
            var groupedByStory = allData.GroupBy(x => x.UserStoryId).ToList();
            var trends = new List<SprintDailyTrendDto>();

            foreach (var sprintPath in iterationPaths)
            {
                var dates = sprintDateMap[sprintPath];
                var sStart = dates.Start.ToLocalTime().Date; // 12:00 AM Cutoff
                var sEnd = dates.End.ToLocalTime().Date;

                var trend = new SprintDailyTrendDto { IterationPath = sprintPath };

                // We loop from sStart to sEnd to create the daily points
                for (var day = sStart; day <= sEnd; day = day.AddDays(1))
                {
                    // Snapshot timing: 
                    // For the first day, we want to show the 'Initial' state as defined in your stats
                    // For subsequent days, we show the state at the end of that day.
                    var snapshotTime = (day == sStart)
                        ? sStart
                        : day.AddDays(1).AddTicks(-1);

                    double dailyTotal = 0;

                    foreach (var storyGroup in groupedByStory)
                    {
                        var storyHistory = storyGroup.OrderBy(x => x.AssignedDate).ToList();

                        // --- LINE MATCH: stateAtPlanningEnd Logic ---
                        // This determines if the story belongs to this sprint at this point in time
                        var currentSnapshot = storyHistory
                            .Where(us => us.AssignedDate.ToLocalTime() <= snapshotTime)
                            .OrderByDescending(us => us.AssignedDate)
                            .FirstOrDefault();

                        if (currentSnapshot != null &&
                            currentSnapshot.IterationPath.Equals(sprintPath, StringComparison.OrdinalIgnoreCase))
                        {
                            dailyTotal += (currentSnapshot.StoryPoints ?? 0.0);
                        }
                    }

                    trend.DayByDayPoints.Add(new DailyPointDto
                    {
                        Date = day,
                        TotalPoints = dailyTotal
                    });
                }
                trends.Add(trend);
            }

            return trends;
        }

        // for monthly/quarterly timeframe
        public async Task<SpillageSummaryDto> GetAggregatedTeamStatsAsync(
    string? timeframe,
    int n,
    List<string> adoAreaPaths,
    List<SprintDto> adoSprints)
        {
            // 1. Get the timeframe boundaries (e.g., last 6 months)
            var (unit, bucketMonths, defaultN) = NormalizePeriodUnit(timeframe);
            var periods = n > 0 ? n : defaultN;
            var windowStart = ComputeWindowStart(unit, periods);

            // 2. Fetch the high-precision "Sprint-wise" data first
            // This uses your working GetAggregatedStatsAsync logic for every individual sprint
            var rawSummary = await GetFullSummaryAsync(adoAreaPaths, adoSprints);

            // 3. Define the aggregator helper
            SectionDto AggregateByStartTime(SectionDto rawSection)
            {
                var groupedSection = new SectionDto
                {
                    Stats = new List<SprintProgressDto>(),
                    Spillage = new List<SpillageTrendDto>(),
                    History = rawSection.History // Keep original history
                };

                for (int p = 0; p < periods; p++)
                {
                    // Define the Month/Quarter bucket boundaries
                    var periodStart = windowStart.AddMonths(p * bucketMonths).Date;
                    var periodEnd = periodStart.AddMonths(bucketMonths);

                    string label = unit switch
                    {
                        "quarterly" => $"Q{((periodStart.Month - 1) / 3) + 1} {periodStart:yyyy}",
                        "yearly" => periodStart.ToString("yyyy"),
                        _ => periodStart.ToString("MMM yyyy")
                    };

                    // CRITICAL FILTER: Only include sprints that STARTED in this month
                    var sprintsInThisMonth = rawSection.Stats
                        .Where(s => s.SortDate.HasValue &&
                                    s.SortDate.Value.ToLocalTime().Date >= periodStart &&
                                    s.SortDate.Value.ToLocalTime().Date < periodEnd)
                        .ToList();

                    // Match spillage trends for the same sprints
                    var spillageInThisMonth = rawSection.Spillage
                        .Where(s => s.SortDate.HasValue &&
                                    s.SortDate.Value.ToLocalTime().Date >= periodStart &&
                                    s.SortDate.Value.ToLocalTime().Date < periodEnd)
                        .ToList();

                    // Sum up the data for all sprints that started in this month
                    groupedSection.Stats.Add(new SprintProgressDto
                    {
                        IterationPath = label,
                        SortDate = periodStart,
                        InitialPoints = sprintsInThisMonth.Sum(x => x.InitialPoints),
                        MidSprintAddedPoints = sprintsInThisMonth.Sum(x => x.MidSprintAddedPoints),
                        TotalPointsAssigned = sprintsInThisMonth.Sum(x => x.TotalPointsAssigned),
                        TotalPointsCompleted = sprintsInThisMonth.Sum(x => x.TotalPointsCompleted),
                        ClosedTimely = sprintsInThisMonth.Sum(x => x.ClosedTimely),
                        ClosedLate = sprintsInThisMonth.Sum(x => x.ClosedLate)
                    });

                    groupedSection.Spillage.Add(new SpillageTrendDto
                    {
                        IterationPath = label,
                        SortDate = periodStart,
                        SpillagePoints = spillageInThisMonth.Sum(x => x.SpillagePoints)
                    });
                }
                return groupedSection;
            }

            // 4. Return the final summary grouped by Start Date
            return new SpillageSummaryDto
            {
                All = AggregateByStartTime(rawSummary.All),
                Feature = AggregateByStartTime(rawSummary.Feature),
                Client = AggregateByStartTime(rawSummary.Client)
            };
        }
        //public async Task<SpillageSummaryDto> GetAggregatedTeamStatsAsync(
        //    string? timeframe,
        //    int n,
        //    List<string> adoAreaPaths,
        //    List<SprintDto> adoSprints)
        //{
        //    // 1. Get the timeframe boundaries
        //    var (unit, bucketMonths, defaultN) = NormalizePeriodUnit(timeframe);
        //    var periods = n > 0 ? n : defaultN;
        //    var windowStart = ComputeWindowStart(unit, periods);

        //    // 2. CRITICAL: Get the "Working" Sprint-wise data first
        //    var rawSummary = await GetFullSummaryAsync(adoAreaPaths, adoSprints);

        //    var result = new SpillageSummaryDto();

        //    // 3. Helper to group the existing rawSummary sections into months
        //    SectionDto AggregateSection(SectionDto rawSection)
        //    {
        //        var groupedSection = new SectionDto
        //        {
        //            Stats = new List<SprintProgressDto>(),
        //            Spillage = new List<SpillageTrendDto>(),
        //            History = rawSection.History
        //        };

        //        for (int p = 0; p < periods; p++)
        //        {
        //            // 1. Force the bucket to start at exactly 00:00:00 Local
        //            var periodStart = windowStart.AddMonths(p * bucketMonths).Date;
        //            var periodEnd = periodStart.AddMonths(bucketMonths);

        //            string label = unit switch
        //            {
        //                "quarterly" => $"Q{((periodStart.Month - 1) / 3) + 1} {periodStart:yyyy}",
        //                "yearly" => periodStart.ToString("yyyy"),
        //                _ => periodStart.ToString("MMM yyyy")
        //            };

        //            // 2. IMPORTANT: Normalize the sprint date to Local BEFORE comparing
        //            var inBucket = rawSection.Stats
        //            .Where(s => {
        //                if (!s.SortDate.HasValue) return false;

        //                // Treat date as Local
        //                var sDate = s.SortDate.Value.Date;
        //                return sDate >= periodStart && sDate < periodEnd;
        //            })
        //            .ToList();

        //            var inBucketSpillage = rawSection.Spillage
        //                .Where(s => {
        //                    if (!s.SortDate.HasValue) return false;
        //                    var sDate = s.SortDate.Value.Date;
        //                    return sDate >= periodStart && sDate < periodEnd;
        //                })
        //                .ToList();

        //            groupedSection.Stats.Add(new SprintProgressDto
        //            {
        //                IterationPath = label,
        //                TotalPointsAssigned = inBucket.Sum(x => x.TotalPointsAssigned),
        //                MidSprintAddedPoints = inBucket.Sum(x => x.MidSprintAddedPoints),
        //                TotalPointsCompleted = inBucket.Sum(x => x.TotalPointsCompleted),
        //                SortDate = periodStart
        //            });

        //            groupedSection.Spillage.Add(new SpillageTrendDto
        //            {
        //                IterationPath = label,
        //                SpillagePoints = inBucketSpillage.Sum(x => x.SpillagePoints),
        //                SortDate = periodStart
        //            });
        //        }
        //        return groupedSection;
        //    }

        //    // 4. Map the sections inside the initializer
        //    return new SpillageSummaryDto
        //    {
        //        All = AggregateSection(rawSummary.All),
        //        Feature = AggregateSection(rawSummary.Feature),
        //        Client = AggregateSection(rawSummary.Client)
        //    };
        //}

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
            var dateMap = adoSprints.ToDictionary(
                s => s.Path,
                s => (s.Attributes.StartDate.Value, s.Attributes.FinishDate.Value),
                StringComparer.OrdinalIgnoreCase);

            return new SpillageSummaryDto
            {
                All = new SectionDto
                {
                    Stats = await GetSprintStatsAsync(areaPaths, adoSprints, "all"),
                    Spillage = await GetSpillageTrendAsync(areaPaths, adoSprints, "all"),
                    History = await GetImpactedParentHistoryAsync(areaPaths, adoSprints, "all"),
                    DailyTrends = await GetDailyTrendStatsAsync(areaPaths, dateMap, "all") // New addition
                },
                Feature = new SectionDto
                {
                    Stats = await GetSprintStatsAsync(areaPaths, adoSprints, "Feature"),
                    Spillage = await GetSpillageTrendAsync(areaPaths, adoSprints, "Feature"),
                    History = await GetImpactedParentHistoryAsync(areaPaths, adoSprints, "Feature"),
                    DailyTrends = await GetDailyTrendStatsAsync(areaPaths, dateMap, "Feature") // New addition
                },
                Client = new SectionDto
                {
                    Stats = await GetSprintStatsAsync(areaPaths, adoSprints, "Client Issue"),
                    Spillage = await GetSpillageTrendAsync(areaPaths, adoSprints, "Client Issue"),
                    History = await GetImpactedParentHistoryAsync(areaPaths, adoSprints, "Client Issue"),
                    DailyTrends = await GetDailyTrendStatsAsync(areaPaths, dateMap, "Client Issue") // New addition
                }
            };
        }

        //public async Task<List<SprintDto>> GetSprintsForTimeframeAsync(string projectId, string teamId, string? timeframe, int n)
        //{
        //    var t = timeframe?.ToLower() ?? "";

        //    // 1. Calculate how many sprints we need
        //    int multiplier = t switch
        //    {
        //        var x when x.Contains("year") => 30,    // ~30 sprints per year
        //        var x when x.Contains("quarter") => 8,  // ~8 sprints per quarter
        //        _ => 3                                  // ~3 sprints per month
        //    };

        //    int fetchCount = n * multiplier;

        //    // 2. Fetch a larger pool
        //    var allSprints = await _devops.GetRecentSprintsAsync(projectId, teamId, lastNSprints: fetchCount + 10);

        //    // 3. Filter using Local Time
        //    return allSprints
        //        .Where(s => s.Attributes.StartDate.HasValue &&
        //                    s.Attributes.StartDate.Value.Date <= DateTime.Now.Date)
        //        .OrderByDescending(s => s.Attributes.StartDate)
        //        .Take(fetchCount)
        //        .ToList();
        //}
        public async Task<List<SprintDto>> GetSprintsForTimeframeAsync(string projectId, string teamId, string? timeframe, int n)
        {
            var t = timeframe?.ToLower() ?? "";

            // 1. Calculate Multiplier
            int multiplier = t switch
            {
                var x when x.Contains("year") => 30,
                var x when x.Contains("quarter") => 10,
                _ => 4// Monthly needs ~3, Sprint-wise needs enough to cover the 'n'
            };

            // 2. Fetch a large enough raw pool (n * multiplier + buffer)
            int fetchCount = (n * multiplier) + 10;
            var allSprints = await _devops.GetRecentSprintsAsync(projectId, teamId, lastNSprints: fetchCount);

            // 3. Filter for Started Sprints only
            // This removes the "30 Mar - 17 Apr" sprint from the top
            return allSprints
                .Where(s => s.Attributes.StartDate.HasValue &&
                            s.Attributes.StartDate.Value.ToLocalTime() <= DateTime.Now.Date)
                .OrderByDescending(s => s.Attributes.StartDate)
                .ToList(); // Return the whole valid list; let the Controller 'Take(n)'
        }
    }

    public class ParentImpactDto
    {
        public int? ParentId { get; set; }
        public int ImpactedStoriesCount { get; set; }
        public double TotalPointsImpacted { get; set; }
        public int MaxSpillageHops { get; set; }
        public string ParentStatus { get; set; }
        public DateTime? LatestAssignedDate { get; set; }
    }

    public class StoryUpdateRow
    {
        public int UserStoryId { get; set; }
        public string IterationPath { get; set; }
        public DateTime AssignedDate { get; set; }
        public double? StoryPoints { get; set; }
        public DateTime? ClosedDate { get; set; }
    }

    public class SprintDailyTrendDto
    {
        public string IterationPath { get; set; }
        public List<DailyPointDto> DayByDayPoints { get; set; } = new();
    }

    public class DailyPointDto
    {
        public DateTime Date { get; set; }
        public double TotalPoints { get; set; }
    }
}