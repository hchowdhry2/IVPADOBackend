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

        private sealed record AggregatedStat(
            string FullPath,
            double Total,
            double InitialPoints,
            double AddedLater,
            double TotalClosed,
            double ClosedTimely,
            double ClosedLate,
            DateTime? SortDate);

        // Private aggregate holder to return from in-memory mapping
        private class UnifiedWorkItem
        {
            public int Id { get; set; }
            public int? ParentId { get; set; }
            public string IterationPath { get; set; }
            public DateTime AssignedDate { get; set; }
            public double Value { get; set; }
            public DateTime? ClosedDate { get; set; }
            public string? ParentType { get; set; }
            public string? State { get; set; } // Added to help determine Parent Status in impact analysis

            public string? AssignedTo { get; set; }
            public decimal? DevEffort { get; set; }
            public decimal? InitialEffort { get; set; }
            public string Activity { get; set; }

        }


        private async Task<List<UnifiedWorkItem>> GetUserStoryDataAsync(List<string> iterationPaths, List<string> adoAreaPaths)
        {
            return await _context.IvpUserStoryIterations
                .Join(_context.IvpUserStoryDetails, usi => usi.UserStoryId, usd => usd.UserStoryId, (usi, usd) => new { usi, usd })
                .Where(c => iterationPaths.Contains(c.usi.IterationPath) && adoAreaPaths.Contains(c.usd.AreaPath))
                .Select(x => new UnifiedWorkItem
                {
                    Id = x.usi.UserStoryId,
                    ParentId = x.usd.ParentId, // Capture ParentId for later use in Task mapping
                    IterationPath = x.usi.IterationPath,
                    AssignedDate = x.usi.AssignedDate,
                    AssignedTo = null, // Stories don't use this for the dev report
                    DevEffort = 0,
                    Value = x.usd.StoryPoints ?? 0.0,
                    ClosedDate = x.usd.ClosedDate,
                    ParentType = x.usd.ParentType,
                    State = x.usd.State // Capture current state for impact analysis
                })
                .OrderBy(x => x.Id).ThenBy(x => x.AssignedDate)
                .ToListAsync();
        }

        private async Task<List<UnifiedWorkItem>> GetTaskDataAsync(List<string> iterationPaths, List<string> adoAreaPaths)
        {
            // Tasks get AreaPath AND ParentType from the Parent Story table
            return await _context.IvpTaskIterations
                .Join(_context.IvpTaskDetails, ti => ti.TaskId, td => td.TaskId, (ti, td) => new { ti, td })
                .Join(_context.IvpUserStoryDetails, c => c.td.UserStoryId, usd => usd.UserStoryId, (c, usd) => new { c.ti, c.td, usd })
                // for developer level stats
                .Join(_context.IvpTaskAssignees, x => x.ti.TaskId, ta => ta.TaskId, (x, ta) => new { x.ti, x.td, x.usd, ta })
                .Where(x => iterationPaths.Contains(x.ti.IterationPath) && adoAreaPaths.Contains(x.usd.AreaPath))
                .Select(x => new UnifiedWorkItem
                {
                    Id = x.ti.TaskId,
                    ParentId = x.td.UserStoryId, // Link Task back to its User Story's ParentId
                    IterationPath = x.ti.IterationPath,
                    AssignedDate = x.ti.AssignedDate,
                    AssignedTo = x.ta.AssignedTo,    // Now this will work!
                    DevEffort = x.td.DevEffort,
                    Value = 1.0, // COUNTING logic
                    ClosedDate = x.td.ClosedDate,
                    ParentType = x.usd.ParentType, // Pulling ParentType from Story table
                    State = x.td.State, // Capture Task state for impact analysis
                    InitialEffort = x.td.IntialEffort,
                    Activity = x.td.Activity
                })
                .OrderBy(x => x.Id).ThenBy(x => x.AssignedDate)
                .ToListAsync();
        }

        /// <summary>
        /// Internal DB Aggregator: Only returns data for iterations that exist in the DB.
        /// </summary>

        //    private async Task<List<AggregatedStat>> GetAggregatedStatsAsync(
        //List<string> adoAreaPaths,
        //Dictionary<string, (DateTime Start, DateTime End)> sprintDateMap,
        //string? parentType = null,
        //bool isTask = false)

        private List<AggregatedStat> GetAggregatedStatsFromMemory(
            List<UnifiedWorkItem> allData,
            List<string> iterationPaths,
            Dictionary<string, (DateTime Start, DateTime End)> sprintDateMap,
            string? parentType = null)
        {
            var validTypes = new[] { "Feature", "Client Issue" };

            // Filter in-memory instead of DB
            var filteredData = allData
                .Where(c => (string.IsNullOrEmpty(parentType) || parentType.ToLower() == "all")
                            ? (c.ParentType != null && validTypes.Contains(c.ParentType))
                            : (c.ParentType != null && c.ParentType.ToLower() == parentType.ToLower()))
                .ToList();

            var sprintAggregates = iterationPaths.ToDictionary(
                path => path,
                path => new { Initial = 0.0, Added = 0.0, CompletedTimely = 0.0, CompletedLate = 0.0 }
            );

            //main logic
            foreach (var storyGroup in filteredData.GroupBy(x => x.Id))
            {
                var storyHistory = storyGroup.OrderBy(x => x.AssignedDate).ToList();
                foreach (var sprintPath in iterationPaths)
                {
                    var dates = sprintDateMap[sprintPath];
                    var sStart = dates.Start.ToLocalTime().Date;
                    var sEndMax = dates.End.ToLocalTime().Date.AddDays(1).AddTicks(-1);

                    var stateAtPlanningEnd = storyHistory
                        .Where(us => us.AssignedDate.ToLocalTime() <= sStart)
                        .OrderByDescending(us => us.AssignedDate)
                        .FirstOrDefault();

                    bool isInitial = stateAtPlanningEnd != null && stateAtPlanningEnd.IterationPath.Equals(sprintPath, StringComparison.OrdinalIgnoreCase);
                    bool addedMidSprint = storyHistory.Any(us => us.IterationPath.Equals(sprintPath, StringComparison.OrdinalIgnoreCase) && us.AssignedDate.ToLocalTime() > sStart && us.AssignedDate.ToLocalTime() <= sEndMax);

                    if (!(isInitial || addedMidSprint)) continue;

                    var absoluteLatest = storyHistory.LastOrDefault();
                    var latestInWindow = storyHistory.Where(us => us.AssignedDate.ToLocalTime() <= sEndMax).OrderByDescending(us => us.AssignedDate).FirstOrDefault();

                    double points = latestInWindow?.Value ?? 0;
                    bool isClosedTimely = false;
                    bool isClosedLate = false;

                    if (latestInWindow != null && latestInWindow.ClosedDate.HasValue)
                    {
                        var closedTime = latestInWindow.ClosedDate.Value.ToLocalTime();
                        if (closedTime >= sStart && closedTime <= sEndMax) isClosedTimely = true;
                        else if (closedTime > sEndMax && absoluteLatest != null && absoluteLatest.IterationPath.Equals(sprintPath, StringComparison.OrdinalIgnoreCase)) isClosedLate = true;
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

            return iterationPaths.Select(path => {
                var data = sprintAggregates[path];
                return new AggregatedStat(path, data.Initial + data.Added, data.Initial, data.Added, data.CompletedTimely + data.CompletedLate, data.CompletedTimely, data.CompletedLate, sprintDateMap[path].Start);
            }).ToList();
        }

        //private async Task<List<AggregatedStat>> GetAggregatedStatsAsync(
        //    List<string> adoAreaPaths,
        //    Dictionary<string, (DateTime Start, DateTime End)> sprintDateMap,
        //    string? parentType = null,
        //    bool isTask = false)
        //{
        //    var iterationPaths = sprintDateMap.Keys.ToList();
        //    var validTypes = new[] { "Feature", "Client Issue" };

        //    Console.WriteLine("-------------------------------------");
        //    foreach(var area in adoAreaPaths)
        //    {
        //        Console.WriteLine(area);
        //    }

        //    Console.WriteLine("-------------------------------------");

        //    var taskData = _context.IvpTaskIterations
        //    .Join(_context.IvpTaskDetails, ti => ti.TaskId, td => td.TaskId, (ti, td) => new { ti, td })
        //    .Join(_context.IvpUserStoryDetails, c => c.td.UserStoryId, usd => usd.UserStoryId, (c, usd) => new { ti = c.ti, td = c.td, usd })
        //    .Where(x => adoAreaPaths.Contains(x.usd.AreaPath)) // Join to get AreaPath
        //    .Select(x => new
        //    {
        //        Id = x.ti.TaskId,
        //        x.ti.IterationPath,
        //        x.ti.AssignedDate,
        //        Value = 1.0, // COUNTING for tasks
        //        x.td.ClosedDate,
        //        ParentType = x.usd.ParentType
        //    });



        //    // 1. Fetch data
        //    var allData = await _context.IvpUserStoryIterations
        //        .Join(_context.IvpUserStoryDetails,
        //            usi => usi.UserStoryId,
        //            usd => usd.UserStoryId,
        //            (usi, usd) => new { usi, usd })
        //        .Where(c => iterationPaths.Contains(c.usi.IterationPath) &&
        //                    adoAreaPaths.Contains(c.usd.AreaPath))
        //        .Where(c => (string.IsNullOrEmpty(parentType) || parentType.ToLower() == "all")
        //                    ? (c.usd.ParentType != null && validTypes.Contains(c.usd.ParentType))
        //                    : (c.usd.ParentType != null && c.usd.ParentType.ToLower() == parentType.ToLower()))
        //        .Select(x => new
        //        {
        //            x.usi.UserStoryId,
        //            x.usi.IterationPath,
        //            x.usi.AssignedDate,
        //            x.usd.StoryPoints,
        //            x.usd.ClosedDate
        //        })
        //        .OrderBy(x => x.UserStoryId)
        //        .ThenBy(x => x.AssignedDate)
        //        .ToListAsync();


        //        // Updated dictionary to hold Timely vs Late
        //        var sprintAggregates = iterationPaths.ToDictionary(
        //            path => path,
        //            path => new { Initial = 0.0, Added = 0.0, CompletedTimely = 0.0, CompletedLate = 0.0 }
        //        );

        //            foreach (var storyGroup in allData.GroupBy(x => x.UserStoryId))
        //            {
        //                var storyHistory = storyGroup.OrderBy(x => x.AssignedDate).ToList();

        //                foreach (var sprintPath in iterationPaths)
        //                {
        //                    var dates = sprintDateMap[sprintPath];
        //        var sStart = dates.Start.ToLocalTime().Date; // 12:00 AM Cutoff
        //        var sEndMax = dates.End.ToLocalTime().Date.AddDays(1).AddTicks(-1);

        //        // Membership Check
        //        var stateAtPlanningEnd = storyHistory
        //            .Where(us => us.AssignedDate.ToLocalTime() <= sStart)
        //            .OrderByDescending(us => us.AssignedDate)
        //            .FirstOrDefault();

        //        bool isInitial = stateAtPlanningEnd != null &&
        //                         stateAtPlanningEnd.IterationPath.Equals(sprintPath, StringComparison.OrdinalIgnoreCase);

        //        bool addedMidSprint = storyHistory.Any(us =>
        //            us.IterationPath.Equals(sprintPath, StringComparison.OrdinalIgnoreCase) &&
        //            us.AssignedDate.ToLocalTime() > sStart &&
        //            us.AssignedDate.ToLocalTime() <= sEndMax);

        //                    if (!(isInitial || addedMidSprint)) continue;

        //                    // --- COMPLETION LOGIC ---
        //                    // Get the absolute latest record for this story across all history to check current status
        //                    var absoluteLatest = storyHistory.LastOrDefault();

        //        // Get the latest record WITHIN this specific sprint window
        //        var latestInWindow = storyHistory
        //            .Where(us => us.AssignedDate.ToLocalTime() <= sEndMax)
        //            .OrderByDescending(us => us.AssignedDate)
        //            .FirstOrDefault();

        //        double points = latestInWindow?.StoryPoints ?? 0;
        //        bool isClosedTimely = false;
        //        bool isClosedLate = false;

        //                    if (latestInWindow != null && latestInWindow.ClosedDate.HasValue)
        //                    {
        //                        var closedTime = latestInWindow.ClosedDate.Value.ToLocalTime();

        //                        // 1. Completed Timely: Closed within the sprint dates
        //                        if (closedTime >= sStart && closedTime <= sEndMax)
        //                        {
        //                            isClosedTimely = true;
        //                        }
        //                        // 2. Completed Late: Closed after sprint end, but the story was NEVER moved to a new sprint
        //                        else if (closedTime > sEndMax && absoluteLatest != null &&
        //                                 absoluteLatest.IterationPath.Equals(sprintPath, StringComparison.OrdinalIgnoreCase))
        //                        {
        //                            isClosedLate = true;
        //                        }
        //                    }

        //                    var current = sprintAggregates[sprintPath];
        //sprintAggregates[sprintPath] = new
        //{
        //    Initial = current.Initial + (isInitial ? points : 0),
        //    Added = current.Added + (!isInitial ? points : 0),
        //    CompletedTimely = current.CompletedTimely + (isClosedTimely ? points : 0),
        //    CompletedLate = current.CompletedLate + (isClosedLate ? points : 0)
        //};
        //                }
        //            }

        //            // Map to your Final DTO
        //            return iterationPaths.Select(path =>
        //            {
        //                var data = sprintAggregates[path];
        //                return new AggregatedStat(
        //                    path,
        //                    data.Initial + data.Added,           // Total
        //                    data.Initial,                        // initialPoints
        //                    data.Added,                          // AddedLater
        //                    data.CompletedTimely + data.CompletedLate, // TotalClosed (Sum of both)
        //                    data.CompletedTimely,                // ClosedTimely
        //                    data.CompletedLate,                  // ClosedLate
        //                    sprintDateMap[path].Start            // SortDate
        //                );
        //            }).ToList();
        //}

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
        //public async Task<List<SprintProgressDto>> GetSprintStatsAsync(List<string> adoAreaPaths, List<SprintDto> adoSprints, string? parentType = null, bool isTask = false)
        private List<SprintProgressDto> GetSprintStatsFromMemory(List<UnifiedWorkItem> data, List<SprintDto> adoSprints, Dictionary<string, (DateTime Start, DateTime End)> dateMap, string parentType)
        {
            var aggregated = GetAggregatedStatsFromMemory(data, dateMap.Keys.ToList(), dateMap, parentType);
            return adoSprints.Select(s => {
                var dbMatch = aggregated.FirstOrDefault(a => a.FullPath.Equals(s.Path, StringComparison.OrdinalIgnoreCase));
                return new SprintProgressDto
                {
                    IterationPath = s.Name,
                    TotalPointsAssigned = dbMatch?.Total ?? 0,
                    InitialPoints = dbMatch?.InitialPoints ?? 0,
                    MidSprintAddedPoints = dbMatch?.AddedLater ?? 0,
                    TotalPointsCompleted = dbMatch?.TotalClosed ?? 0,
                    ClosedTimely = dbMatch?.ClosedTimely ?? 0,
                    ClosedLate = dbMatch?.ClosedLate ?? 0,
                    SortDate = s.Attributes.StartDate
                };
            }).OrderBy(x => x.SortDate).ToList();
        }
        // CHANGE: Rename to 'FromMemory' and accept List<UnifiedWorkItem>
        private List<SpillageTrendDto> GetSpillageTrendFromMemory(
            List<UnifiedWorkItem> sectionData,
            List<SprintDto> adoSprints,
            string? parentType = null)
        {
            try
            {
                if (adoSprints == null || !adoSprints.Any()) return new List<SpillageTrendDto>();

                var dateMap = adoSprints.ToDictionary(
                    s => s.Path,
                    s => (s.Attributes.StartDate.Value, s.Attributes.FinishDate.Value),
                    StringComparer.OrdinalIgnoreCase
                );

                // FIX: Call the new memory-based method we created (No await needed)
                var aggregated = GetAggregatedStatsFromMemory(sectionData, dateMap.Keys.ToList(), dateMap, parentType);

                return adoSprints.Select(s => {
                    var dbMatch = aggregated.FirstOrDefault(a => a.FullPath.Equals(s.Path, StringComparison.OrdinalIgnoreCase));
                    return new SpillageTrendDto
                    {
                        IterationPath = s.Name,
                        // Logic remains the same: Total points minus closed points
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
        // Inside SpillageService.cs
        private List<ParentImpactDto> GetImpactedParentHistoryFromMemory(List<UnifiedWorkItem> data, Dictionary<int, string> titleMap)
        {
            if (!data.Any()) return new List<ParentImpactDto>();

            var validData = data.Where(x => x.ParentId.HasValue).ToList();
            if (!validData.Any()) return new List<ParentImpactDto>();

            var transitionMap = data.GroupBy(h => h.Id).ToDictionary(
                g => g.Key,
                g => {
                    var list = g.OrderBy(x => x.AssignedDate).ToList();
                    int transitions = 0;
                    for (int i = 1; i < list.Count; i++)
                        if (!list[i].IterationPath.Equals(list[i - 1].IterationPath, StringComparison.OrdinalIgnoreCase)) transitions++;
                    return transitions;
                });

            return data.Where(x => x.ParentId != null).GroupBy(s => s.ParentId).Select(g => {
                int pId = g.Key.Value;
                return new ParentImpactDto
                {
                    ParentId = pId,
                    ParentTitle = titleMap.GetValueOrDefault(pId, $"ID #{pId}"),
                    ParentStatus = g.Any(s => s.State == "In Progress" || s.State == "New" || s.State == "Pending QA Ready Validation") ? "In Progress" : g.All(s => s.State == "Closed") ? "Closed" : g.First().State,
                    TotalStoryCount = g.Select(x => x.Id).Distinct().Count(),
                    TotalImpactScore = g.Select(x => x.Id).Distinct().Sum(id => transitionMap[id])
                };
            }).OrderByDescending(r => r.TotalImpactScore).ToList();
        }
        private List<SprintDailyTrendDto> GetDailyTrendStatsFromMemory(List<UnifiedWorkItem> data, Dictionary<string, (DateTime Start, DateTime End)> sprintDateMap, bool isTask)
        {
            var iterationPaths = sprintDateMap.Keys.ToList();
            var groupedByStory = data.GroupBy(x => x.Id).ToList();
            var trends = new List<SprintDailyTrendDto>();

            foreach (var sprintPath in iterationPaths)
            {
                var dates = sprintDateMap[sprintPath];
                var sStart = dates.Start.ToLocalTime().Date;
                var sEnd = dates.End.ToLocalTime().Date;
                var trend = new SprintDailyTrendDto { IterationPath = sprintPath };

                for (var day = sStart; day <= sEnd; day = day.AddDays(1))
                {
                    var snapshotTime = (day == sStart) ? sStart : day.AddDays(1).AddTicks(-1);
                    double dailyTotal = 0;
                    foreach (var storyGroup in groupedByStory)
                    {
                        var currentSnapshot = storyGroup.Where(us => us.AssignedDate.ToLocalTime() <= snapshotTime).OrderByDescending(us => us.AssignedDate).FirstOrDefault();
                        if (currentSnapshot != null && currentSnapshot.IterationPath.Equals(sprintPath, StringComparison.OrdinalIgnoreCase))
                            dailyTotal += currentSnapshot.Value;
                    }
                    trend.DayByDayPoints.Add(new DailyPointDto { Date = day, TotalPoints = dailyTotal });
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
            List<SprintDto> adoSprints,
            string projectId,
            bool isTask = false)
        {
            // 1. Get the timeframe boundaries
            var (unit, bucketMonths, defaultN) = NormalizePeriodUnit(timeframe);
            var periods = n > 0 ? n : defaultN;
            var windowStart = ComputeWindowStart(unit, periods);

            // 2. Fetch the high-precision "Sprint-wise" data
            var rawSummary = await GetFullSummaryAsync(adoAreaPaths, adoSprints, projectId, isTask);

            // 3. Define the aggregator helper

            SectionDto AggregateByStartTime(SectionDto rawSection)
            {
                var groupedSection = new SectionDto
                {
                    Stats = new List<SprintProgressDto>(),
                    Spillage = new List<SpillageTrendDto>(),
                    DeveloperStats = new List<DeveloperSprintStatDto>(),
                    EffortVariance = new List<EffortVarianceDto>(),
                    DeveloperActivityStats = new List<DeveloperSprintActivityDto>(),
                    History = rawSection.History
                };

                var sprintDateLookup = adoSprints
                    .Where(s => s.Attributes.StartDate.HasValue)
                    .ToDictionary(s => s.Path, s => s.Attributes.StartDate.Value.Date, StringComparer.OrdinalIgnoreCase);

                for (int p = 0; p < periods; p++)
                {
                    var periodStart = windowStart.AddMonths(p * bucketMonths).Date;
                    var periodEnd = periodStart.AddMonths(bucketMonths).Date;

                    string label = unit switch
                    {
                        "quarterly" => $"Q{((periodStart.Month - 1) / 3) + 1} {periodStart:yyyy}",
                        "yearly" => periodStart.ToString("yyyy"),
                        _ => periodStart.ToString("MMM yyyy")
                    };

                    // --- 1. Stats & Spillage ---
                    // Use .Date directly to avoid TimeZone shifts during comparison
                    var sprintsInPeriod = rawSection.Stats
                        .Where(s => s.SortDate.HasValue && s.SortDate.Value.Date >= periodStart && s.SortDate.Value.Date < periodEnd)
                        .ToList();

                    if (sprintsInPeriod.Any())
                    {
                        groupedSection.Stats.Add(new SprintProgressDto
                        {
                            IterationPath = label,
                            SortDate = periodStart,
                            InitialPoints = sprintsInPeriod.Sum(x => x.InitialPoints),
                            MidSprintAddedPoints = sprintsInPeriod.Sum(x => x.MidSprintAddedPoints),
                            TotalPointsAssigned = sprintsInPeriod.Sum(x => x.TotalPointsAssigned),
                            TotalPointsCompleted = sprintsInPeriod.Sum(x => x.TotalPointsCompleted),
                            ClosedTimely = sprintsInPeriod.Sum(x => x.ClosedTimely),
                            ClosedLate = sprintsInPeriod.Sum(x => x.ClosedLate)
                        });

                        // Spillage needs to match the same sprints
                        var spillageInPeriod = rawSection.Spillage
                            .Where(s => s.SortDate.HasValue && s.SortDate.Value.Date >= periodStart && s.SortDate.Value.Date < periodEnd)
                            .ToList();

                        groupedSection.Spillage.Add(new SpillageTrendDto
                        {
                            IterationPath = label,
                            SortDate = periodStart,
                            SpillagePoints = spillageInPeriod.Sum(x => x.SpillagePoints)
                        });
                    }

                    // --- 2. Developer Stats ---
                    var devStatsInPeriod = rawSection.DeveloperStats
                        .Where(ds => sprintDateLookup.TryGetValue(ds.Sprint, out var start) && start >= periodStart && start < periodEnd)
                        .GroupBy(ds => ds.Developer)
                        .Select(g => new DeveloperSprintStatDto
                        {
                            Sprint = label,
                            Developer = g.Key,
                            TotalTasksAssigned = g.Sum(x => x.TotalTasksAssigned),
                            TotalTasksCompleted = g.Sum(x => x.TotalTasksCompleted),
                            TotalHours = g.Sum(x => x.TotalHours)
                        }).ToList();
                    groupedSection.DeveloperStats.AddRange(devStatsInPeriod);

                    // --- 3. Effort Variance ---
                    var effortInPeriod = rawSection.EffortVariance
                        .Where(ev => ev.SortDate.HasValue && ev.SortDate.Value.Date >= periodStart && ev.SortDate.Value.Date < periodEnd)
                        .GroupBy(ev => ev.Developer)
                        .Select(g => new EffortVarianceDto
                        {
                            Sprint = label,
                            Developer = g.Key,
                            CommittedEffort = g.Sum(x => x.CommittedEffort),
                            ActualEffort = g.Sum(x => x.ActualEffort),
                            SortDate = periodStart
                        }).ToList();
                    groupedSection.EffortVariance.AddRange(effortInPeriod);

                    // --- 4. Aggregate Developer Activity Stats ---
                    var activitiesInPeriod = rawSection.DeveloperActivityStats
                        .Where(s => s.SortDate.Date >= periodStart && s.SortDate.Date < periodEnd) // Simplified comparison
                        .SelectMany(s => s.Activities.Select(a => new { Dev = s.Developer, Data = a }))
                        .GroupBy(x => new { x.Dev, x.Data.ActivityType })
                        .Select(g => new {
                            g.Key.Dev,
                            g.Key.ActivityType,
                            Total = g.Sum(x => x.Data.Total),
                            Completed = g.Sum(x => x.Data.Completed)
                        })
                        .GroupBy(x => x.Dev)
                        .Select(devGroup => new DeveloperSprintActivityDto
                        {
                            Developer = devGroup.Key,
                            PeriodLabel = label, // e.g. "Q1 2026"
                            SortDate = periodStart,
                            Activities = devGroup.Select(x => new ActivityDetailDto
                            {
                                ActivityType = x.ActivityType,
                                Total = x.Total,
                                Completed = x.Completed
                            }).ToList()
                        }).ToList();

                    // CRITICAL: Ensure this line exists!
                    groupedSection.DeveloperActivityStats.AddRange(activitiesInPeriod);
                }

                return groupedSection;
            }

            // 4. Return final summary
            return new SpillageSummaryDto
            {
                All = AggregateByStartTime(rawSummary.All),
                Feature = AggregateByStartTime(rawSummary.Feature),
                Client = AggregateByStartTime(rawSummary.Client)
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
            // Use a consistent base date (e.g., First of current month)
            var now = DateTime.Now;
            var baseDate = new DateTime(now.Year, now.Month, 1, 0, 0, 0, DateTimeKind.Local);

            int bucketMonths = unit switch
            {
                "quarterly" => 3,
                "yearly" => 12,
                _ => 1 // monthly
            };

            // Calculate total months to look back
            int totalMonths = nPeriods * bucketMonths;

            // Calculate the start of the window
            var windowStart = baseDate.AddMonths(-totalMonths + bucketMonths);

            // If Quarterly, align to the start of that quarter
            if (unit == "quarterly")
            {
                int monthOffset = (windowStart.Month - 1) % 3;
                windowStart = windowStart.AddMonths(-monthOffset);
            }

            return windowStart;
        }

        //public async Task<SpillageSummaryDto> GetFullSummaryAsync(List<string> areaPaths, List<SprintDto> adoSprints, string projectId, bool isTask = false)
        //{
        //    var dateMap = adoSprints.ToDictionary(
        //        s => s.Path,
        //        s => (s.Attributes.StartDate.Value, s.Attributes.FinishDate.Value),
        //        StringComparer.OrdinalIgnoreCase);

        //    var sprintPaths = adoSprints.Select(s => s.Path).ToList();

        //    // 1. Find every unique ParentId that appears in these sprints
        //    var allParentIds = await _context.IvpUserStoryIterations
        //        .Join(_context.IvpUserStoryDetails, usi => usi.UserStoryId, usd => usd.UserStoryId, (usi, usd) => usd)
        //        .Where(usd => areaPaths.Contains(usd.AreaPath) && usd.ParentId != null)
        //        .Select(usd => usd.ParentId.Value)
        //        .Distinct()
        //        .ToListAsync();

        //    // 2. Fetch all titles in ONE single API call
        //    // 2. Fetch all titles in ONE single API call
        //    Console.WriteLine($"[DEBUG] Fetching titles for {allParentIds.Count} parents using Project: {projectId}");
        //    var titleMap = await _devops.GetWorkItemTitlesBatchAsync(projectId, allParentIds);
        //    Console.WriteLine($"[DEBUG] Successfully mapped {titleMap.Count} titles.");

        //    return new SpillageSummaryDto
        //    {
        //        All = new SectionDto
        //        {
        //            Stats = await GetSprintStatsAsync(areaPaths, adoSprints, "all", isTask),
        //            Spillage = await GetSpillageTrendAsync(areaPaths, adoSprints, "all", isTask),
        //            History = await GetImpactedParentHistoryAsync(areaPaths, adoSprints, titleMap, "all"),
        //            // FIX: Pass isTask here
        //            DailyTrends = await GetDailyTrendStatsAsync(areaPaths, dateMap, "all", isTask)
        //        },
        //        Feature = new SectionDto
        //        {
        //            Stats = await GetSprintStatsAsync(areaPaths, adoSprints, "Feature", isTask),
        //            Spillage = await GetSpillageTrendAsync(areaPaths, adoSprints, "Feature", isTask),
        //            History = await GetImpactedParentHistoryAsync(areaPaths, adoSprints, titleMap, "Feature"),

        //            DailyTrends = await GetDailyTrendStatsAsync(areaPaths, dateMap, "Feature", isTask)
        //        },
        //        Client = new SectionDto
        //        {
        //            Stats = await GetSprintStatsAsync(areaPaths, adoSprints, "Client Issue", isTask),
        //            Spillage = await GetSpillageTrendAsync(areaPaths, adoSprints, "Client Issue", isTask),
        //            History = await GetImpactedParentHistoryAsync(areaPaths, adoSprints, titleMap, "Client Issue"),
        //            // FIX: Pass isTask here
        //            DailyTrends = await GetDailyTrendStatsAsync(areaPaths, dateMap, "Client Issue", isTask)
        //        }
        //    };
        //}
        public async Task<SpillageSummaryDto> GetFullSummaryAsync(List<string> areaPaths, List<SprintDto> adoSprints, string projectId, bool isTask = false)
        {
            var iterationPaths = adoSprints.Select(s => s.Path).ToList();
            var dateMap = adoSprints.ToDictionary(s => s.Path, s => (s.Attributes.StartDate.Value, s.Attributes.FinishDate.Value), StringComparer.OrdinalIgnoreCase);

            // STEP 1: SINGLE DATABASE CALL
            var allRawData = isTask
                ? await GetTaskDataAsync(iterationPaths, areaPaths)
                : await GetUserStoryDataAsync(iterationPaths, areaPaths);

            // STEP 2: SINGLE BATCH API CALL (DevOps Titles)
            var allParentIds = allRawData.Where(x => x.ParentId.HasValue).Select(x => x.ParentId.Value).Distinct().ToList();
            var titleMap = await _devops.GetWorkItemTitlesBatchAsync(projectId, allParentIds);

            // STEP 3: SPLIT DATA IN MEMORY (No more DB calls)
            var featureData = allRawData.Where(x => x.ParentType == "Feature").ToList();
            var clientData = allRawData.Where(x => x.ParentType == "Client Issue").ToList();

            // STEP 4: RUN CALCULATIONS IN PARALLEL
            var allTask = Task.Run(() => ProcessSectionFromMemory(allRawData, adoSprints, dateMap, titleMap, "all", isTask));
            var featureTask = Task.Run(() => ProcessSectionFromMemory(featureData, adoSprints, dateMap, titleMap, "Feature", isTask));
            var clientTask = Task.Run(() => ProcessSectionFromMemory(clientData, adoSprints, dateMap, titleMap, "Client Issue", isTask));

            await Task.WhenAll(allTask, featureTask, clientTask);

            return new SpillageSummaryDto
            {
                All = allTask.Result,
                Feature = featureTask.Result,
                Client = clientTask.Result
            };
        }

        private SectionDto ProcessSectionFromMemory(List<UnifiedWorkItem> data, List<SprintDto> adoSprints, Dictionary<string, (DateTime Start, DateTime End)> dateMap, Dictionary<int, string> titleMap, string parentType, bool isTask)
        {
            // Filter the segment for this section (Feature or Client Issue)
            var cleanData = data.Where(x => !string.IsNullOrEmpty(x.ParentType) && x.ParentId.HasValue).ToList();

            var sectionSegment = (parentType.ToLower() == "all")
                ? cleanData
                : cleanData.Where(x => x.ParentType!.Equals(parentType, StringComparison.OrdinalIgnoreCase)).ToList();

            return new SectionDto
            {
                Stats = GetSprintStatsFromMemory(sectionSegment, adoSprints, dateMap, parentType),
                Spillage = GetSpillageTrendFromMemory(sectionSegment, adoSprints, parentType),
                History = GetImpactedParentHistoryFromMemory(sectionSegment, titleMap),
                DailyTrends = GetDailyTrendStatsFromMemory(sectionSegment, dateMap, isTask),

                DeveloperActivityStats = isTask ? GetDeveloperActivityStats(sectionSegment, adoSprints) : new List<DeveloperSprintActivityDto>(),
                DeveloperStats = isTask
                    ? GetDeveloperStatsInternal(sectionSegment, adoSprints)
                    : new List<DeveloperSprintStatDto>(),
                EffortVariance = isTask ? GetTeamEffortVariance(sectionSegment, dateMap) : new List<EffortVarianceDto>()
            };
        }

        private List<UnifiedWorkItem> GetQualifiedTaskAssignments(List<UnifiedWorkItem> sectionData, List<SprintDto> adoSprints)
        {
            var sprintDateMap = adoSprints.ToDictionary(
                s => s.Path,
                s => new { Start = s.Attributes.StartDate ?? DateTime.MinValue, End = s.Attributes.FinishDate ?? DateTime.Now },
                StringComparer.OrdinalIgnoreCase);

            return sectionData
                .GroupBy(t => new { t.Id, t.IterationPath })
                .Select(group =>
                {
                    var history = group.OrderBy(h => h.AssignedDate).ToList();
                    var dates = sprintDateMap.GetValueOrDefault(group.Key.IterationPath);
                    if (dates == null) return null;

                    UnifiedWorkItem lastQualified = null;

                    // Traverse backwards to find the last assignment that lasted >= 24 hours
                    for (int i = history.Count - 1; i >= 0; i--)
                    {
                        var current = history[i];
                        // Safe approach if you aren't sure about the dictionary value
                        DateTime effectiveEnd = dates.End != default ? dates.End : DateTime.Now;
                        DateTime nextDate = current.ClosedDate ?? effectiveEnd;

                        if ((nextDate - current.AssignedDate).TotalHours >= 24)
                        {
                            lastQualified = current;
                            break;
                        }
                    }
                    return lastQualified;
                })
                .Where(v => v != null)
                .ToList();
        }

        private List<DeveloperSprintStatDto> GetDeveloperStatsInternal(List<UnifiedWorkItem> sectionData, List<SprintDto> adoSprints)
        {
            var sprintDateMap = adoSprints.ToDictionary(
                s => s.Path,
                s => new { Start = s.Attributes.StartDate, End = s.Attributes.FinishDate },
                StringComparer.OrdinalIgnoreCase);

            // Identify the "True Owner" per task per sprint
            var validOwners = GetQualifiedTaskAssignments(sectionData, adoSprints);

            // Final Aggregation and Sort (Most recent sprint first)
            return validOwners
                .GroupBy(v => new { v.IterationPath, v.AssignedTo })
                .Select(g => {
                    var sprintStart = sprintDateMap.GetValueOrDefault(g.Key.IterationPath)?.Start;
                    return new
                    {
                        Dto = new DeveloperSprintStatDto
                        {
                            Sprint = g.Key.IterationPath,
                            Developer = g.Key.AssignedTo ?? "Unassigned",
                            TotalTasksAssigned = g.Count(),
                            TotalTasksCompleted = g.Count(x => x.State != null && x.State.Equals("Closed", StringComparison.OrdinalIgnoreCase)),
                            TotalHours = (double)g.Sum(x => x.DevEffort ?? 0m),
                            SprintStartDate = sprintStart
                        },
                        SortDate = sprintStart
                    };
                })
                .OrderByDescending(r => r.SortDate)
                .ThenByDescending(r => r.Dto.TotalTasksCompleted)
                .Select(r => r.Dto)
                .ToList();
        }

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

        public async Task<List<DeveloperSprintStatDto>> GetDeveloperPerformanceReportAsync(
    List<string> areaPaths,
    List<SprintDto> adoSprints)
        {
            var iterationPaths = adoSprints.Select(s => s.Path).ToList();

            // 1. Map sprint boundaries
            var sprintDateMap = adoSprints.ToDictionary(
                s => s.Path,
                s => new
                {
                    Start = s.Attributes.StartDate ?? DateTime.MinValue,
                    End = s.Attributes.FinishDate ?? DateTime.Now
                },
                StringComparer.OrdinalIgnoreCase);

            // 2. Fetch raw data (FULL assignment history, not grouped by sprint)
            var rawData = await (from itr in _context.IvpTaskIterations
                                 join asgn in _context.IvpTaskAssignees on itr.TaskId equals asgn.TaskId
                                 join det in _context.IvpTaskDetails on itr.TaskId equals det.TaskId
                                 join usd in _context.IvpUserStoryDetails on det.UserStoryId equals usd.UserStoryId
                                 where iterationPaths.Contains(itr.IterationPath)
                                       && areaPaths.Contains(usd.AreaPath)
                                 select new UnifiedWorkItem
                                 {
                                     Id = itr.TaskId,
                                     IterationPath = itr.IterationPath, // original (not used for grouping)
                                     AssignedTo = asgn.AssignedTo,
                                     AssignedDate = asgn.AssignedDate,
                                     State = det.State,
                                     DevEffort = det.DevEffort
                                 })
                                .OrderBy(x => x.Id)
                                .ThenBy(x => x.AssignedDate)
                                .ToListAsync();

            // 3. Group by TASK (IMPORTANT: not by sprint)
            var tasksGrouped = rawData
                .GroupBy(t => t.Id)
                .ToList();

            var validOwners = new List<UnifiedWorkItem>();

            // 4. Evaluate EACH task across EACH sprint
            foreach (var taskGroup in tasksGrouped)
            {
                var history = taskGroup.OrderBy(x => x.AssignedDate).ToList();

                foreach (var sprint in adoSprints)
                {
                    var sprintPath = sprint.Path;

                    if (!sprintDateMap.TryGetValue(sprintPath, out var sprintDates))
                        continue;

                    DateTime sprintStart = sprintDates.Start;
                    DateTime sprintEnd = sprintDates.End;

                    // Traverse backwards (latest assignment first)
                    for (int i = history.Count - 1; i >= 0; i--)
                    {
                        var current = history[i];

                        DateTime nextDate = (i + 1 < history.Count)
                            ? history[i + 1].AssignedDate
                            : DateTime.Now;

                        // Calculate overlap WITHIN sprint
                        DateTime effectiveStart = current.AssignedDate > sprintStart
                            ? current.AssignedDate
                            : sprintStart;

                        DateTime effectiveEnd = nextDate < sprintEnd
                            ? nextDate
                            : sprintEnd;

                        var hoursHeld = (effectiveEnd - effectiveStart).TotalHours;

                        if (hoursHeld >= 24)
                        {
                            validOwners.Add(new UnifiedWorkItem
                            {
                                Id = current.Id,
                                IterationPath = sprintPath, // assign THIS sprint
                                AssignedTo = current.AssignedTo,
                                State = current.State,
                                DevEffort = current.DevEffort
                            });

                            break; // stop after finding last valid owner
                        }
                    }
                }
            }

            // 5. Final aggregation
            var result = validOwners
                .GroupBy(v => new { v.IterationPath, v.AssignedTo })
                .Select(g =>
                {
                    var sprintDates = sprintDateMap[g.Key.IterationPath];

                    return new DeveloperSprintStatDto
                    {
                        Sprint = g.Key.IterationPath,
                        Developer = g.Key.AssignedTo ?? "Unassigned",
                        TotalTasksAssigned = g.Count(),
                        TotalTasksCompleted = g.Count(x =>
                            x.State != null &&
                            x.State.Equals("Closed", StringComparison.OrdinalIgnoreCase)),
                        TotalHours = (double)g.Sum(x => x.DevEffort ?? 0m),
                        SprintStartDate = sprintDates.Start
                    };
                })
                .OrderBy(x => x.SprintStartDate)
                .ThenByDescending(x => x.TotalTasksCompleted)
                .ToList();

            return result;
        }

        //private List<EffortVarianceDto> GetEffortVarianceFromMemory(List<UnifiedWorkItem> data, Dictionary<string, (DateTime Start, DateTime End)> sprintDateMap, List<SprintDto> adoSprints)
        //{
        //    // 1. FILTER: Only include tasks that passed the "True Owner" logic
        //    var validTasks = GetQualifiedTaskAssignments(data, adoSprints);

        //    // 2. AGGREGATE: Now calculate variance only on these tasks
        //    return validTasks
        //        .Where(x => x.State != null && x.State.Equals("Closed", StringComparison.OrdinalIgnoreCase))
        //        .GroupBy(x => new { x.IterationPath, x.AssignedTo })
        //        .Select(g => {
        //            var sprintDates = sprintDateMap.GetValueOrDefault(g.Key.IterationPath);
        //            return new EffortVarianceDto
        //            {
        //                Sprint = g.Key.IterationPath,
        //                Developer = g.Key.AssignedTo ?? "Unassigned",
        //                CommittedEffort = (double)g.Sum(x => x.InitialEffort ?? 0m),
        //                ActualEffort = (double)g.Sum(x => (x.DevEffort ?? 0m) * 7m),
        //                SortDate = sprintDates.Start
        //            };
        //        })
        //        .OrderByDescending(x => x.SortDate)
        //        .ToList();
        //}
        private List<EffortVarianceDto> GetTeamEffortVariance(List<UnifiedWorkItem> data, Dictionary<string, (DateTime Start, DateTime End)> sprintDateMap)
        {
            // 1. Identify the LATEST iteration path and state for every task
            var closedTasks = data
                .GroupBy(x => x.Id)
                .Select(g => g.OrderByDescending(x => x.AssignedDate).FirstOrDefault())
                .Where(t => t != null && t.State != null && t.State.Equals("Closed", StringComparison.OrdinalIgnoreCase))
                .ToList();

            // 2. Aggregate by IterationPath (Team level)
            return closedTasks
                .GroupBy(t => t.IterationPath)
                .Select(g =>
                {
                    var sprintDates = sprintDateMap.GetValueOrDefault(g.Key);

                    // Calculate capacity threshold dynamically based on the sprint dates
                    double capacity = sprintDates != default ? Math.Round((sprintDates.End - sprintDates.Start).TotalDays * 8.0, 1) : 0;

                    return new EffortVarianceDto
                    {
                        Sprint = g.Key,
                        Developer = "Team Total", // Aggregated
                        CommittedEffort = (double)g.Sum(x => x.InitialEffort ?? 0m),
                        ActualEffort = (double)g.Sum(x => (x.DevEffort ?? 0m) * 7m),
                        SortDate = sprintDates.Start
                    };
                })
                .OrderByDescending(x => x.SortDate)
                .ToList();
        }

        private List<DeveloperSprintActivityDto> GetDeveloperActivityStats(List<UnifiedWorkItem> sectionData, List<SprintDto> adoSprints)
        {
            // Create a dictionary to find the StartDate for each iteration path
            var sprintDateMap = adoSprints.ToDictionary(
                s => s.Path,
                s => s.Attributes.StartDate ?? DateTime.MinValue,
                StringComparer.OrdinalIgnoreCase);

            // Reuses your existing "True Owner" logic
            var validAssignments = GetQualifiedTaskAssignments(sectionData, adoSprints);

            return validAssignments
                .Where(x => !string.IsNullOrEmpty(x.Activity) && !string.IsNullOrEmpty(x.AssignedTo))
                .GroupBy(x => new { x.AssignedTo, x.IterationPath })
                .Select(group => new DeveloperSprintActivityDto
                {
                    Developer = group.Key.AssignedTo,
                    PeriodLabel = group.Key.IterationPath,
                    SortDate = sprintDateMap.GetValueOrDefault(group.Key.IterationPath),
                    Activities = group
                        .GroupBy(a => a.Activity)
                        .Select(actGroup => new ActivityDetailDto
                        {
                            ActivityType = actGroup.Key,
                            Total = actGroup.Count(),
                            Completed = actGroup.Count(x => x.State != null && x.State.Equals("Closed", StringComparison.OrdinalIgnoreCase))
                        }).ToList()
                })
                .ToList();
        }

    }

    //for task/user story
    public class WorkItemHistoryRow
    {
        public int Id { get; set; }
        public string IterationPath { get; set; }
        public DateTime AssignedDate { get; set; }
        public double Value { get; set; } // Will be StoryPoints for Stories, 1.0 for Tasks
        public DateTime? ClosedDate { get; set; }
    }

    // Inside SpillageService.cs or your Dto file
    public class ParentImpactDto
    {
        public int? ParentId { get; set; }
        public string? ParentTitle { get; set; } // NEW: Added for ADO Title
        public string ParentStatus { get; set; }
        public int TotalStoryCount { get; set; }
        public int TotalImpactScore { get; set; }
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

    public class DeveloperSprintActivityDto
    {
        public string PeriodLabel { get; set; } // "sprint 05 jan - 23 jan 2026"
        public string Developer { get; set; }
        public List<ActivityDetailDto> Activities { get; set; } = new();
        public DateTime SortDate { get; set; }
    }

    public class ActivityDetailDto
    {
        public string ActivityType { get; set; } // "Dev", "PR", "Testing"
        public int Total { get; set; }
        public int Completed { get; set; }
    }
}