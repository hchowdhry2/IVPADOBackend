using SRMDevOps.DataAccess;
using SRMDevOps.Dto;
using Microsoft.EntityFrameworkCore;

namespace SRMDevOps.Repo;

public class TaskService : ITask
{
    private readonly IvpadodashboardContext _context;

    public TaskService(IvpadodashboardContext context)
    {
        _context = context;
    }

    // Aligned these names with the logic below
    private sealed record AggregatedStat(
        string FullPath,
        double TotalCount,
        double InitialCount,
        double AddedLaterCount,
        double TotalClosedCount,
        double ClosedTimelyCount,
        double ClosedLateCount,
        DateTime? SortDate);

    private async Task<List<AggregatedStat>> GetTaskAggregatedStatsAsync(
        List<string> adoAreaPaths,
        Dictionary<string, (DateTime Start, DateTime End)> sprintDateMap)
    {
        var iterationPaths = sprintDateMap.Keys.ToList();

        // 1. Join Task -> TaskDetails -> UserStoryDetails (to get AreaPath)
        var allData = await _context.IvpTaskIterations
            .Join(_context.IvpTaskDetails,
                ti => ti.TaskId,
                td => td.TaskId,
                (ti, td) => new { ti, td })
            .Join(_context.IvpUserStoryDetails,
                combined => combined.td.UserStoryId,
                usd => usd.UserStoryId,
                (combined, usd) => new { combined.ti, combined.td, usd })
            .Where(c => adoAreaPaths.Contains(c.usd.AreaPath))
            .Select(x => new
            {
                x.ti.TaskId,
                x.ti.IterationPath,
                x.ti.AssignedDate,
                x.td.ClosedDate
            })
            .OrderBy(x => x.TaskId)
            .ThenBy(x => x.AssignedDate)
            .ToListAsync();

        var sprintAggregates = iterationPaths.ToDictionary(
            path => path,
            path => new { Initial = 0.0, Added = 0.0, CompletedTimely = 0.0, CompletedLate = 0.0 }
        );

        foreach (var taskGroup in allData.GroupBy(x => x.TaskId))
        {
            var taskHistory = taskGroup.OrderBy(x => x.AssignedDate).ToList();

            foreach (var sprintPath in iterationPaths)
            {
                var dates = sprintDateMap[sprintPath];
                var sStart = dates.Start.ToLocalTime().Date;
                var sEndMax = dates.End.ToLocalTime().Date.AddDays(1).AddTicks(-1);

                var stateAtPlanningEnd = taskHistory
                    .Where(t => t.AssignedDate.ToLocalTime() <= sStart)
                    .OrderByDescending(t => t.AssignedDate)
                    .FirstOrDefault();

                bool isInitial = stateAtPlanningEnd != null &&
                                 stateAtPlanningEnd.IterationPath.Equals(sprintPath, StringComparison.OrdinalIgnoreCase);

                bool addedMidSprint = taskHistory.Any(t =>
                    t.IterationPath.Equals(sprintPath, StringComparison.OrdinalIgnoreCase) &&
                    t.AssignedDate.ToLocalTime() > sStart &&
                    t.AssignedDate.ToLocalTime() <= sEndMax);

                if (!(isInitial || addedMidSprint)) continue;

                var absoluteLatest = taskHistory.LastOrDefault();
                var latestInWindow = taskHistory
                    .Where(t => t.AssignedDate.ToLocalTime() <= sEndMax)
                    .OrderByDescending(t => t.AssignedDate)
                    .FirstOrDefault();

                double countIncrement = 1.0;
                bool isClosedTimely = false;
                bool isClosedLate = false;

                if (latestInWindow != null && latestInWindow.ClosedDate.HasValue)
                {
                    var closedTime = latestInWindow.ClosedDate.Value.ToLocalTime();
                    if (closedTime >= sStart && closedTime <= sEndMax)
                        isClosedTimely = true;
                    else if (closedTime > sEndMax && absoluteLatest != null &&
                             absoluteLatest.IterationPath.Equals(sprintPath, StringComparison.OrdinalIgnoreCase))
                        isClosedLate = true;
                }

                var current = sprintAggregates[sprintPath];
                sprintAggregates[sprintPath] = new
                {
                    Initial = current.Initial + (isInitial ? countIncrement : 0),
                    Added = current.Added + (!isInitial ? countIncrement : 0),
                    CompletedTimely = current.CompletedTimely + (isClosedTimely ? countIncrement : 0),
                    CompletedLate = current.CompletedLate + (isClosedLate ? countIncrement : 0)
                };
            }
        }

        return iterationPaths.Select(path =>
        {
            var data = sprintAggregates[path];
            return new AggregatedStat(
                path,
                data.Initial + data.Added,
                data.Initial,
                data.Added,
                data.CompletedTimely + data.CompletedLate,
                data.CompletedTimely,
                data.CompletedLate,
                sprintDateMap[path].Start
            );
        }).ToList();
    }

    public async Task<SpillageSummaryDto> GetTaskAggregatedTimeframeStatsAsync(
        string? timeframe,
        int n,
        List<string> adoAreaPaths,
        List<SprintDto> adoSprints)
    {
        var (unit, bucketMonths, defaultN) = NormalizePeriodUnit(timeframe);
        var periods = n > 0 ? n : defaultN;
        var windowStart = ComputeWindowStart(unit, periods);

        var dateMap = adoSprints.ToDictionary(s => s.Path, s => (s.Attributes.StartDate.Value, s.Attributes.FinishDate.Value), StringComparer.OrdinalIgnoreCase);
        var rawStats = await GetTaskAggregatedStatsAsync(adoAreaPaths, dateMap);

        var section = new SectionDto { Stats = new List<SprintProgressDto>(), Spillage = new List<SpillageTrendDto>() };

        for (int p = 0; p < periods; p++)
        {
            var periodStart = windowStart.AddMonths(p * bucketMonths).Date;
            var label = GetLabel(unit, periodStart);

            // Grouping logic for timeframe consistency
            var inBucket = rawStats
                .Where(s => s.SortDate.HasValue &&
                            s.SortDate.Value.ToLocalTime().Month == periodStart.Month &&
                            s.SortDate.Value.ToLocalTime().Year == periodStart.Year)
                .ToList();

            section.Stats.Add(new SprintProgressDto
            {
                IterationPath = label,
                SortDate = periodStart,
                InitialPoints = inBucket.Sum(x => x.InitialCount),
                MidSprintAddedPoints = inBucket.Sum(x => x.AddedLaterCount),
                TotalPointsAssigned = inBucket.Sum(x => x.TotalCount),
                TotalPointsCompleted = inBucket.Sum(x => x.TotalClosedCount),
                ClosedTimely = inBucket.Sum(x => x.ClosedTimelyCount),
                ClosedLate = inBucket.Sum(x => x.ClosedLateCount)
            });

            section.Spillage.Add(new SpillageTrendDto
            {
                IterationPath = label,
                SortDate = periodStart,
                SpillagePoints = inBucket.Sum(x => x.TotalCount - x.TotalClosedCount)
            });
        }

        return new SpillageSummaryDto { All = section };
    }

    private string GetLabel(string unit, DateTime date) => unit switch
    {
        "quarterly" => $"Q{((date.Month - 1) / 3) + 1} {date:yyyy}",
        "yearly" => date.ToString("yyyy"),
        _ => date.ToString("MMM yyyy")
    };

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
        var now = DateTime.Now;
        var bucketMonths = unit == "quarterly" ? 3 : unit == "yearly" ? 12 : 1;
        return new DateTime(now.Year, now.Month, 1, 0, 0, 0, DateTimeKind.Local)
                    .AddMonths(-((nPeriods * bucketMonths) - 1));
    }

    public Task<SpillageSummaryDto?> GetTaskAggregatedStatsAsync(string? timeframe, int n, List<string> areaPaths, List<SprintDto> validSprints)
    {
        throw new NotImplementedException();
    }
}