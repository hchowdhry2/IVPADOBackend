using SRMDevOps.Dto;

namespace SRMDevOps.Repo
{
    public interface ITask
    {
        Task<SpillageSummaryDto?> GetTaskAggregatedTimeframeStatsAsync(string? timeframe, int n, List<string> areaPaths, List<SprintDto> validSprints);

        //Task<List<AggregatedStat>> GetTaskAggregatedStatsAsync(List<string> adoAreaPaths,Dictionary<string, (DateTime Start, DateTime End)> sprintDateMap);
    }
}
