using Microsoft.AspNetCore.Mvc;
using SRMDevOps.Dto;
using static SRMDevOps.Repo.DevopsService;

namespace SRMDevOps.Repo
{
    public interface ISpillage
    {

        Task<List<SprintProgressDto>> GetSprintStatsAsync(List<string> adoAreaPaths, List<SprintDto> adoSprints, string? parentType);
        Task<List<SpillageTrendDto>> GetSpillageTrendAsync(List<string> adoAreaPaths, List<SprintDto> adoSprints, string? parentType);
        Task<List<StoryHistoryDto>> GetStoryHistoryAsync(List<string> adoAreaPaths, List<SprintDto> adoSprints, string? parentType = null);
        Task<SpillageSummaryDto> GetFullSummaryAsync(List<string> adoAreaPaths, List<SprintDto> adoSprints);

        Task<SpillageSummaryDto> GetAggregatedTeamStatsAsync(string? timeframe,int n,List<string> adoAreaPaths, List<SprintDto> adoSprints);

        Task<List<SprintDto>> GetSprintsForTimeframeAsync(string projectId, string teamId, string? timeframe, int n);

        //// Aggregated business methods (summary) — unchanged for last-N
        //Task<SpillageSummaryDto> GetSpillageSummaryLast(string projectName, int lastNSprints);

        //// Updated summary-by-time: supports periodUnit ("monthly","quarterly","yearly") and n = number of periods
        //Task<SpillageSummaryDto> GetSpillageSummaryTime(string projectName, string? periodUnit = null, int? n = null);

    }
}
