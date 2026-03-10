using Microsoft.AspNetCore.Mvc;
using SRMDevOps.Dto;

namespace SRMDevOps.Repo
{
    public interface ISpillage
    {
        Task<List<SprintProgressDto>> GetAllSprintStats(string projectName, int lastNSprints);
        Task<List<SprintProgressDto>> GetAllSprintStatsByTime(string projectName, string timeframe);
        Task<List<SpillageTrendDto>> GetAllSpillageTrend(string projectName, int lastNSprints);
        Task<List<SpillageTrendDto>> GetAllSpillageTimeline(string projectName, string timeframe);

        Task<List<SprintProgressDto>> GetFeatureSprintStats(string projectName, int lastNSprints);
        Task<List<SprintProgressDto>> GetFeatureSprintStatsByTime(string projectName, string timeframe);
        Task<List<SpillageTrendDto>> GetFeatureSpillageTrend(string projectName, int lastNSprints);
        Task<List<SpillageTrendDto>> GetFeatureSpillageTimeline(string projectName, string timeframe);

        Task<List<SprintProgressDto>> GetClientSprintStats(string projectName, int lastNSprints);
        Task<List<SprintProgressDto>> GetClientSprintStatsByTime(string projectName, string timeframe);
        Task<List<SpillageTrendDto>> GetClientSpillageTrend(string projectName, int lastNSprints);
        Task<List<SpillageTrendDto>> GetClientSpillageTimeline(string projectName, string timeframe);

        // Aggregated business methods (summary) — unchanged for last-N
        Task<SpillageSummaryDto> GetSpillageSummaryLast(string projectName, int lastNSprints);

        // Updated summary-by-time: supports periodUnit ("monthly","quarterly","yearly") and n = number of periods
        Task<SpillageSummaryDto> GetSpillageSummaryTime(string projectName, string? periodUnit = null, int? n = null);

        // History helpers
        Task<List<StoryHistoryDto>> GetStoryHistoryLastNSprints(string projectName, int lastNSprints, string? parentType = null);
        Task<List<StoryHistoryDto>> GetStoryHistoryByTimeframe(string projectName, string timeframe, string? parentType = null);
    }
}
