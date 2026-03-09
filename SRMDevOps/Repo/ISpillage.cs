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

        // Aggregated business methods (summary) — unchanged
        Task<SpillageSummaryDto> GetSpillageSummaryLast(string projectName, int lastNSprints);
        Task<SpillageSummaryDto> GetSpillageSummaryTime(string projectName, string timeframe);

        // New: per-user-story history (added alongside existing summary functionality)
        // parentType: null for all, "Feature" or "Client Issue" to filter
        Task<List<StoryHistoryDto>> GetStoryHistoryLastNSprints(string projectName, int lastNSprints, string? parentType = null);
        Task<List<StoryHistoryDto>> GetStoryHistoryByTimeframe(string projectName, string timeframe, string? parentType = null);

        // new for monthly-yearly
        Task<List<SpillageTrendDto>> GetSpillageByMonthsAsync(string projectName, string timeframe, int? n = null, string? parentType = null);
        Task<List<SprintProgressDto>> GetSprintStatsByMonthsAsync(string projectName, string timeframe, int? n = null, string? parentType = null);
    }
}
