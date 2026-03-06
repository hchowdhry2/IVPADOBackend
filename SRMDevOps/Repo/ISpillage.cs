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

        // Aggregated business methods moved from controller
        Task<SpillageSummaryDto> GetSpillageSummaryLast(string projectName, int lastNSprints);
        Task<SpillageSummaryDto> GetSpillageSummaryTime(string projectName, string timeframe);
    }
}
