using SRMDevOps.Controllers;
using SRMDevOps.Dto;
using static SRMDevOps.Repo.DevopsService;

namespace SRMDevOps.Repo
{
    public interface IADO
    {
        Task<List<TeamDto>> GetTeamsByProjectIdAsync(string projectId);
        Task<List<string>> GetTeamAreaPathsAsync(string projectId, string teamId);
        Task<List<SprintDto>> GetRecentSprintsAsync(string projectId, string teamId, int lastNSprints);
        Task<List<SprintProgressDto>> GetSprintDataByAreaPathAsync(
            string projectId,
            string teamId,
            int lastNSprints);
        //Task<CombinedSprintDataDto> GetAggregatedTeamStatsAsync(
        //string projectId,
        //string teamId,
        //string? timeframe,
        //int n);

        //public Task<string> GetTeamsInProject(string projectName);
        //public Task<TeamFieldValuesDto> GetTeamAreaPaths(string projectName, string teamName);

        //public Task<List<SprintProgressDto>> GetSprintDataByAreaPathAsync(string projectId, string teamId, string selectedAreaPath, int lastNSprints);
    }
}
