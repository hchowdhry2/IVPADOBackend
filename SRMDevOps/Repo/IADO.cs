using SRMDevOps.Controllers;
using SRMDevOps.Dto;

namespace SRMDevOps.Repo
{
    public interface IADO
    {
        public Task<string> GetTeamsInProject(string projectName);
        public Task<TeamFieldValuesDto> GetTeamAreaPaths(string projectName, string teamName);

        public Task<CombinedSprintDataDto> GetSprintAndSpillageDataAsync(string projectId, string teamId, string selectedAreaPath, int lastNSprints);
        public Task<CombinedSprintDataDto> GetSprintStatsByTimeframeAsync(string projectId, string teamId, string selectedAreaPath, string timeframe, int? n);
    }
}
