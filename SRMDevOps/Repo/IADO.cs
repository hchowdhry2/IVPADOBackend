using SRMDevOps.Controllers;

namespace SRMDevOps.Repo
{
    public interface IADO
    {
        public Task<string> GetTeamsInProject(string projectName);
        public Task<TeamFieldValuesDto> GetTeamAreaPaths(string projectName, string teamName);
    }
}
