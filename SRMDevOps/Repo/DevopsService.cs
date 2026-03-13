using SRMDevOps.Controllers;
using System.Net.Http.Headers;
using System.Text;
using System.Text.Json;

namespace SRMDevOps.Repo
{
    public class DevopsService : IADO
    {

        private readonly IConfiguration _configuration;

        public DevopsService(IConfiguration configuration)
        {
            _configuration = configuration;
        }

        public async Task<string> GetTeamsInProject(string projectId)
        {
            // Retrieve PAT from configuration securely
            var pat = _configuration["AzureDevOps:PAT"];
            var credentials = Convert.ToBase64String(Encoding.ASCII.GetBytes($":{pat}"));

            using (HttpClient client = new HttpClient())
            {
                client.DefaultRequestHeaders.Accept.Add(
                    new MediaTypeWithQualityHeaderValue("application/json"));

                client.DefaultRequestHeaders.Authorization = new AuthenticationHeaderValue("Basic", credentials);

                // URL to fetch all teams for a specific project
                string url = $"https://dev.azure.com/Indusvalleypartners/_apis/projects/{projectId}/teams?api-version=7.1";

                using (HttpResponseMessage response = await client.GetAsync(url))
                {
                    if (response.IsSuccessStatusCode)
                    {
                        return await response.Content.ReadAsStringAsync();
                    }
                    throw new Exception($"Failed to fetch teams. Status: {response.StatusCode}");
                }
            }
        }

        public async Task<TeamFieldValuesDto> GetTeamAreaPaths(string projectId, string teamId)
        {
            var pat = _configuration["AzureDevOps:PAT"];
            var credentials = Convert.ToBase64String(Encoding.ASCII.GetBytes($":{pat}"));

            using (HttpClient client = new HttpClient())
            {
                client.DefaultRequestHeaders.Authorization = new AuthenticationHeaderValue("Basic", credentials);

                // API call to get Team Field Values (which are Area Paths by default)
                string url = $"https://dev.azure.com/Indusvalleypartners/{projectId}/{teamId}/_apis/work/teamsettings/teamfieldvalues?api-version=7.1"; 

                var response = await client.GetAsync(url);
                if (response.IsSuccessStatusCode)
                {
                    var rawJson = await response.Content.ReadAsStringAsync();
                    var options = new JsonSerializerOptions { PropertyNameCaseInsensitive = true };
                    return JsonSerializer.Deserialize<TeamFieldValuesDto>(rawJson, options);
                }
                throw new Exception($"Error: {response.StatusCode}");
            }
        }
    }
}
