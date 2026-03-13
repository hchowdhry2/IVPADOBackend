using Microsoft.AspNetCore.Http;
using Microsoft.AspNetCore.Mvc;
using SRMDevOps.Repo;
using System.Net.Http.Headers;
using System.Text;
using System.Text.Json;

namespace SRMDevOps.Controllers
{
    [Route("api/[controller]")]
    [ApiController]
    public class DevopsController : ControllerBase
    {
        private readonly IADO _adoService;
        private readonly IConfiguration _configuration;

        public DevopsController(IADO adoService,IConfiguration configuration)
        {
            _adoService = adoService;
            _configuration = configuration;

        }

        [HttpGet("devops-projects/")]
        public async Task<IActionResult> GetProjects()
        {
            // 1. Move the PAT to configuration (appsettings.json)
            var pat = _configuration["AzureDevOps:PAT"];
            var credentials = Convert.ToBase64String(Encoding.ASCII.GetBytes($":{pat}"));

            try
            {
                // Using a shared client or factory is better than 'new HttpClient()'
                using var client = new HttpClient();
                client.DefaultRequestHeaders.Accept.Add(new MediaTypeWithQualityHeaderValue("application/json"));
                client.DefaultRequestHeaders.Authorization = new AuthenticationHeaderValue("Basic", credentials);

                // Note: This gets ALL projects. 
                var url = $"https://dev.azure.com/Indusvalleypartners/_apis/projects/"; ;

                var response = await client.GetAsync(url);

                if (response.IsSuccessStatusCode)
                {
                    var content = await response.Content.ReadAsStringAsync();
                    // You might want to parse this JSON to find the specific 'projectName'
                    return Ok(content);
                }

                return StatusCode((int)response.StatusCode, "Error calling DevOps API");
            }
            catch (Exception ex)
            {
                // Log the actual error, don't just return NotFound
                //_logger.LogError(ex, "Failed to fetch DevOps projects");
                return StatusCode(500, "Internal Server Error");
            }
        }



        [HttpGet("devops-teams/{projectId}")]
        public async Task<IActionResult> GetProjectTeams(string projectId)
        {
            try
            {
                // 1. Fetch the raw JSON from the API
                string rawJson = await _adoService.GetTeamsInProject(projectId);

                // 2. Deserialize into our clean objects
                var options = new JsonSerializerOptions { PropertyNameCaseInsensitive = true };
                var result = JsonSerializer.Deserialize<AzureDevOpsResponse<TeamDto>>(rawJson, options);

                // 3. Return the "Value" list (the actual teams)
                return Ok(result?.Value ?? new List<TeamDto>());
            }
            catch (Exception ex)
            {
                return StatusCode(500, $"Internal server error: {ex.Message}");
            }
        }


        [HttpGet("devops-area-paths/{projectId}/{teamId}")]
        public async Task<IActionResult> GetProjectTeams([FromRoute] string projectId,[FromRoute] string teamId)
        {
            try
            {
                // 1. Fetch the raw JSON from the API
                TeamFieldValuesDto result = await _adoService.GetTeamAreaPaths(projectId,teamId);

                // 3. Return the "Value" list (the actual teams)
                return Ok(result ?? new TeamFieldValuesDto());
            }
            catch (Exception ex)
            {
                return StatusCode(500, $"Internal server error: {ex.Message}");
            }
        }
    }

    public class TeamFieldValuesDto
    {
        public string DefaultValue { get; set; } // The primary Area Path
        public List<AreaPathValue> Values { get; set; } // All associated Area Paths
    }

    public class AreaPathValue
    {
        public string Value { get; set; } // The Area Path string
        public bool IncludeChildren { get; set; } // If sub-areas are included
    }
}
