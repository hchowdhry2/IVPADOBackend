using Microsoft.AspNetCore.Mvc;
using Microsoft.VisualBasic;
using SRMDevOps.Dto;
using SRMDevOps.Repo;
using System.Collections.Generic;
using System.Net.Http.Headers;
using System.Text;
using System.Text.Json;
using System.Threading.Tasks;
using static SRMDevOps.Repo.DevopsService;

namespace SRMDevOps.Controllers
{

    // Wrapper to match the Azure DevOps "value" array structure
    public class AzureDevOpsResponse<T>
    {
        public int Count { get; set; }
        public List<T> Value { get; set; }
    }

    [Route("api/[controller]")]
    [ApiController]
    public class SpillageController : ControllerBase
    {
        private readonly ISpillage _spillage;
        private readonly IADO _devops;
        private readonly ITask _taskService;

        public SpillageController(ISpillage spillage, IConfiguration configuration, IADO devops, ITask taskService)
        {
            _spillage = spillage;
            _devops = devops;
            _taskService = taskService;
        }
        [HttpGet]
        public async Task<IActionResult> GetStats([FromQuery]string projectId, [FromQuery] string teamId, [FromQuery] string? timeframe, [FromQuery] int n = 6, [FromQuery] string workType = "story")
        {
            var areaPaths = await _devops.GetTeamAreaPathsAsync(projectId, teamId);

            // 1. Fetch a larger pool regardless of timeframe (e.g., n + 10)
            // This gives us "room" to filter out future sprints without losing December.
            var validSprints = await _spillage.GetSprintsForTimeframeAsync(projectId, teamId, timeframe, n);

            bool isTask = workType.Equals("task", StringComparison.OrdinalIgnoreCase);

            if (string.IsNullOrEmpty(timeframe))
            {
                // 2. Take the top 'n' from the VALID (past/current) list.
                // This ensures if n=6, you get 6 sprints that have actually started.
                var sprintwiseList = validSprints.Take(n).ToList();

                var results = await _spillage.GetFullSummaryAsync(areaPaths, sprintwiseList, isTask);
                return Ok(results);
            }

            // Monthly/Quarterly logic
            var result = await _spillage.GetAggregatedTeamStatsAsync(timeframe, n, areaPaths, validSprints, isTask);
            return Ok(result ?? new SpillageSummaryDto());
        }

        //[HttpGet("tasks")]
        //public async Task<IActionResult> GetTaskStats(string projectId, string teamId, string? timeframe, int n = 6)
        //{
        //    // 1. Reuse existing logic to get Area Paths and Sprints
        //    var areaPaths = await _devops.GetTeamAreaPathsAsync(projectId, teamId);
        //    var validSprints = await _spillage.GetSprintsForTimeframeAsync(projectId, teamId, timeframe, n);

        //    // 2. Call the Task Service
        //    // This will return the "Counts" of tasks instead of "Sum of points"
        //    var result = await _taskService.GetTaskAggregatedTimeframeStatsAsync(timeframe, n, areaPaths, validSprints);

        //    return Ok(result ?? new SpillageSummaryDto());
        //}
    }
}

//[HttpGet]
//public async Task<IActionResult> GetStats(string projectId, string teamId, string? timeframe, int n = 6)
//{
//    // 1. Fetch Area Paths (Metadata)
//    var areaPaths = await _devops.GetTeamAreaPathsAsync(projectId, teamId);

//    // 2. DYNAMIC FETCH LOGIC
//    // If timeframe is null, we just want 'n' individual sprints.
//    // If monthly/quarterly, we need more sprints to fill the buckets.
//    int fetchCount = n;

//    if (!string.IsNullOrEmpty(timeframe))
//    {
//        // We calculate a multiplier to ensure we get enough data:
//        // Monthly: ~3 sprints per month, Quarterly: ~8 sprints per quarter
//int multiplier = timeframe.ToLower() switch
//{
//    "quarter" or "quarterly" => 8,
//    "year" or "yearly" => 28,
//    _ => 3 // Default for monthly
//};
//fetchCount = n* multiplier;
//    }

//    // 3. Fetch the sprints using the calculated count
//    var adoSprints = await _devops.GetRecentSprintsAsync(projectId, teamId, lastNSprints: fetchCount);

//    // 4. Handle Standard View (Individual Sprints)
//    if (string.IsNullOrEmpty(timeframe))
//    {
//        var limitedSprints = adoSprints.Take(n).ToList();
//        var results = await _spillage.GetFullSummaryAsync(areaPaths, limitedSprints);
//        return Ok(results);
//    }

//    // 5. Handle Aggregated View (Monthly/Quarterly)
//    // The service now has a "large pool" of sprints to group into months
//    var result = await _spillage.GetAggregatedTeamStatsAsync(timeframe, n, areaPaths, adoSprints);

//    return Ok(result ?? new SpillageSummaryDto());
//}

//[HttpGet]
//public async Task<IActionResult> GetStats(string projectId, string teamId, string? timeframe, int n = 6)
//{
//    // 1. Fetch ADO definitions (Dates and Paths)
//    var areaPaths = await _devops.GetTeamAreaPathsAsync(projectId, teamId);
//    var sprints = await _devops.GetRecentSprintsAsync(projectId, teamId, lastNSprints: n);
//    if (string.IsNullOrEmpty(timeframe))
//    {
//        //var sprints = await _devops.GetRecentSprintsAsync(projectId, teamId, lastNSprints: n);
//        var results = await _spillage.GetFullSummaryAsync(areaPaths, sprints);

//        return Ok(results);
//    }

//    //var result = await _devops.GetAggregatedTeamStatsAsync(projectId, teamId, timeframe, n);
//    var result = await _spillage.GetAggregatedTeamStatsAsync(timeframe, n, areaPaths, sprints);

//    //return Ok(result ?? new CombinedSprintDataDto());
//    return Ok(result ?? new SpillageSummaryDto());

//}


//using System.Collections.Generic;
//using System.Threading.Tasks;
//using Microsoft.AspNetCore.Mvc;
//using SRMDevOps.Dto;
//using SRMDevOps.Repo;

//namespace SRMDevOps.Controllers
//{
//    [Route("api/[controller]")]
//    [ApiController]
//    public class SpillageController : ControllerBase
//    {
//        private readonly ISpillage _spillage;

//        public SpillageController(ISpillage spillage)
//        {
//            _spillage = spillage;
//        }

//        /// <summary>
//        /// Unified summary endpoint. Use query parameters to select mode:
//        /// - ?lastNSprints=5  -> last-N-sprints mode
//        /// - ?timeframe=yearly -> timeframe mode ("yearly" or default 6 months)
//        /// If both provided, lastNSprints takes precedence.
//        /// </summary>
//        [HttpGet("summary/{projectName}")]
//        public async Task<IActionResult> GetSpillageSummary(
//            string projectName,
//            [FromQuery] int? lastNSprints,
//            [FromQuery] string? timeframe)
//        {
//            // last-N-sprints mode (takes precedence when provided)
//            if (lastNSprints.HasValue && lastNSprints.Value > 0)
//            {
//                // Fix: Pass lastNSprints.Value (int) instead of lastNSprints (int?)
//                var summary = await _spillage.GetSpillageSummaryLast(projectName, lastNSprints.Value);
//                return Ok(summary);
//            }

//            else
//            {
//                // timeframe mode (defaults to service behavior if timeframe is null/empty)
//                var summary = await _spillage.GetSpillageSummaryTime(projectName, timeframe);
//                return Ok(summary);
//            }
//        }
//    }
//}

//using System.Collections.Generic;
//using System.Threading.Tasks;
//using Microsoft.AspNetCore.Mvc;
//using SRMDevOps.Dto;
//using SRMDevOps.Repo;

//namespace SRMDevOps.Controllers
//{
//    [Route("api/[controller]")]
//    [ApiController]
//    public class SpillageController : ControllerBase
//    {
//        private readonly ISpillage _spillage;

//        public SpillageController(ISpillage spillage)
//        {
//            _spillage = spillage;
//        }

//        // Aggregated endpoint for last N sprints:
//        // Returns SpillageSummaryDto with segregated sections for clarity on the frontend.
//        [HttpGet("summary-last/{projectName}/{lastNSprints}")]
//        public async Task<IActionResult> GetSpillageSummaryLast(string projectName, int lastNSprints)
//        {
//            var summary = await _spillage.GetSpillageSummaryLast(projectName, lastNSprints);
//            return Ok(summary);
//        }

//        // Aggregated endpoint for timeframe-based queries:
//        // Returns SpillageSummaryDto with segregated sections for clarity on the frontend.
//        [HttpGet("summary-time/{projectName}/{timeframe}")]
//        public async Task<IActionResult> GetSpillageSummaryTime(string projectName, string timeframe)
//        {
//            var summary = await _spillage.GetSpillageSummaryTime(projectName, timeframe);
//            return Ok(summary);
//        }
//    }
//}

//using Microsoft.AspNetCore.Http;
//using Microsoft.AspNetCore.Mvc;
//using Microsoft.EntityFrameworkCore;
//using SRMDevOps.DataAccess;
//using SRMDevOps.Dto;
//using SRMDevOps.Repo;

//namespace SRMDevOps.Controllers
//{
//    [Route("api/[controller]")]
//    [ApiController]
//    public class SpillageController : ControllerBase
//    {
//        private readonly ISpillage _spillage;

//        public SpillageController(ISpillage spillage)
//        {
//            _spillage = spillage;
//        }

//        // Controller of fetching details for all feature or client

//        [HttpGet("burnup-all/{projectName}/{lastNSprints}")]
//        public async Task<IActionResult> GetAllSprintStats(string projectName, int lastNSprints)
//        {
//            var finalResult = await _spillage.GetAllSprintStats(projectName, lastNSprints);

//            return Ok(finalResult);
//        }

//        [HttpGet("burnup-timeline-all/{projectName}/{timeframe}")]
//        public async Task<IActionResult> GetAllSprintStatsByTime(string projectName, string timeframe)
//        {
//            var stats = await _spillage.GetAllSprintStatsByTime(projectName, timeframe);

//            return Ok(stats);
//        }

//        [HttpGet("spillage-trend-all/{projectName}/{lastNSprints}")]
//        public async Task<IActionResult> GetAllSpillageTrend(string projectName, int lastNSprints)
//        {
//            var result = await _spillage.GetAllSpillageTrend(projectName, lastNSprints);

//            return Ok(result);
//        }

//        [HttpGet("spillage-timeline-all/{projectName}/{timeframe}")]
//        public async Task<IActionResult> GetAllSpillageTimeline(string projectName, string timeframe)
//        {
//            var result = await _spillage.GetAllSpillageTimeline(projectName, timeframe);

//            return Ok(result);
//        }


//        // Controller of fetching data for feature

//        [HttpGet("burnup-feature/{projectName}/{lastNSprints}")]
//        public async Task<IActionResult> GetFeatureSprintStats(string projectName, int lastNSprints)
//        {
//            var finalResult = await _spillage.GetFeatureSprintStats(projectName, lastNSprints);

//            return Ok(finalResult);
//        }

//        [HttpGet("burnup-timeline-feature/{projectName}/{timeframe}")]
//        public async Task<IActionResult> GetFeatureSprintStatsByTime(string projectName, string timeframe)
//        {
//            var stats = await _spillage.GetFeatureSprintStatsByTime(projectName, timeframe);

//            return Ok(stats);
//        }

//        [HttpGet("spillage-trend-feature/{projectName}/{lastNSprints}")]
//        public async Task<IActionResult> GetFeatureSpillageTrend(string projectName, int lastNSprints)
//        {
//            var result = await _spillage.GetFeatureSpillageTrend(projectName, lastNSprints);

//            return Ok(result);
//        }

//        [HttpGet("spillage-timeline-feature/{projectName}/{timeframe}")]
//        public async Task<IActionResult> GetFeatureSpillageTimeline(string projectName, string timeframe)
//        {
//            var result = await _spillage.GetFeatureSpillageTimeline(projectName, timeframe);

//            return Ok(result);
//        }

//        // Controller of fetching data for client
//        // Routes renamed to avoid conflicts with "all" endpoints.

//        [HttpGet("burnup-client/{projectName}/{lastNSprints}")]
//        public async Task<IActionResult> GetClientSprintStats(string projectName, int lastNSprints)
//        {
//            var finalResult = await _spillage.GetClientSprintStats(projectName, lastNSprints);

//            return Ok(finalResult);
//        }

//        [HttpGet("burnup-timeline-client/{projectName}/{timeframe}")]
//        public async Task<IActionResult> GetClientSprintStatsByTime(string projectName, string timeframe)
//        {
//            var stats = await _spillage.GetClientSprintStatsByTime(projectName, timeframe);

//            return Ok(stats);
//        }

//        [HttpGet("spillage-trend-client/{projectName}/{lastNSprints}")]
//        public async Task<IActionResult> GetClientSpillageTrend(string projectName, int lastNSprints)
//        {
//            var result = await _spillage.GetClientSpillageTrend(projectName, lastNSprints);

//            return Ok(result);
//        }

//        [HttpGet("spillage-timeline-client/{projectName}/{timeframe}")]
//        public async Task<IActionResult> GetClientSpillageTimeline(string projectName, string timeframe)
//        {
//            var result = await _spillage.GetClientSpillageTimeline(projectName, timeframe);

//            return Ok(result);
//        }


//    }
//}
