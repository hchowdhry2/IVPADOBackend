using Microsoft.AspNetCore.Mvc;
using SRMDevOps.Dto;
using SRMDevOps.Repo;
using System.Collections.Generic;
using System.Threading.Tasks;
using Microsoft.AspNetCore.Mvc;
using SRMDevOps.Dto;
using SRMDevOps.Repo;

namespace SRMDevOps.Controllers
{
    [Route("api/[controller]")]
    [ApiController]
    public class SpillageController : ControllerBase
    {
        private readonly ISpillage _spillage;

        public SpillageController(ISpillage spillage)
        {
            _spillage = spillage;
        }

        /// <summary>
        /// Unified summary endpoint.
        /// - ?lastNSprints=5  -> last-N-sprints mode (takes precedence)
        /// - ?timeframe=monthly|yearly -> timeframe mode
        /// - ?n=6 -> used with timeframe=monthly (months) or timeframe=yearly (years)
        /// </summary>
        [HttpGet("summary/{projectName}")]
        public async Task<IActionResult> GetSpillageSummary(
            string projectName,
            [FromQuery] int? lastNSprints,
            [FromQuery] string? timeframe,
            [FromQuery] int? n)
        {
            // last-N-sprints mode (takes precedence when provided)
            if (lastNSprints.HasValue && lastNSprints.Value > 0)
            {
                var summary = await _spillage.GetSpillageSummaryLast(projectName, lastNSprints.Value);
                return Ok(summary);
            }

            // timeframe mode
            var tf = timeframe ?? string.Empty;

            // If monthly/yearly bucketing is requested, call month-bucketing service methods.
            if (tf.Equals("monthly", System.StringComparison.OrdinalIgnoreCase) ||
                tf.Equals("yearly", System.StringComparison.OrdinalIgnoreCase))
            {
                // Sequential calls to avoid concurrent DbContext access
                var spillageAll = await _spillage.GetSpillageByMonthsAsync(projectName, tf, n, null);
                var spillageFeature = await _spillage.GetSpillageByMonthsAsync(projectName, tf, n, "Feature");
                var spillageClient = await _spillage.GetSpillageByMonthsAsync(projectName, tf, n, "Client Issue");

                var statsAll = await _spillage.GetSprintStatsByMonthsAsync(projectName, tf, n, null);
                var statsFeature = await _spillage.GetSprintStatsByMonthsAsync(projectName, tf, n, "Feature");
                var statsClient = await _spillage.GetSprintStatsByMonthsAsync(projectName, tf, n, "Client Issue");

                var historyAll = await _spillage.GetStoryHistoryByTimeframe(projectName, tf, null);
                var historyFeature = await _spillage.GetStoryHistoryByTimeframe(projectName, tf, "Feature");
                var historyClient = await _spillage.GetStoryHistoryByTimeframe(projectName, tf, "Client Issue");


                var summary = new SpillageSummaryDto
                {
                    All = new SectionDto { Stats = statsAll, Spillage = spillageAll, History = historyAll },
                    Feature = new SectionDto { Stats = statsFeature, Spillage = spillageFeature, History = historyFeature },
                    Client = new SectionDto { Stats = statsClient, Spillage = spillageClient, History = historyClient }
                };

                return Ok(summary);
            }

            // Fallback: use existing service summary-by-time behaviour
            var fallback = await _spillage.GetSpillageSummaryTime(projectName, tf);
            return Ok(fallback);
        }
    }
}



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
