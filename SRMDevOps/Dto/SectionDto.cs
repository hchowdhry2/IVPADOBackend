using SRMDevOps.Repo;

namespace SRMDevOps.Dto
{
    // Represents one segment (All / Feature / Client) with stats, spillage/timeline and optional story history.
    public class SectionDto
    {
        public List<SprintProgressDto> Stats { get; init; } = new();
        public List<SpillageTrendDto> Spillage { get; init; } = new();

        // New: per-story history relevant to this section (empty when not requested)
        public List<ParentImpactDto> History { get; init; } = new();
        public List<SprintDailyTrendDto> DailyTrends { get; init; } = new();

        public List<DeveloperSprintStatDto> DeveloperStats { get; set; } = new();

        public List<EffortVarianceDto> EffortVariance { get; set; } = new();
        public List<DeveloperSprintActivityDto> DeveloperActivityStats { get; set; } = new();
    }
}
