namespace SRMDevOps.Dto
{
    // Represents one segment (All / Feature / Client) with stats, spillage/timeline and optional story history.
    public class SectionDto
    {
        public List<SprintProgressDto> Stats { get; init; } = new();
        public List<SpillageTrendDto> Spillage { get; init; } = new();

        // New: per-story history relevant to this section (empty when not requested)
        public List<StoryHistoryDto> History { get; init; } = new();
    }
}
