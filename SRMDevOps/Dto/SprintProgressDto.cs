namespace SRMDevOps.Dto
{
    public class SprintProgressDto
    {
        public string IterationPath { get; set; }
        public double TotalPointsAssigned { get; set; }
        public double InitialPoints { get; set; }
        public double MidSprintAddedPoints { get; set; }
        public double TotalPointsCompleted { get; set; }
        public DateTime? SortDate { get; set; }
    }
}
