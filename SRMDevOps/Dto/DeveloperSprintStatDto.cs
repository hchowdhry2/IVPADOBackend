namespace SRMDevOps.Dto
{
    public class DeveloperSprintStatDto
    {
        public string Sprint { get; set; }
        public string Developer { get; set; }
        public int TotalTasksAssigned { get; set; }
        public int TotalTasksCompleted { get; set; }
        public double TotalHours { get; set; }
        public DateTime? SprintStartDate { get; set; } // Add this for sorting
    }
}
