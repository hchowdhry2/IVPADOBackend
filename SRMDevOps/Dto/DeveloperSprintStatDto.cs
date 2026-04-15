namespace SRMDevOps.Dto
{
    public class DeveloperSprintStatDto
    {
        public string Developer { get; set; }
        public string Sprint { get; set; }
        public int TotalTasksAssigned { get; set; }
        public int TotalTasksCompleted { get; set; }
        public double TotalHours { get; set; }
    }
}
