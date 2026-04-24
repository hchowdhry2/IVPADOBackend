using SRMDevOps.Repo;

namespace SRMDevOps.Dto
{
    public class EffortVarianceDto
    {
        public string Developer { get; set; }
        public double ActualEffort { get; set; }    // DevEffort * 7
        public DateTime? SortDate { get; set; }
    }
}
