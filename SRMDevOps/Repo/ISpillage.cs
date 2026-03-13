using Microsoft.AspNetCore.Mvc;
using SRMDevOps.Dto;

namespace SRMDevOps.Repo
{
    public interface ISpillage
    {

        // Aggregated business methods (summary) — unchanged for last-N
        Task<SpillageSummaryDto> GetSpillageSummaryLast(string projectName, int lastNSprints);

        // Updated summary-by-time: supports periodUnit ("monthly","quarterly","yearly") and n = number of periods
        Task<SpillageSummaryDto> GetSpillageSummaryTime(string projectName, string? periodUnit = null, int? n = null);

    }
}
