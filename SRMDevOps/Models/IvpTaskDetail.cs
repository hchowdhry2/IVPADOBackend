using System;
using System.Collections.Generic;

namespace SRMDevOps.Models;

public partial class IvpTaskDetail
{
    public int TaskId { get; set; }

    public int? UserStoryId { get; set; }

    public DateTime CreationDate { get; set; }

    public DateTime? FirstInprogressTime { get; set; }

    public DateTime? ClosedDate { get; set; }

    public string? Title { get; set; }

    public string? State { get; set; }

    public decimal? DevEffort { get; set; }

    public decimal? IntialEffort { get; set; }

    public string? Activity { get; set; }
}
