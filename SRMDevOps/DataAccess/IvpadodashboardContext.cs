using System;
using System.Collections.Generic;
using Microsoft.EntityFrameworkCore;
using SRMDevOps.Models;

namespace SRMDevOps.DataAccess;

public partial class IvpadodashboardContext : DbContext
{
    public IvpadodashboardContext()
    {
    }

    public IvpadodashboardContext(DbContextOptions<IvpadodashboardContext> options)
        : base(options)
    {
    }

    public virtual DbSet<IvpTaskAssignee> IvpTaskAssignees { get; set; }

    public virtual DbSet<IvpTaskDetail> IvpTaskDetails { get; set; }

    public virtual DbSet<IvpTaskIteration> IvpTaskIterations { get; set; }

    public virtual DbSet<IvpUserStoryAssignee> IvpUserStoryAssignees { get; set; }

    public virtual DbSet<IvpUserStoryDetail> IvpUserStoryDetails { get; set; }

    public virtual DbSet<IvpUserStoryIteration> IvpUserStoryIterations { get; set; }

    public virtual DbSet<VwIvpAssignedUserStory> VwIvpAssignedUserStories { get; set; }

    protected override void OnConfiguring(DbContextOptionsBuilder optionsBuilder)
#warning To protect potentially sensitive information in your connection string, you should move it out of source code. You can avoid scaffolding the connection string by using the Name= syntax to read it from configuration - see https://go.microsoft.com/fwlink/?linkid=2131148. For more guidance on storing connection strings, see https://go.microsoft.com/fwlink/?LinkId=723263.
        => optionsBuilder.UseSqlServer("Server=192.168.0.78\\dev13; Database=IVPADODashboard; User ID=sa; Password=sa@12345678; TrustServerCertificate=True;");

    protected override void OnModelCreating(ModelBuilder modelBuilder)
    {
        modelBuilder.Entity<IvpTaskAssignee>(entity =>
        {
            entity
                .HasNoKey()
                .ToTable("ivp_task_assignees");

            entity.Property(e => e.AssignedDate).HasColumnName("assigned_date");
            entity.Property(e => e.AssignedTo)
                .HasMaxLength(200)
                .HasColumnName("assigned_to");
            entity.Property(e => e.TaskId).HasColumnName("task_id");
        });

        modelBuilder.Entity<IvpTaskDetail>(entity =>
        {
            entity
                .HasNoKey()
                .ToTable("ivp_task_details");

            entity.Property(e => e.Activity)
                .HasMaxLength(50)
                .HasColumnName("activity");
            entity.Property(e => e.ClosedDate).HasColumnName("closed_date");
            entity.Property(e => e.CreationDate).HasColumnName("creation_date");
            entity.Property(e => e.DevEffort)
                .HasColumnType("decimal(18, 5)")
                .HasColumnName("dev_effort");
            entity.Property(e => e.FirstInprogressTime).HasColumnName("first_inprogress_time");
            entity.Property(e => e.IntialEffort)
                .HasColumnType("decimal(18, 5)")
                .HasColumnName("intial_effort");
            entity.Property(e => e.State)
                .HasMaxLength(50)
                .HasColumnName("state");
            entity.Property(e => e.TaskId).HasColumnName("task_id");
            entity.Property(e => e.Title)
                .HasMaxLength(500)
                .HasColumnName("title");
            entity.Property(e => e.UserStoryId).HasColumnName("user_story_id");
        });

        modelBuilder.Entity<IvpTaskIteration>(entity =>
        {
            entity
                .HasNoKey()
                .ToTable("ivp_task_iterations");

            entity.Property(e => e.AssignedDate).HasColumnName("assigned_date");
            entity.Property(e => e.IterationPath)
                .HasMaxLength(200)
                .HasColumnName("iteration_path");
            entity.Property(e => e.TaskId).HasColumnName("task_id");
        });

        modelBuilder.Entity<IvpUserStoryAssignee>(entity =>
        {
            entity
                .HasNoKey()
                .ToTable("ivp_user_story_assignees");

            entity.Property(e => e.AssignedDate).HasColumnName("assigned_date");
            entity.Property(e => e.AssignedTo)
                .HasMaxLength(200)
                .HasColumnName("assigned_to");
            entity.Property(e => e.UserStoryId).HasColumnName("user_story_id");
        });

        modelBuilder.Entity<IvpUserStoryDetail>(entity =>
        {
            entity
                .HasNoKey()
                .ToTable("ivp_user_story_details");

            entity.Property(e => e.AreaPath)
                .HasMaxLength(200)
                .HasColumnName("area_path");
            entity.Property(e => e.ClosedDate).HasColumnName("closed_date");
            entity.Property(e => e.CreationDate).HasColumnName("creation_date");
            entity.Property(e => e.DevEffort)
                .HasColumnType("decimal(18, 5)")
                .HasColumnName("dev_effort");
            entity.Property(e => e.FirstInprogressTime).HasColumnName("first_inprogress_time");
            entity.Property(e => e.LastUpdatedOn).HasColumnName("last_updated_on");
            entity.Property(e => e.ParentId).HasColumnName("parent_id");
            entity.Property(e => e.ParentType)
                .HasMaxLength(50)
                .HasColumnName("parent_type");
            entity.Property(e => e.Project)
                .HasMaxLength(50)
                .HasColumnName("project");
            entity.Property(e => e.State)
                .HasMaxLength(50)
                .HasColumnName("state");
            entity.Property(e => e.StoryPoints).HasColumnName("story_points");
            entity.Property(e => e.Title)
                .HasMaxLength(500)
                .HasColumnName("title");
            entity.Property(e => e.UserStoryId).HasColumnName("user_story_id");
        });

        modelBuilder.Entity<IvpUserStoryIteration>(entity =>
        {
            entity
                .HasNoKey()
                .ToTable("ivp_user_story_iterations");

            entity.Property(e => e.AssignedDate).HasColumnName("assigned_date");
            entity.Property(e => e.IterationPath)
                .HasMaxLength(200)
                .HasColumnName("iteration_path");
            entity.Property(e => e.UserStoryId).HasColumnName("user_story_id");
        });

        modelBuilder.Entity<VwIvpAssignedUserStory>(entity =>
        {
            entity
                .HasNoKey()
                .ToView("vw_ivp_assigned_user_stories");

            entity.Property(e => e.AreaPath)
                .HasMaxLength(200)
                .HasColumnName("area_path");
            entity.Property(e => e.AssignedDate).HasColumnName("assigned_date");
            entity.Property(e => e.AssignedTo)
                .HasMaxLength(200)
                .HasColumnName("assigned_to");
            entity.Property(e => e.ClosedDate).HasColumnName("closed_date");
            entity.Property(e => e.CreationDate).HasColumnName("creation_date");
            entity.Property(e => e.DevEffort)
                .HasColumnType("decimal(18, 5)")
                .HasColumnName("dev_effort");
            entity.Property(e => e.FirstInprogressTime).HasColumnName("first_inprogress_time");
            entity.Property(e => e.LastUpdatedOn).HasColumnName("last_updated_on");
            entity.Property(e => e.ParentId).HasColumnName("parent_id");
            entity.Property(e => e.ParentType)
                .HasMaxLength(50)
                .HasColumnName("parent_type");
            entity.Property(e => e.Project)
                .HasMaxLength(50)
                .HasColumnName("project");
            entity.Property(e => e.State)
                .HasMaxLength(50)
                .HasColumnName("state");
            entity.Property(e => e.StoryPoints).HasColumnName("story_points");
            entity.Property(e => e.Title)
                .HasMaxLength(500)
                .HasColumnName("title");
            entity.Property(e => e.UserStoryId).HasColumnName("user_story_id");
        });

        OnModelCreatingPartial(modelBuilder);
    }

    partial void OnModelCreatingPartial(ModelBuilder modelBuilder);
}
