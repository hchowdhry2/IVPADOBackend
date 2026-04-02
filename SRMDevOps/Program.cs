using SRMDevOps.DataAccess;
using SRMDevOps.Repo;
using System.Text.Json;

var builder = WebApplication.CreateBuilder(args);

// Add services to the container.

builder.Services.AddControllers()
    .AddJsonOptions(options =>
    {
        options.JsonSerializerOptions.WriteIndented = true; // Makes JSON "Pretty Printed"
        options.JsonSerializerOptions.PropertyNamingPolicy = JsonNamingPolicy.CamelCase;
    });
builder.Services.AddDbContext<IvpadodashboardContext>();
builder.Services.AddTransient<ISpillage, SpillageService>();
builder.Services.AddTransient<IADO, DevopsService>();
builder.Services.AddScoped<ITask, TaskService>();

builder.Services.AddCors(options =>
    options.AddPolicy(
        "SRMCorsPolicy",
        policy => policy
            .AllowAnyOrigin()
            .AllowAnyHeader()
            .AllowAnyMethod()
    )
);

// Learn more about configuring Swagger/OpenAPI at https://aka.ms/aspnetcore/swashbuckle
builder.Services.AddEndpointsApiExplorer();
builder.Services.AddSwaggerGen();

var app = builder.Build();

// Configure the HTTP request pipeline.
if (app.Environment.IsDevelopment())
{
    app.UseSwagger();
    app.UseSwaggerUI();
}

app.UseHttpsRedirection();

// Apply CORS policy
app.UseCors("SRMCorsPolicy");

app.UseAuthorization();

app.MapControllers();

app.Run();
