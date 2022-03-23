using BeerFactory;
using Hopster;
using Microsoft.AspNetCore.Mvc;

var builder = WebApplication.CreateBuilder(args);

// Add services to the container.
// Learn more about configuring Swagger/OpenAPI at https://aka.ms/aspnetcore/swashbuckle
builder.Services.AddEndpointsApiExplorer();
builder.Services.AddSwaggerGen();

builder.Services.Configure<JsonOptions>(options =>
{
    options.JsonSerializerOptions.PropertyNameCaseInsensitive = false;
    options.JsonSerializerOptions.PropertyNamingPolicy = null;
    options.JsonSerializerOptions.WriteIndented = true;
});

builder.Services.AddSingleton<Client>((sp) =>
{
    var httpClient = new HttpClient();
    httpClient.DefaultRequestHeaders.Add("apikey", "NQju7ZCKxEc7BhWEKwgc7Ll97i4Su7ic6zJx2JyLleo=");
    return new Client("http://hopster.m07039.clients.dev.nrk.no", httpClient);
});
builder.Services.AddSingleton<Bottling>();

var app = builder.Build();

// Configure the HTTP request pipeline.
if (app.Environment.IsDevelopment())
{
    app.UseSwagger();
    app.UseSwaggerUI();
}

app.MapPost("/bottles", async ([FromBody] Bottle bottle, [FromServices] Bottling bottling) =>
{
    Console.WriteLine($"Received bottle on HTTP");
    await bottling.BottleReceived(new BottleReceived(bottle));
})
.WithName("Bottles");

app.Run();