using Microsoft.EntityFrameworkCore;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;
using RabbitConsumer.Data;
using RabbitConsumer.Services;
using System;
using System.Collections;

class Program
{
    static async Task Main()
    {
        var services = new ServiceCollection();
        var configuration = new ConfigurationBuilder()
           .SetBasePath(Directory.GetCurrentDirectory())
           .AddJsonFile("Appsettings.json")
           .Build();

        services.AddSingleton<IConfiguration>(configuration);

        services.AddScoped<RabbitService>();
        services.AddScoped<DbManeger>();


        services.AddDbContext<MyDbContext>(options => options.UseMySql(configuration["MySQL:ConnectionString"], ServerVersion
                .AutoDetect(configuration["MySQL:ConnectionString"])));

        var providor = services.BuildServiceProvider();

        using (var scope = providor.CreateScope())
        {
            var db = scope.ServiceProvider.GetService<MyDbContext>();
            db.Database.EnsureCreated();
        }

        using (var scope = providor.CreateScope())
        {
            var reader = scope.ServiceProvider.GetRequiredService<RabbitService>();
            
            await reader.Run();

        }
    }
}