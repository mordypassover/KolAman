using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;
using RabbitConsumer.Services;
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

        var providor = services.BuildServiceProvider();

        using (var scope = providor.CreateScope())
        {
            var reader = scope.ServiceProvider.GetRequiredService<RabbitService>();
            
            await reader.Run()
            
            
            ;

        }
    }
}