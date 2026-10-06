using CsFileSystemWatcher.Services;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;

namespace CsFileSystemWatcher;

class Program
{
    static void Main()
    {
        var configuration = new ConfigurationBuilder()
            .SetBasePath(Directory.GetCurrentDirectory())
            .AddJsonFile("Appsettings.json")
            .Build();

        var services = new ServiceCollection();
        services.AddSingleton<IConfiguration>(configuration);
        services.AddScoped<KafkaService>();
        services.AddScoped<FileWatchingService>();



        var providor = services.BuildServiceProvider();

        using (var scope = providor.CreateScope())
        {
            var watcher = scope.ServiceProvider.GetRequiredService<FileWatchingService>();
            try
            {
                watcher.Watch();
            }
            catch (Exception ex)
            {
                Console.WriteLine(ex);
            }
        }

    }
}