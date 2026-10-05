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

        //var services = new ServiceCollection();



        var kafkaService = new KafkaService(configuration["Kafka:BootstrapServers"]!);
        //services.AddScoped<KafkaService>();

        var watcher = new FileWatchingService(kafkaService);

        watcher.Watch();
    }
}