using System;
using System.Collections.Generic;
using System.Linq;
using System.Security.Cryptography.X509Certificates;
using System.Text;
using System.Text.Json;
using System.Threading.Tasks;
using Confluent.Kafka;
using CsFileSystemWatcher.Models;
using Microsoft.Extensions.Configuration;

namespace CsFileSystemWatcher.Services;

public class KafkaService
{
    private readonly IConfiguration _configuration;


    public KafkaService(IConfiguration configuration)
    {
        _configuration = configuration;
    }

    public void Produce(string topic, RawMesege mesege)
    {
        var config = new ProducerConfig
        {
            BootstrapServers = _configuration["Kafka:BootstrapServers"]
        };

        using (var producer = new ProducerBuilder<Null, string>(config).Build())
        {

            var mesegeString = JsonSerializer.Serialize(mesege);
            if (mesegeString == null)
            {
                throw new Exception("Mesege not valid!");
            }
            producer.Produce(topic, new Message<Null, string> { Value = mesegeString });


            producer.Flush(TimeSpan.FromSeconds(10));
        }
       
    }
    public void Log(string level, string mesege)
    {
        var config = new ProducerConfig
        {
            BootstrapServers = _configuration["Kafka:BootstrapServers"]
        };

        using (var producer = new ProducerBuilder<Null, string>(config).Build())
        {
            var mesegeString = $"[{level}], [CsFileSystemWatcher], {mesege}, {DateTime.Now}";
            producer.Produce("logs", new Message<Null, string> { Value = mesegeString });
            Console.WriteLine($"logged mesege{mesege}");
            producer.Flush();
        }


    }
}
