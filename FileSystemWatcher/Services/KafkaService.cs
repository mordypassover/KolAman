using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Text.Json;
using System.Threading.Tasks;
using Confluent.Kafka;
using CsFileSystemWatcher.Models;
using Microsoft.Extensions.Configuration;

namespace CsFileSystemWatcher.Services;

public class KafkaService
{
    private readonly string _bootStrap;

    public KafkaService(string bootStrap)
    {
        _bootStrap = bootStrap;
    }

    public void Produce(string topic, RawMesege mesege)
    {
        var config = new ProducerConfig
        {
            BootstrapServers = _bootStrap
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
}
