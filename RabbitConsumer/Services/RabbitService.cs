using Microsoft.Extensions.Configuration;
using RabbitMQ.AMQP.Client;
using RabbitMQ.AMQP.Client.Impl;


namespace RabbitConsumer.Services;

public class RabbitService
{
    private readonly IConfiguration _configuration;
    public RabbitService(IConfiguration configuration)
    {
        _configuration = configuration;
    }
    public async Task Run()
    {
        string brokerUri = _configuration["Rabbit:Conecction"]!;

        ConnectionSettings settings = ConnectionSettingsBuilder.Create()
            .Uri(new Uri(brokerUri))
            .ContainerId("tutorial-receive")
            .Build();

        IEnvironment environment = AmqpEnvironment.Create(settings);
        IConnection connection = await environment.CreateConnectionAsync();

        try
        {
            IManagement management = connection.Management();

            string[] queues =
            {
                _configuration["Rabbit:Ques:Overseas"]!,
                _configuration["Rabbit:Ques:South"]!,
                _configuration["Rabbit:Ques:North"]!,
                _configuration["Rabbit:Ques:Center"]!
            };

            foreach (string queue in queues)
            {
                IQueueSpecification queueSpec =
                    management.Queue(queue).Type(QueueType.QUORUM);

                await queueSpec.DeclareAsync();
            }

            var consumers = new List<IConsumer>();

            foreach (string queue in queues)
            {
                IConsumer consumer = await connection.ConsumerBuilder()
                    .Queue(queue)
                    .MessageHandler((ctx, message) =>
                    {
                        Console.WriteLine(
                            $"[{queue}] Received: {message.BodyAsString()}");

                        ctx.Accept();

                        return Task.CompletedTask;
                    })
                    .BuildAndStartAsync();

                consumers.Add(consumer);
            }

            try
            {
                Console.WriteLine(" [*] Waiting for messages. To exit press CTRL+C");

                using var cts = new CancellationTokenSource();

                Console.CancelKeyPress += (_, e) =>
                {
                    e.Cancel = true;
                    cts.Cancel();
                };

                await Task.Delay(Timeout.Infinite, cts.Token);
            }
            catch (OperationCanceledException)
            {
            }
            finally
            {
                foreach (IConsumer consumer in consumers)
                {
                    await consumer.CloseAsync();
                }
            }
        }
        finally
        {
            await connection.CloseAsync();
            await environment.CloseAsync();
        }
    }
}
