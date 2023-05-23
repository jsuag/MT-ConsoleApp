// See https://aka.ms/new-console-template for more information
using JobEntities;
using MassTransit;
using Microsoft.Extensions.Hosting;
using Serilog;

using IHost host = Host.CreateDefaultBuilder(args)
    .UseSerilog((host, log) => 
    {
        log.MinimumLevel.Verbose();
        log.WriteTo.Console();
    })
    .ConfigureServices(services =>
    {
        services.AddMassTransit(x => 
        {
            x.AddConsumer<JobMessageConsumer, JobMessageConsumerDefinition>();

            x.UsingAmazonSqs((context, cfg) =>
            {
                cfg.Host("region", amazon =>
                {
                    amazon.AccessKey("key");
                    amazon.SecretKey("secret");
                });

                cfg.ConfigureEndpoints(context);
            });
        });
    })    
    .Build();

await host.RunAsync();

class JobMessageConsumer : IConsumer<JobMessage>
{
    public Task Consume(ConsumeContext<JobMessage> context)
    {
        Console.WriteLine($"Processed Job Id: {context.Message.job_id}");
        return Task.CompletedTask;
    }
}

class JobMessageConsumerDefinition : ConsumerDefinition<JobMessageConsumer>
{
    public JobMessageConsumerDefinition()
    {
        EndpointName = "aws-raw-message-sqs";
    }

    protected override void ConfigureConsumer(IReceiveEndpointConfigurator endpointConfigurator, IConsumerConfigurator<JobMessageConsumer> consumerConfigurator)
    {
        //endpointConfigurator.ConfigureConsumeTopology = false;
        //endpointConfigurator.ClearSerialization();

        endpointConfigurator.DefaultContentType = new System.Net.Mime.ContentType("application/json");
        endpointConfigurator.UseRawJsonSerializer();

        //This config is just to see the skipped message
        endpointConfigurator.ConfigureDeadLetter(x => x.UseFilter(new JobMessageFilter()));
    }
}

class JobMessageFilter : IFilter<ReceiveContext>
{
    public async Task Send(ReceiveContext context, IPipe<ReceiveContext> next)
    {
        Console.WriteLine(System.Text.Encoding.Default.GetString(context.GetBody()));
        await next.Send(context);
    }

    public void Probe(ProbeContext context) { }
}

namespace JobEntities
{
    record JobMessage
    {
        public int job_id { get; set; }
    }
}
