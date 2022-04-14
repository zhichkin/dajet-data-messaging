using DaJet.Data.Messaging;
using DaJet.Metadata;
using DaJet.Metadata.Model;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Options;
using Serilog;
using Serilog.Events;
using System;
using Handlers = DaJet.Data.Messaging.Handlers;
using PostgreSQL = DaJet.Data.Messaging.PostgreSQL;
using SqlServer = DaJet.Data.Messaging.SqlServer;
using Logging = Microsoft.Extensions.Logging;

namespace test_app
{
    public static class Program
    {
        private const string MS_CONNECTION_STRING = "Data Source=zhichkin;Initial Catalog=dajet-messaging-ms;Integrated Security=True";
        private const string PG_CONNECTION_STRING = "Host=localhost;Port=5432;Database=dajet-messaging-pg;Username=postgres;Password=postgres;";

        private static DatabaseConsumerOptions _consumerOptions;
        private static DatabaseProducerOptions _producerOptions;

        public static void Main()
        {
            Log.Logger = new LoggerConfiguration()
                .MinimumLevel.Information()
                .MinimumLevel.Override("Microsoft", LogEventLevel.Warning)
                .WriteTo.File("dajet-agent.log", fileSizeLimitBytes: 1048576, rollOnFileSizeLimit: true,
                outputTemplate: "[{Timestamp:yyyy-MM-dd HH:mm:ss}] [{Level:u3}] {Message}{NewLine}{Exception}")
                .CreateLogger();

            try
            {
                Log.Information("Starting host ...");

                ConfigureConsumerOptions();
                ConfigureProducerOptions();

                Log.Information("Host is running");

                CreateHostBuilder().Build().Run();

                Log.Information("Host is stopped");
            }
            catch (Exception error)
            {
                Log.Fatal(error, "Failed to start host");
            }
            finally
            {
                Log.CloseAndFlush();
            }
        }
        private static void ConfigureConsumerOptions()
        {
            if (!new MetadataService()
                .UseConnectionString(MS_CONNECTION_STRING)
                .UseDatabaseProvider(DatabaseProvider.SQLServer)
                .TryOpenInfoBase(out InfoBase infoBase, out string error))
            {
                Console.WriteLine(error);
                return;
            }

            ApplicationObject queue = infoBase.GetApplicationObjectByName($"РегистрСведений.ИсходящаяОчередь1");

            _consumerOptions = new DatabaseConsumerOptions()
            {
                ConnectionString = MS_CONNECTION_STRING,
                DatabaseProvider = DatabaseProvider.SQLServer,
                YearOffset = infoBase.YearOffset,
                QueueTable = queue.TableName,
                MessagesPerTransaction = 1
            };

            foreach (MetadataProperty property in queue.Properties)
            {
                DatabaseField field = property.Fields[0];

                if (property.Name == "НомерСообщения" ||
                    property.Name == "МоментВремени" ||
                    property.Name == "Идентификатор")
                {
                    _consumerOptions.OrderColumns.Add(property.Name, field.Name);
                }

                _consumerOptions.TableColumns.Add(property.Name, field.Name);
            }
        }
        private static void ConfigureProducerOptions()
        {
            if (!new MetadataService()
                .UseConnectionString(PG_CONNECTION_STRING)
                .UseDatabaseProvider(DatabaseProvider.PostgreSQL)
                .TryOpenInfoBase(out InfoBase infoBase, out string error))
            {
                Console.WriteLine(error);
                return;
            }

            ApplicationObject queue = infoBase.GetApplicationObjectByName($"РегистрСведений.ВходящаяОчередь1");

            _producerOptions = new DatabaseProducerOptions()
            {
                ConnectionString = PG_CONNECTION_STRING,
                DatabaseProvider = DatabaseProvider.PostgreSQL,
                YearOffset = infoBase.YearOffset,
                QueueTableName = queue.TableName,
                SequenceObject = queue.TableName.ToLower() + "_so"
            };

            foreach (MetadataProperty property in queue.Properties)
            {
                _producerOptions.TableColumns.Add(property.Name, property.Fields[0].Name);
            }
        }
        private static IHostBuilder CreateHostBuilder()
        {
            IHostBuilder builder = Host
                .CreateDefaultBuilder()
                .ConfigureServices(ConfigureServices)
                .UseSerilog();

            return builder;
        }
        private static void ConfigureServices(HostBuilderContext context, IServiceCollection services)
        {
            services
                .AddOptions()
                .AddSingleton(Options.Create(_consumerOptions))
                .AddSingleton(Options.Create(_producerOptions));

            services.AddSingleton(DataMapperProvider.Factory);
            services.AddSingleton<SqlServer.MsMessageDataMapper>();
            services.AddSingleton<PostgreSQL.PgMessageDataMapper>();

            if (_consumerOptions.DatabaseProvider == DatabaseProvider.SQLServer)
            {
                services.AddSingleton<IDbMessageConsumer, SqlServer.MsMessageConsumer>();
            }
            else
            {
                services.AddSingleton<IDbMessageConsumer, PostgreSQL.PgMessageConsumer>();
            }

            if (_producerOptions.DatabaseProvider == DatabaseProvider.SQLServer)
            {
                services.AddSingleton<IDbMessageProducer, SqlServer.MsMessageProducer>();
            }
            else
            {
                services.AddSingleton<IDbMessageProducer, PostgreSQL.PgMessageProducer>();
            }

            //services.AddHostedService<MessageProducerService>();

            services.AddSingleton<IHostedService>(serviceProvider =>
            {
                IDbMessageConsumer consumer = serviceProvider.GetRequiredService<IDbMessageConsumer>();
                IDbMessageProducer producer = serviceProvider.GetRequiredService<IDbMessageProducer>();
                Logging.ILogger<MessageProducerService> logger = serviceProvider.GetRequiredService<Logging.ILogger<MessageProducerService>>();
                
                return new MessageProducerService(consumer, producer, logger);
            });

            //services.AddSingleton<Handlers.DbMessageProducerHandler>();
            //services.AddSingleton(serviceProvider =>
            //{
            //    IDbMessageHandler handler = serviceProvider.GetRequiredService<Handlers.DbMessageProducerHandler>();

            //    handler
            //    .Use(new Handlers.TestDbMessageHandler())
            //    .Use(new Handlers.MessageHeadersHandler())
            //    .Use(new Handlers.MessageTypeHandler())
            //    .Use(new Handlers.MessageBodyHandler());

            //    return handler;
            //});
            //services.AddHostedService<MessageConsumerService>();
        }
    }
}