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

namespace test_app
{
    public static class Program
    {
        private static string CONNECTION_STRING;
        private static DatabaseProvider DATABASE_PROVIDER;

        private const string MS_CONNECTION_STRING = "Data Source=zhichkin;Initial Catalog=dajet-messaging-ms;Integrated Security=True";
        private const string PG_CONNECTION_STRING = "Host=localhost;Port=5432;Database=dajet-messaging-pg;Username=postgres;Password=postgres;";

        private static DatabaseConsumerOptions _consumerOptions;
        private static DatabaseProducerOptions _producerOptions;

        public static void Main()
        {
            CONNECTION_STRING = MS_CONNECTION_STRING;
            DATABASE_PROVIDER = DatabaseProvider.SQLServer;

            //CONNECTION_STRING = PG_CONNECTION_STRING;
            //DATABASE_PROVIDER = DatabaseProvider.PostgreSQL;

            Log.Logger = new LoggerConfiguration()
                .MinimumLevel.Information()
                .MinimumLevel.Override("Microsoft", LogEventLevel.Warning)
                .WriteTo.File("dajet-agent.log", fileSizeLimitBytes: 1048576,
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
                .UseDatabaseProvider(DATABASE_PROVIDER)
                .UseConnectionString(CONNECTION_STRING)
                .TryOpenInfoBase(out InfoBase infoBase, out string error))
            {
                Console.WriteLine(error);
                return;
            }

            ApplicationObject queue = infoBase.GetApplicationObjectByName($"РегистрСведений.ИсходящаяОчередь1");

            _consumerOptions = new DatabaseConsumerOptions()
            {
                ConnectionString = CONNECTION_STRING,
                YearOffset = infoBase.YearOffset,
                QueueTableName = queue.TableName,
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
                .UseDatabaseProvider(DATABASE_PROVIDER)
                .UseConnectionString(CONNECTION_STRING)
                .TryOpenInfoBase(out InfoBase infoBase, out string error))
            {
                Console.WriteLine(error);
                return;
            }

            ApplicationObject queue = infoBase.GetApplicationObjectByName($"РегистрСведений.ВходящаяОчередь1");

            _producerOptions = new DatabaseProducerOptions()
            {
                ConnectionString = CONNECTION_STRING,
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

            //IDbMessageHandler handler = new Handlers.TestDbMessageHandler();
            //handler
            //    .Use(new Handlers.MessageHeadersHandler())
            //    .Use(new Handlers.MessageTypeHandler())
            //    .Use(new Handlers.MessageBodyHandler());

            //services.AddSingleton(handler);

            if (DATABASE_PROVIDER == DatabaseProvider.SQLServer)
            {
                services.AddSingleton<IDbMessageConsumer, SqlServer.MsMessageConsumer>();
                services.AddSingleton<IDbMessageProducer, SqlServer.MsMessageProducer>();
                services.AddSingleton<IMessageDataMapper, SqlServer.MsMessageDataMapper>();
            }
            else
            {
                services.AddSingleton<IDbMessageConsumer, PostgreSQL.PgMessageConsumer>();
                services.AddSingleton<IDbMessageProducer, PostgreSQL.PgMessageProducer>();
                services.AddSingleton<IMessageDataMapper, PostgreSQL.PgMessageDataMapper>();
            }

            //services.AddHostedService<MessageConsumerService>();
            services.AddHostedService<MessageProducerService>();

            //TODO: consume MS message and produce it to PG =) or vice versa
        }
    }
}