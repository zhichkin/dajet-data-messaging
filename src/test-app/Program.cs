using DaJet.Data.Messaging;
using DaJet.Metadata;
using DaJet.Metadata.Model;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Options;
using System;
using Handlers = DaJet.Data.Messaging.Handlers;
using SqlServer = DaJet.Data.Messaging.SqlServer;

namespace test_app
{
    public static class Program
    {
        private static DatabaseConsumerOptions _options;
        private const string MS_CONNECTION_STRING = "Data Source=zhichkin;Initial Catalog=dajet-messaging-ms;Integrated Security=True";
        private const string PG_CONNECTION_STRING = "Host=localhost;Port=5432;Database=dajet-messaging-pg;Username=postgres;Password=postgres;";

        public static void Main()
        {
            ConfigureConsumerOptions();

            CreateHostBuilder().Build().Run();
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

            _options = new DatabaseConsumerOptions()
            {
                DatabaseProvider = "ms",
                ConnectionString = MS_CONNECTION_STRING,
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
                    _options.OrderColumns.Add(property.Name, field.Name);
                }

                _options.TableColumns.Add(property.Name, field.Name);
            }
        }
        private static IHostBuilder CreateHostBuilder()
        {
            IHostBuilder builder = Host
                .CreateDefaultBuilder()
                .ConfigureServices(ConfigureServices);

            return builder;
        }
        private static void ConfigureServices(HostBuilderContext context, IServiceCollection services)
        {
            services
                .AddOptions()
                .AddSingleton(Options.Create(_options));

            IDbMessageHandler handler = new Handlers.TestDbMessageHandler();
            handler
                .Use(new Handlers.MessageHeadersHandler())
                .Use(new Handlers.MessageTypeHandler())
                .Use(new Handlers.MessageBodyHandler());

            services.AddSingleton(handler);

            if (_options.DatabaseProvider == "ms")
            {
                services.AddSingleton<IDbMessageConsumer, SqlServer.MsMessageConsumer>();
                services.AddSingleton<IMessageDataMapper, SqlServer.MsMessageDataMapper>();
                
                //TODO: incoming message data mapper ?
            }
            
            services.AddHostedService<MessageConsumerService>();
        }
    }
}