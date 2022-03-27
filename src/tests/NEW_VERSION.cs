using DaJet.Metadata;
using DaJet.Metadata.Model;
using Microsoft.Extensions.Options;
using Microsoft.VisualStudio.TestTools.UnitTesting;
using System;
using System.ComponentModel.DataAnnotations;
using System.ComponentModel.DataAnnotations.Schema;
using System.Reflection;
using System.Threading;

namespace DaJet.Data.Messaging.Test
{
    [TestClass] public class NEW_VERSION
    {
        private readonly InfoBase _infoBase;
        private const string MS_CONNECTION_STRING = "Data Source=zhichkin;Initial Catalog=dajet-messaging-ms;Integrated Security=True";
        private const string PG_CONNECTION_STRING = "Host=localhost;Port=5432;Database=dajet-messaging-pg;Username=postgres;Password=postgres;";
        public NEW_VERSION()
        {
            //if (!new MetadataService()
            //    .UseConnectionString(MS_CONNECTION_STRING)
            //    .UseDatabaseProvider(DatabaseProvider.SQLServer)
            //    .TryOpenInfoBase(out _infoBase, out string error))
            //{
            //    Console.WriteLine(error);
            //    return;
            //}

            if (!new MetadataService()
                .UseConnectionString(PG_CONNECTION_STRING)
                .UseDatabaseProvider(DatabaseProvider.PostgreSQL)
                .TryOpenInfoBase(out _infoBase, out string error))
            {
                Console.WriteLine(error);
                return;
            }
        }

        [TestMethod] public void Build()
        {
            Type contract = typeof(V1.OutgoingMessage);

            TableAttribute table = contract.GetCustomAttribute<TableAttribute>();
            if (table != null)
            {
                Console.WriteLine($"{table.Name}");
            }

            foreach (PropertyInfo property in contract.GetProperties())
            {
                KeyAttribute key = property.GetCustomAttribute<KeyAttribute>();
                ColumnAttribute column = property.GetCustomAttribute<ColumnAttribute>();

                if (column != null)
                {
                    Console.WriteLine($"{column.Order}. {(key != null ? "{key} " : string.Empty)}{column.Name} [{column.TypeName}]");
                }
            }
        }
        [TestMethod] public void MS_Test_Consumer()
        {
            ApplicationObject queue = _infoBase.GetApplicationObjectByName($"РегистрСведений.ИсходящаяОчередь1");

            DatabaseConsumerOptions options = new DatabaseConsumerOptions()
            {
                QueueTableName = queue.TableName,
                ConnectionString = MS_CONNECTION_STRING,
                MessagesPerTransaction = 1
            };

            foreach (MetadataProperty property in queue.Properties)
            {
                options.TableColumns.Add(property.Name, property.Fields[0].Name);
            }

            CancellationTokenSource source = new CancellationTokenSource();

            IDbMessageHandler handler = new Handlers.TestDbMessageHandler();
            handler
                .Use(new Handlers.MessageHeadersHandler())
                .Use(new Handlers.MessageTypeHandler())
                .Use(new Handlers.MessageBodyHandler());

            IDbMessageConsumer consumer = new SqlServer.MsMessageConsumer(Options.Create(options));

            consumer.Consume(in handler, source.Token);
        }
        [TestMethod] public void PG_Test_Consumer()
        {
            ApplicationObject queue = _infoBase.GetApplicationObjectByName($"РегистрСведений.ИсходящаяОчередь1");

            DatabaseConsumerOptions options = new DatabaseConsumerOptions()
            {
                QueueTableName = queue.TableName,
                ConnectionString = PG_CONNECTION_STRING,
                MessagesPerTransaction = 1
            };

            foreach (MetadataProperty property in queue.Properties)
            {
                options.TableColumns.Add(property.Name, property.Fields[0].Name);
            }

            CancellationTokenSource source = new CancellationTokenSource();

            IDbMessageHandler handler = new Handlers.TestDbMessageHandler();
            handler
                .Use(new Handlers.MessageHeadersHandler())
                .Use(new Handlers.MessageTypeHandler())
                .Use(new Handlers.MessageBodyHandler());

            IDbMessageConsumer consumer = new PostgreSQL.PgMessageConsumer(Options.Create(options));

            consumer.Consume(in handler, source.Token);
        }
    }
}