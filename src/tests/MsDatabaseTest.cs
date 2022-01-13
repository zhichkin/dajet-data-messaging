using DaJet.Metadata;
using DaJet.Metadata.Model;
using Microsoft.VisualStudio.TestTools.UnitTesting;
using System;
using System.Collections.Generic;

namespace DaJet.Data.Messaging.Test
{
    [TestClass] public class MsDatabaseTest
    {
        private const string MS_CONNECTION_STRING = "Data Source=zhichkin;Initial Catalog=dajet_agent;Integrated Security=True";

        [TestMethod] public void IncomingQueueSetup()
        {
            if (!new MetadataService()
                .UseDatabaseProvider(DatabaseProvider.SQLServer)
                .UseConnectionString(MS_CONNECTION_STRING)
                .TryOpenInfoBase(out InfoBase infoBase, out string message))
            {
                Console.WriteLine(message);
                return;
            }

            ApplicationObject queue = infoBase.GetApplicationObjectByName("–егистр—ведений.¬ход€ща€ќчередьKafka");

            MsQueueConfigurator configurator = new MsQueueConfigurator(MS_CONNECTION_STRING);
            configurator.ConfigureIncomingMessageQueue(in queue, out List<string> errors);

            if (errors.Count > 0)
            {
                foreach (string error in errors)
                {
                    Console.WriteLine(error);
                }
            }
            else
            {
                Console.WriteLine("Incoming queue configured successfully.");
            }
        }
        [TestMethod] public void OutgoingQueueSetup()
        {
            if (!new MetadataService()
                .UseDatabaseProvider(DatabaseProvider.SQLServer)
                .UseConnectionString(MS_CONNECTION_STRING)
                .TryOpenInfoBase(out InfoBase infoBase, out string message))
            {
                Console.WriteLine(message);
                return;
            }

            ApplicationObject queue = infoBase.GetApplicationObjectByName("–егистр—ведений.»сход€ща€ќчередьKafka");

            MsQueueConfigurator configurator = new MsQueueConfigurator(MS_CONNECTION_STRING);
            configurator.ConfigureOutgoingMessageQueue(in queue, out List<string> errors);

            if (errors.Count > 0)
            {
                foreach (string error in errors)
                {
                    Console.WriteLine(error);
                }
            }
            else
            {
                Console.WriteLine("Outgoing queue configured successfully.");
            }
        }
        [TestMethod] public void DbInterfaceValidation()
        {
            if (!new MetadataService()
                .UseDatabaseProvider(DatabaseProvider.SQLServer)
                .UseConnectionString(MS_CONNECTION_STRING)
                .TryOpenInfoBase(out InfoBase infoBase, out string error))
            {
                Console.WriteLine(error);
                return;
            }

            int version = -1;
            ApplicationObject incomingQueue = infoBase.GetApplicationObjectByName("–егистр—ведений.¬ход€ща€ќчередьKafka");
            ApplicationObject outgoingQueue = infoBase.GetApplicationObjectByName("–егистр—ведений.»сход€ща€ќчередьKafka");

            DbInterfaceValidator validator = new DbInterfaceValidator();
            
            version = validator.GetIncomingInterfaceVersion(in incomingQueue);
            Console.WriteLine($"Incoming queue version = {version}");
            
            version = validator.GetOutgoingInterfaceVersion(in outgoingQueue);
            Console.WriteLine($"Outgoing queue version = {version}");
        }
        [TestMethod] public void IncomingInsertScript()
        {
            if (!new MetadataService()
                .UseDatabaseProvider(DatabaseProvider.SQLServer)
                .UseConnectionString(MS_CONNECTION_STRING)
                .TryOpenInfoBase(out InfoBase infoBase, out string error))
            {
                Console.WriteLine(error);
                return;
            }
            ApplicationObject queue = infoBase.GetApplicationObjectByName("–егистр—ведений.¬ход€ща€ќчередьKafka");

            QueryBuilder builder = new QueryBuilder(DatabaseProvider.SQLServer);

            Console.WriteLine($"{builder.BuildIncomingQueueInsertScript(in queue)}");
        }
        [TestMethod] public void OutgoingSelectScript()
        {
            if (!new MetadataService()
                .UseDatabaseProvider(DatabaseProvider.SQLServer)
                .UseConnectionString(MS_CONNECTION_STRING)
                .TryOpenInfoBase(out InfoBase infoBase, out string error))
            {
                Console.WriteLine(error);
                return;
            }
            ApplicationObject queue = infoBase.GetApplicationObjectByName("–егистр—ведений.»сход€ща€ќчередьKafka");

            QueryBuilder builder = new QueryBuilder(DatabaseProvider.SQLServer);

            Console.WriteLine($"{builder.BuildOutgoingQueueSelectScript(in queue)}");
        }

        private IEnumerable<IncomingMessage> GetTestIncomingMessages()
        {
            for (int i = 0; i < 10; i++)
            {
                yield return new IncomingMessage()
                {
                    Uuid = Guid.NewGuid(),
                    Sender = "Kafka",
                    Headers = string.Empty,
                    MessageType = "test",
                    MessageBody = $"{{ \"message\": {(i + 1)} }}",
                    OperationType = "INSERT",
                    DateTimeStamp = DateTime.Now
                };
            }
        }
        [TestMethod] public void MessageProducer_Insert()
        {
            if (!new MetadataService()
                .UseDatabaseProvider(DatabaseProvider.SQLServer)
                .UseConnectionString(MS_CONNECTION_STRING)
                .TryOpenInfoBase(out InfoBase infoBase, out string error))
            {
                Console.WriteLine(error);
                return;
            }

            ApplicationObject queue = infoBase.GetApplicationObjectByName("–егистр—ведений.¬ход€ща€ќчередьKafka");

            using (MsMessageProducer producer = new MsMessageProducer(MS_CONNECTION_STRING, queue, infoBase.YearOffset))
            {
                foreach (IncomingMessage message in GetTestIncomingMessages())
                {
                    producer.Insert(in message);
                }

                producer.TxBegin();
                foreach (IncomingMessage message in GetTestIncomingMessages())
                {
                    producer.Insert(in message);
                }
                producer.TxCommit();

                producer.TxBegin();
                foreach (IncomingMessage message in GetTestIncomingMessages())
                {
                    producer.Insert(in message);
                }
                producer.TxCommit();
            }
        }
    }
}