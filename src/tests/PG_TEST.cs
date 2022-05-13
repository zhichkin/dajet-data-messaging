using DaJet.Metadata;
using DaJet.Metadata.Model;
using Microsoft.VisualStudio.TestTools.UnitTesting;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Reflection;

namespace DaJet.Data.Messaging.Test
{
    [TestClass] public class PG_TEST
    {
        private readonly InfoBase _infoBase;
        private const string PG_CONNECTION_STRING = "Host=localhost;Port=5432;Database=dajet-messaging-pg;Username=postgres;Password=postgres;";

        private readonly DbInterfaceValidator _validator = new DbInterfaceValidator();
        private readonly QueryBuilder _builder = new QueryBuilder(DatabaseProvider.PostgreSQL);

        public PG_TEST()
        {
            if (!new MetadataService()
                .UseDatabaseProvider(DatabaseProvider.PostgreSQL)
                .UseConnectionString(PG_CONNECTION_STRING)
                .TryOpenInfoBase(out _infoBase, out string error))
            {
                Console.WriteLine(error);
                return;
            }
        }

        [TestMethod] public void Validate_DbInterface_Incoming()
        {
            ApplicationObject queue;

            List<int> versions = new List<int>() { 1, 10, 11, 12 };

            foreach (int version in versions)
            {
                queue = _infoBase.GetApplicationObjectByName($"–егистр—ведений.¬ход€ща€ќчередь{version}");

                if (queue != null)
                {
                    Assert.AreEqual(version, _validator.GetIncomingInterfaceVersion(queue));

                    Console.WriteLine($"Incoming queue data contract version {version} is valid.");
                }
            }
        }
        [TestMethod] public void Validate_DbInterface_Outgoing()
        {
            ApplicationObject queue;

            List<int> versions = new List<int>() { 1, 10, 11, 12 };

            foreach (int version in versions)
            {
                queue = _infoBase.GetApplicationObjectByName($"–егистр—ведений.»сход€ща€ќчередь{version}");

                if (queue != null)
                {
                    Assert.AreEqual(version, _validator.GetOutgoingInterfaceVersion(queue));

                    Console.WriteLine($"Outgoing queue data contract version {version} is valid.");
                }
            }
        }

        [TestMethod] public void Script_IncomingInsert()
        {
            List<int> versions = new List<int>() { 1, 10, 11, 12 };

            foreach (int version in versions)
            {
                ApplicationObject queue = _infoBase.GetApplicationObjectByName($"–егистр—ведений.¬ход€ща€ќчередь{version}");

                Console.WriteLine($"{_builder.BuildIncomingQueueInsertScript(in queue, IncomingMessageDataMapper.Create(version))}");
                Console.WriteLine();
            }
        }
        [TestMethod] public void Script_OutgoingSelect()
        {
            List<int> versions = new List<int>() { 1, 10, 11, 12 };

            foreach (int version in versions)
            {
                ApplicationObject queue = _infoBase.GetApplicationObjectByName($"–егистр—ведений.»сход€ща€ќчередь{version}");

                Console.WriteLine($"{_builder.BuildOutgoingQueueSelectScript(in queue, OutgoingMessageDataMapper.Create(version))}");
                Console.WriteLine();
            }
        }

        [TestMethod] public void Configure_IncomingQueue()
        {
            ApplicationObject queue;

            List<int> versions = new List<int>() { 1, 10, 11, 12 };

            foreach (int version in versions)
            {
                queue = _infoBase.GetApplicationObjectByName($"–егистр—ведений.¬ход€ща€ќчередь{version}");

                Assert.AreEqual(version, _validator.GetIncomingInterfaceVersion(queue));
                Console.WriteLine($"Incoming queue data contract version {version} is valid.");

                DbQueueConfigurator configurator = new DbQueueConfigurator(version, DatabaseProvider.PostgreSQL, PG_CONNECTION_STRING);
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
                    Console.WriteLine($"Incoming queue [{queue.TableName}] configured successfully.");
                }

                Console.WriteLine();
            }
        }
        [TestMethod] public void Configure_OutgoingQueue()
        {
            ApplicationObject queue;

            List<int> versions = new List<int>() { 1, 10, 11, 12 };

            foreach (int version in versions)
            {
                queue = _infoBase.GetApplicationObjectByName($"–егистр—ведений.»сход€ща€ќчередь{version}");

                Assert.AreEqual(version, _validator.GetOutgoingInterfaceVersion(queue));
                Console.WriteLine($"Outgoing queue data contract version {version} is valid.");

                DbQueueConfigurator configurator = new DbQueueConfigurator(version, DatabaseProvider.PostgreSQL, PG_CONNECTION_STRING);
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
                    Console.WriteLine($"Outgoing queue [{queue.TableName}] configured successfully.");
                }

                Console.WriteLine();
            }
        }

        long current = 0L;
        private IEnumerable<IncomingMessageDataMapper> GetTestIncomingMessages(int version)
        {
            for (int i = 0; i < 10; i++)
            {
                if (version == 1)
                {
                    yield return new V1.IncomingMessage()
                    {
                        Sender = "DaJet",
                        Headers = "{ \"version\": \"1\" }",
                        MessageType = "version 1",
                        MessageBody = $"{{ \"message\": {(i + 1)} }}",
                        DateTimeStamp = DateTime.Now.AddYears(_infoBase.YearOffset)
                    };
                }
                else if (version == 10)
                {
                    yield return new V10.IncomingMessage()
                    {
                        Uuid = Guid.NewGuid(),
                        Sender = "DaJet",
                        OperationType = "INSERT",
                        MessageType = "version 10",
                        MessageBody = $"{{ \"message\": {(i + 1)} }}",
                        DateTimeStamp = DateTime.Now.AddYears(_infoBase.YearOffset),
                        ErrorCount = 0,
                        ErrorDescription = string.Empty
                    };
                }
                else if (version == 11)
                {
                    yield return new V11.IncomingMessage()
                    {
                        Uuid = Guid.NewGuid(),
                        Sender = "DaJet",
                        Headers = "{ \"version\": \"11\" }",
                        MessageType = "version 11",
                        MessageBody = $"{{ \"message\": {(i + 1)} }}",
                        DateTimeStamp = DateTime.Now.AddYears(_infoBase.YearOffset),
                        ErrorCount = 0,
                        ErrorDescription = string.Empty,
                        OperationType = "INSERT",
                    };
                }
                else if (version == 12)
                {
                    yield return new V12.IncomingMessage()
                    {
                        Sender = "DaJet",
                        Headers = "{ \"version\": \"12\" }",
                        MessageType = "version 12",
                        MessageBody = $"{{ \"message\": {(i + 1)} }}",
                        DateTimeStamp = DateTime.Now.AddYears(_infoBase.YearOffset),
                        ErrorCount = 0,
                        ErrorDescription = string.Empty
                    };
                }
            }
        }
        [TestMethod] public void MessageProducer_Insert()
        {
            int total = 0;

            ApplicationObject queue;
            List<int> versions = new List<int>() { 1, 10, 11, 12 };

            foreach (int version in versions)
            {
                queue = _infoBase.GetApplicationObjectByName($"–егистр—ведений.¬ход€ща€ќчередь{version}");

                Console.WriteLine($"Produce, version {version}: {queue.Name} [{queue.TableName}]");

                using (IMessageProducer producer = new PgMessageProducer(PG_CONNECTION_STRING, in queue))
                {
                    int count = 0;

                    foreach (IncomingMessageDataMapper message in GetTestIncomingMessages(version))
                    {
                        producer.Insert(in message); count++; total++;
                    }

                    Console.WriteLine($"Count [{version}] = {count}");
                }

                Console.WriteLine($"Total [{version}] = {total}");
                Console.WriteLine();
            }
        }
        [TestMethod] public void MessageConsumer_Select()
        {
            int total = 0;
            
            ApplicationObject queue;
            List<int> versions = new List<int>() { 10 }; //{ 1, 10, 11, 12 };

            do
            {
                foreach (int version in versions)
                {
                    queue = _infoBase.GetApplicationObjectByName($"–егистр—ведений.»сход€ща€ќчередь{version}");

                    Console.WriteLine($"Consume, version {version}: {queue.Name} [{queue.TableName}]");

                    using (IMessageConsumer consumer = new PgMessageConsumer(PG_CONNECTION_STRING, in queue))
                    {
                        do
                        {
                            total = 0;
                            consumer.TxBegin();
                            foreach (OutgoingMessageDataMapper message in consumer.Select())
                            {
                                ShowMessageData(in message);
                                total++;
                            }
                            consumer.TxCommit();

                            Console.WriteLine($"Count [{version}] = {consumer.RecordsAffected}");
                        }
                        while (consumer.RecordsAffected > 0);
                    }
                }

                //Console.WriteLine($"Total [{version}] = {total}");
                //Console.WriteLine();
            }
            while (total > 0);

            Console.WriteLine($"Total = {total}");
        }
        private void ShowMessageData(in OutgoingMessageDataMapper message)
        {
            V10.OutgoingMessage msg = message as V10.OutgoingMessage;

            if (msg == null) return;

            if (current == 0L)
            {
                current = msg.MessageNumber;
                return;
            }

            if (current > msg.MessageNumber)
            {
                Console.WriteLine($"{current} > {msg.MessageNumber}");
            }

            current = msg.MessageNumber;

            //Type type = message.GetType();

            //foreach (PropertyInfo property in type.GetProperties())
            //{
            //    Console.WriteLine($"{property.Name} = {property.GetValue(message)}");
            //}

            //Console.WriteLine();
        }

        [TestMethod] public void Settings_Publication()
        {
            ApplicationObject metadata = _infoBase.GetApplicationObjectByName("ѕланќбмена.DaJetMessaging");

            Console.WriteLine($"ѕлан обмена: {metadata.Name}");

            Publication publication = metadata as Publication;
            TablePart publications = publication.TableParts.Where(t => t.Name == "»сход€щие—ообщени€").FirstOrDefault();
            TablePart subscriptions = publication.TableParts.Where(t => t.Name == "¬ход€щие—ообщени€").FirstOrDefault();

            PublicationSettings settings = new PublicationSettings(DatabaseProvider.PostgreSQL, PG_CONNECTION_STRING);
            settings.Select(in publication);

            Console.WriteLine($"”зел: {publication.Publisher.Code} ({publication.Publisher.Name})");

            foreach (Subscriber subscriber in publication.Subscribers)
            {
                Console.WriteLine($"- {subscriber.Code} : {subscriber.Name}");
            }

            PublicationNode node = settings.Select(in publication, publication.Publisher.Uuid);
            Console.WriteLine();
            Console.WriteLine($"{node.Code} : {node.Name} ({(node.IsActive ? "active" : "idle")})");
            Console.WriteLine($"Broker: {node.BrokerServer}");
            Console.WriteLine($"Node IN: {node.NodeIncomingQueue}");
            Console.WriteLine($"Node OUT: {node.NodeOutgoingQueue}");
            Console.WriteLine($"Broker IN: {node.BrokerIncomingQueue}");
            Console.WriteLine($"Broker OUT: {node.BrokerOutgoingQueue}");
            Console.WriteLine();

            Console.WriteLine($"Publications:");
            node.Publications = settings.SelectNodePublications(publications, node.Uuid);
            foreach (NodePublication item in node.Publications)
            {
                Console.WriteLine($"* “ип сообщени€: {item.MessageType}");
                Console.WriteLine($"* ќчередь cообщений узла: {item.NodeQueue}");
                Console.WriteLine($"* ќчередь cообщений брокера: {item.BrokerQueue}");
            }
            Console.WriteLine();

            Console.WriteLine($"Subscriptions:");
            node.Subscriptions = settings.SelectNodeSubscriptions(subscriptions, node.Uuid);
            foreach (NodeSubscription item in node.Subscriptions)
            {
                Console.WriteLine($"* “ип сообщени€: {item.MessageType}");
                Console.WriteLine($"* ќчередь cообщений узла: {item.NodeQueue}");
                Console.WriteLine($"* ќчередь cообщений брокера: {item.BrokerQueue}");
            }
            Console.WriteLine();
        }
        [TestMethod] public void SelectMainNode()
        {
            PublicationSettings settings = new PublicationSettings(DatabaseProvider.PostgreSQL, PG_CONNECTION_STRING);
            settings.SelectMainNode("DaJetMessaging", out PublicationNode node);

            Console.WriteLine();
            Console.WriteLine($"{node.Code} : {node.Name} ({(node.IsActive ? "active" : "idle")})");
            Console.WriteLine($"Broker: {node.BrokerServer}");
            Console.WriteLine($"Node IN: {node.NodeIncomingQueue}");
            Console.WriteLine($"Node OUT: {node.NodeOutgoingQueue}");
            Console.WriteLine($"Broker IN: {node.BrokerIncomingQueue}");
            Console.WriteLine($"Broker OUT: {node.BrokerOutgoingQueue}");
            Console.WriteLine();

            Console.WriteLine($"Publications:");
            foreach (NodePublication item in node.Publications)
            {
                Console.WriteLine($"* “ип сообщени€: {item.MessageType}");
                Console.WriteLine($"* ќчередь cообщений узла: {item.NodeQueue}");
                Console.WriteLine($"* ќчередь cообщений брокера: {item.BrokerQueue}");
            }
            Console.WriteLine();

            Console.WriteLine($"Subscriptions:");
            foreach (NodeSubscription item in node.Subscriptions)
            {
                Console.WriteLine($"* “ип сообщени€: {item.MessageType}");
                Console.WriteLine($"* ќчередь cообщений узла: {item.NodeQueue}");
                Console.WriteLine($"* ќчередь cообщений брокера: {item.BrokerQueue}");
            }
            Console.WriteLine();
        }
        [TestMethod] public void SelectMessagingSettings()
        {
            new PublicationSettings(DatabaseProvider.PostgreSQL, PG_CONNECTION_STRING)
                .SelectMessagingSettings("DaJetMessaging", out MessagingSettings settings);

            Console.WriteLine();
            Console.WriteLine($"{settings.MainNode.Code} : {settings.MainNode.Name} ({(settings.MainNode.IsActive ? "active" : "idle")})");
            Console.WriteLine($"Broker: {settings.MainNode.BrokerServer}");
            Console.WriteLine($"Node IN: {settings.MainNode.NodeIncomingQueue}");
            Console.WriteLine($"Node OUT: {settings.MainNode.NodeOutgoingQueue}");
            Console.WriteLine($"Broker IN: {settings.MainNode.BrokerIncomingQueue}");
            Console.WriteLine($"Broker OUT: {settings.MainNode.BrokerOutgoingQueue}");
            Console.WriteLine();

            Console.WriteLine($"Publications:");
            foreach (NodePublication item in settings.MainNode.Publications)
            {
                Console.WriteLine($"* “ип сообщени€: {item.MessageType}");
                Console.WriteLine($"* ќчередь cообщений узла: {item.NodeQueue}");
                Console.WriteLine($"* ќчередь cообщений брокера: {item.BrokerQueue}");
            }
            Console.WriteLine();

            Console.WriteLine($"Subscriptions:");
            foreach (NodeSubscription item in settings.MainNode.Subscriptions)
            {
                Console.WriteLine($"* “ип сообщени€: {item.MessageType}");
                Console.WriteLine($"* ќчередь cообщений узла: {item.NodeQueue}");
                Console.WriteLine($"* ќчередь cообщений брокера: {item.BrokerQueue}");
            }
            Console.WriteLine();
        }
    }
}