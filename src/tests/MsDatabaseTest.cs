using DaJet.Metadata;
using DaJet.Metadata.Model;
using Microsoft.VisualStudio.TestTools.UnitTesting;
using System;
using System.Collections.Generic;
using System.Linq;

namespace DaJet.Data.Messaging.Test
{
    [TestClass] public class MsDatabaseTest
    {
        private readonly InfoBase _infoBase;
        private readonly ApplicationObject _incomingQueue;
        private readonly ApplicationObject _outgoingQueue;
        private const string MS_CONNECTION_STRING = "Data Source=zhichkin;Initial Catalog=dajet-messaging-ms;Integrated Security=True";

        private readonly DbInterfaceValidator _validator = new DbInterfaceValidator();
        private readonly QueryBuilder _builder = new QueryBuilder(DatabaseProvider.SQLServer);
        private readonly MsQueueConfigurator _configurator = new MsQueueConfigurator(MS_CONNECTION_STRING);

        public MsDatabaseTest()
        {
            if (!new MetadataService()
                .UseDatabaseProvider(DatabaseProvider.SQLServer)
                .UseConnectionString(MS_CONNECTION_STRING)
                .TryOpenInfoBase(out InfoBase infoBase, out string error))
            {
                Console.WriteLine(error);
                return;
            }
            _infoBase = infoBase;
            _incomingQueue = _infoBase.GetApplicationObjectByName("–егистр—ведений.¬ход€ща€ќчередь");
            _outgoingQueue = _infoBase.GetApplicationObjectByName("–егистр—ведений.»сход€ща€ќчередь");
        }

        [TestMethod] public void Validate_DbInterface()
        {
            int version = -1;

            version = _validator.GetIncomingInterfaceVersion(in _incomingQueue);
            Console.WriteLine($"Incoming queue version = {version}");

            version = _validator.GetOutgoingInterfaceVersion(in _outgoingQueue);
            Console.WriteLine($"Outgoing queue version = {version}");
        }
        [TestMethod] public void Script_IncomingInsert()
        {
            Console.WriteLine($"{_builder.BuildIncomingQueueInsertScript(in _incomingQueue)}");
        }
        [TestMethod] public void Script_OutgoingSelect()
        {
            Console.WriteLine($"{_builder.BuildOutgoingQueueSelectScript(in _outgoingQueue)}");
        }

        [TestMethod] public void Configure_IncomingQueue()
        {
            _configurator.ConfigureIncomingMessageQueue(in _incomingQueue, out List<string> errors);

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
        [TestMethod] public void Configure_OutgoingQueue()
        {
            _configurator.ConfigureOutgoingMessageQueue(in _outgoingQueue, out List<string> errors);

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
        
        private IEnumerable<IncomingMessage> GetTestIncomingMessages()
        {
            for (int i = 0; i < 10; i++)
            {
                yield return new IncomingMessage()
                {
                    Sender = "DaJet",
                    Headers = string.Empty,
                    MessageType = "test",
                    MessageBody = $"{{ \"message\": {(i + 1)} }}",
                    DateTimeStamp = DateTime.Now
                };
            }
        }
        [TestMethod] public void MessageProducer_Insert()
        {
            int total = 0;

            using (IMessageProducer producer = new MsMessageProducer(MS_CONNECTION_STRING, in _incomingQueue, _infoBase.YearOffset))
            {
                foreach (IncomingMessage message in GetTestIncomingMessages())
                {
                    producer.Insert(in message); total++;
                }

                producer.TxBegin();
                foreach (IncomingMessage message in GetTestIncomingMessages())
                {
                    producer.Insert(in message); total++;
                }
                producer.TxCommit();
            }

            Console.WriteLine($"Total = {total}");
        }
        [TestMethod] public void MessageConsumer_Select()
        {
            int total = 0;

            using (IMessageConsumer consumer = new MsMessageConsumer(MS_CONNECTION_STRING, in _outgoingQueue, _infoBase.YearOffset))
            {
                do
                {
                    foreach (OutgoingMessage message in consumer.Select())
                    {
                        total++;
                    }

                    consumer.TxBegin();
                    foreach (OutgoingMessage message in consumer.Select())
                    {
                        total++;
                    }
                    consumer.TxCommit();

                    Console.WriteLine($"Count = {consumer.RecordsAffected}");
                }
                while (consumer.RecordsAffected > 0);
            }
            Console.WriteLine($"Total = {total}");
        }

        [TestMethod] public void Settings_Publication()
        {
            ApplicationObject metadata = _infoBase.GetApplicationObjectByName("ѕланќбмена.DaJetMessaging");

            Console.WriteLine($"ѕлан обмена: {metadata.Name}");

            Publication publication = metadata as Publication;
            TablePart publications = publication.TableParts.Where(t => t.Name == "»сход€щие—ообщени€").FirstOrDefault();
            TablePart subscriptions = publication.TableParts.Where(t => t.Name == "¬ход€щие—ообщени€").FirstOrDefault();

            PublicationSettings settings = new PublicationSettings(DatabaseProvider.SQLServer, MS_CONNECTION_STRING);
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
                Console.WriteLine($"* ¬ерсионирование: {item.UseVersioning}");
            }
            Console.WriteLine();

            Console.WriteLine($"Subscriptions:");
            node.Subscriptions = settings.SelectNodeSubscriptions(subscriptions, node.Uuid);
            foreach (NodeSubscription item in node.Subscriptions)
            {
                Console.WriteLine($"* “ип сообщени€: {item.MessageType}");
                Console.WriteLine($"* ќчередь cообщений узла: {item.NodeQueue}");
                Console.WriteLine($"* ќчередь cообщений брокера: {item.BrokerQueue}");
                Console.WriteLine($"* ¬ерсионирование: {item.UseVersioning}");
            }
            Console.WriteLine();
        }
    }
}