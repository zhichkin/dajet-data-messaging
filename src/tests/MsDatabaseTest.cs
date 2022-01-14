using DaJet.Metadata;
using DaJet.Metadata.Model;
using Microsoft.VisualStudio.TestTools.UnitTesting;
using System;
using System.Collections.Generic;

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
            int total = 0;

            using (MsMessageProducer producer = new MsMessageProducer(MS_CONNECTION_STRING, in _incomingQueue, _infoBase.YearOffset))
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

            using (MsMessageConsumer consumer = new MsMessageConsumer(MS_CONNECTION_STRING, in _outgoingQueue, _infoBase.YearOffset))
            {
                do
                {
                    foreach (OutgoingMessage message in consumer.Select())
                    {
                        total++;

                        // Send message to recipient
                        // Check if message accepted
                        // If not break foreach loop
                    }
                    Console.WriteLine($"Count = {consumer.RecordsAffected}");
                }
                while (consumer.RecordsAffected > 0);
            }
            Console.WriteLine($"Total = {total}");
        }
    }
}