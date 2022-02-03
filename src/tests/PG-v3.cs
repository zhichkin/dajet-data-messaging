using DaJet.Metadata;
using DaJet.Metadata.Model;
using Microsoft.VisualStudio.TestTools.UnitTesting;
using System;
using System.Collections.Generic;
using System.Reflection;

namespace DaJet.Data.Messaging.Test
{
    [TestClass] public class PG_v3
    {
        private readonly InfoBase _infoBase;
        private readonly ApplicationObject _incomingQueue;
        private readonly ApplicationObject _outgoingQueue;
        private const string INCOMING_QUEUE_NAME = "РегистрСведений.ВходящаяОчередь2";
        private const string OUTGOING_QUEUE_NAME = "РегистрСведений.ИсходящаяОчередь2";
        private const string PG_CONNECTION_STRING = "Host=127.0.0.1;Port=5432;Database=dajet-messaging-pg;Username=postgres;Password=postgres;";
        public PG_v3()
        {
            if (!new MetadataService()
                .UseDatabaseProvider(DatabaseProvider.PostgreSQL)
                .UseConnectionString(PG_CONNECTION_STRING)
                .TryOpenInfoBase(out InfoBase infoBase, out string error))
            {
                Console.WriteLine(error);
                return;
            }
            _infoBase = infoBase;
            _incomingQueue = _infoBase.GetApplicationObjectByName(INCOMING_QUEUE_NAME);
            _outgoingQueue = _infoBase.GetApplicationObjectByName(OUTGOING_QUEUE_NAME);
        }

        [TestMethod] public void Validate_DbInterface()
        {
            DbInterfaceValidator validator = new DbInterfaceValidator();

            int version = validator.GetIncomingInterfaceVersion(in _incomingQueue);
            Assert.AreEqual(2, version);
            Console.WriteLine($"Incoming queue version = {version}");

            version = validator.GetOutgoingInterfaceVersion(in _outgoingQueue);
            Assert.AreEqual(3, version);
            Console.WriteLine($"Outgoing queue version = {version}");
        }
        [TestMethod] public void MessageProducer_Insert()
        {
            int total = 0;

            V2.IncomingMessage message = IncomingMessageDataMapper.Create(2) as V2.IncomingMessage;
            Assert.IsNotNull(message);

            message.Uuid = Guid.NewGuid();
            message.Sender = "DaJet";
            message.OperationType = "INSERT";
            message.MessageType = "test";
            message.MessageBody = "{ \"message\": \"test\" }";
            message.DateTimeStamp = DateTime.Now.AddYears(_infoBase.YearOffset);

            using (IMessageProducer producer = new PgMessageProducer(PG_CONNECTION_STRING, in _incomingQueue))
            {
                producer.Insert(message); total++;

                //producer.TxBegin();
                //producer.Insert(in message); total++;
                //producer.TxCommit();
            }
            Assert.AreEqual(1, total);

            Console.WriteLine($"Total = {total}");
        }
        [TestMethod] public void MessageConsumer_Select()
        {
            int total = 0;

            using (IMessageConsumer consumer = new PgMessageConsumer(PG_CONNECTION_STRING, in _outgoingQueue))
            {
                do
                {
                    foreach (OutgoingMessageDataMapper message in consumer.Select())
                    {
                        ShowMessageData(in message);
                    }

                    //consumer.TxBegin();
                    //foreach (OutgoingMessageDataMapper message in consumer.Select())
                    //{
                    //    total++;
                    //}
                    //consumer.TxCommit();

                    total += consumer.RecordsAffected;
                }
                while (consumer.RecordsAffected > 0);
            }

            Console.WriteLine($"Total = {total}");
        }
        private void ShowMessageData(in OutgoingMessageDataMapper message)
        {
            Type type = message.GetType();

            foreach (PropertyInfo property in type.GetProperties())
            {
                Console.WriteLine($"{property.Name} = {property.GetValue(message)}");
            }
        }

        [TestMethod] public void Configure_IncomingQueue()
        {
            DbQueueConfigurator configurator = new DbQueueConfigurator(2, DatabaseProvider.PostgreSQL, PG_CONNECTION_STRING);

            configurator.ConfigureIncomingMessageQueue(in _incomingQueue, out List<string> errors);

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
            DbQueueConfigurator configurator = new DbQueueConfigurator(3, DatabaseProvider.PostgreSQL, PG_CONNECTION_STRING);

            configurator.ConfigureOutgoingMessageQueue(in _outgoingQueue, out List<string> errors);

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

        [TestMethod] public void Script_OutgoingSelect()
        {
            DbInterfaceValidator validator = new DbInterfaceValidator();
            int version = validator.GetOutgoingInterfaceVersion(in _outgoingQueue);

            QueryBuilder builder = new QueryBuilder(DatabaseProvider.PostgreSQL);

            OutgoingMessageDataMapper mapper = OutgoingMessageDataMapper.Create(version);

            string script = builder.BuildOutgoingQueueSelectScript(in _outgoingQueue, in mapper);

            Console.WriteLine(script);
        }
    }
}