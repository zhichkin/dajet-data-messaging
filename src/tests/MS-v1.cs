using DaJet.Metadata;
using DaJet.Metadata.Model;
using Microsoft.VisualStudio.TestTools.UnitTesting;
using System;
using System.Collections.Generic;
using System.Reflection;

namespace DaJet.Data.Messaging.Test
{
    [TestClass] public class MS_v1
    {
        private readonly InfoBase _infoBase;
        private readonly ApplicationObject _incomingQueue;
        private readonly ApplicationObject _outgoingQueue;
        private const string INCOMING_QUEUE_NAME = "РегистрСведений.ВходящаяОчередь";
        private const string OUTGOING_QUEUE_NAME = "РегистрСведений.ИсходящаяОчередь";
        private const string MS_CONNECTION_STRING = "Data Source=zhichkin;Initial Catalog=dajet-messaging-ms;Integrated Security=True";
        public MS_v1()
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
            _incomingQueue = _infoBase.GetApplicationObjectByName(INCOMING_QUEUE_NAME);
            _outgoingQueue = _infoBase.GetApplicationObjectByName(OUTGOING_QUEUE_NAME);
        }

        [TestMethod] public void Validate_DbInterface()
        {
            DbInterfaceValidator validator = new DbInterfaceValidator();

            int version = validator.GetIncomingInterfaceVersion(in _incomingQueue);
            Assert.AreEqual(1, version);
            Console.WriteLine($"Incoming queue version = {version}");

            version = validator.GetOutgoingInterfaceVersion(in _outgoingQueue);
            Assert.AreEqual(1, version);
            Console.WriteLine($"Outgoing queue version = {version}");
        }
        [TestMethod] public void Configure_DbInterface()
        {
            DbInterfaceValidator validator = new DbInterfaceValidator();
            
            int version = validator.GetIncomingInterfaceVersion(in _incomingQueue);
            Console.WriteLine($"Incoming queue version = {version}");

            DbQueueConfigurator configurator = new DbQueueConfigurator(version, DatabaseProvider.SQLServer, MS_CONNECTION_STRING);
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

            Console.WriteLine();

            version = validator.GetOutgoingInterfaceVersion(in _outgoingQueue);
            Console.WriteLine($"Outgoing queue version = {version}");

            configurator = new DbQueueConfigurator(version, DatabaseProvider.SQLServer, MS_CONNECTION_STRING);
            configurator.ConfigureOutgoingMessageQueue(in _outgoingQueue, out errors);

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
        [TestMethod] public void MessageProducer_Insert()
        {
            int total = 0;

            V1.IncomingMessage message = IncomingMessageDataMapper.Create(1) as V1.IncomingMessage;
            Assert.IsNotNull(message);

            message.Sender = "DaJet";
            message.Headers = "{ \"Header\": \"test\" }";
            message.MessageType = "test";
            message.MessageBody = "{ \"message\": \"test\" }";
            message.DateTimeStamp = DateTime.Now.AddYears(_infoBase.YearOffset);

            using (IMessageProducer producer = new MsMessageProducer(MS_CONNECTION_STRING, in _incomingQueue))
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

            using (IMessageConsumer consumer = new MsMessageConsumer(MS_CONNECTION_STRING, in _outgoingQueue))
            {
                do
                {
                    foreach (OutgoingMessageDataMapper message in consumer.Select())
                    {
                        Console.WriteLine();
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

            Console.WriteLine();
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
    }
}