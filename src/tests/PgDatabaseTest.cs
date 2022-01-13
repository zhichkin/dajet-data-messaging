using DaJet.Metadata;
using DaJet.Metadata.Model;
using Microsoft.VisualStudio.TestTools.UnitTesting;
using System;

namespace DaJet.Data.Messaging.Test
{
    [TestClass] public class PgDatabaseTest
    {
        private const string PG_CONNECTION_STRING = "Host=127.0.0.1;Port=5432;Database=dajet-kafka-pg;Username=postgres;Password=postgres;";

        [TestMethod] public void QueueSetup()
        {


        }
        [TestMethod] public void DbInterfaceValidation()
        {
            if (!new MetadataService()
                .UseDatabaseProvider(DatabaseProvider.PostgreSQL)
                .UseConnectionString(PG_CONNECTION_STRING)
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
    }
}