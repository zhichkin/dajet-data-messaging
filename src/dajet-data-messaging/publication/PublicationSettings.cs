using DaJet.Metadata;
using DaJet.Metadata.Model;
using System;
using System.Collections.Generic;
using System.Data;

namespace DaJet.Data.Messaging
{
    public sealed class PublicationSettings
    {
        private readonly string _connectionString;
        private readonly DatabaseProvider _provider;
        public PublicationSettings(DatabaseProvider provider, in string connectionString)
        {
            _provider = provider;
            _connectionString = connectionString;
        }
        public void Select(in Publication publication)
        {
            publication.Publisher = null;
            publication.Subscribers.Clear();

            string PUBLICATION_SELECT_SCRIPT = new QueryBuilder(_provider)
                .BuildPublicationSelectScript(in publication);

            QueryExecutor executor = new QueryExecutor(_provider, in _connectionString);

            foreach(IDataReader reader in executor.ExecuteReader(PUBLICATION_SELECT_SCRIPT, 10))
            {
                Guid predefinedid = new Guid((byte[])reader["ЭтотУзел"]);

                if (predefinedid == Guid.Empty)
                {
                    Subscriber subscriber = new Subscriber
                    {
                        Uuid = new Guid((byte[])reader["Ссылка"]),
                        Code = (string)reader["Код"],
                        Name = (string)reader["Наименование"],
                        IsMarkedForDeletion = !(bool)reader["ПометкаУдаления"]
                    };
                    publication.Subscribers.Add(subscriber);
                }
                else
                {
                    Publisher publisher = new Publisher
                    {
                        Uuid = new Guid((byte[])reader["Ссылка"]),
                        Code = (string)reader["Код"],
                        Name = (string)reader["Наименование"],
                    };
                    publication.Publisher = publisher;
                }
            }
        }
        public PublicationNode Select(in Publication publication, in Guid uuid)
        {
            PublicationNode node = new PublicationNode();

            string PUBLICATION_NODE_SELECT_SCRIPT = new QueryBuilder(_provider)
                .BuildPublicationNodeSelectScript(in publication);

            QueryExecutor executor = new QueryExecutor(_provider, in _connectionString);

            Dictionary<string, object> parameters = new Dictionary<string, object>()
            {
                { "uuid", uuid.ToByteArray() }
            };

            foreach (IDataReader reader in executor.ExecuteReader(PUBLICATION_NODE_SELECT_SCRIPT, 10, parameters))
            {
                node.Uuid = new Guid((byte[])reader["Ссылка"]);
                node.Code = (string)reader["Код"];
                node.Name = (string)reader["Наименование"];
                node.IsActive = !(bool)reader["ПометкаУдаления"];
                node.BrokerServer = (string)reader["СерверБрокера"];
                node.NodeIncomingQueue = (string)reader["ВходящаяОчередьУзла"];
                node.NodeOutgoingQueue = (string)reader["ИсходящаяОчередьУзла"];
                node.BrokerIncomingQueue = (string)reader["ВходящаяОчередьБрокера"];
                node.BrokerOutgoingQueue = (string)reader["ИсходящаяОчередьБрокера"];
            }

            return node;
        }
        public List<NodePublication> SelectNodePublications(in TablePart publications, in Guid uuid)
        {
            List<NodePublication> list = new List<NodePublication>();

            string NODE_PUBLICATIONS_SELECT_SCRIPT = new QueryBuilder(_provider)
                .BuildPublicationNodePublicationsSelectScript(in publications);

            QueryExecutor executor = new QueryExecutor(_provider, in _connectionString);

            Dictionary<string, object> parameters = new Dictionary<string, object>()
            {
                { "uuid", uuid.ToByteArray() }
            };

            foreach (IDataReader reader in executor.ExecuteReader(NODE_PUBLICATIONS_SELECT_SCRIPT, 10, parameters))
            {
                NodePublication item = new NodePublication();

                item.MessageType = (string)reader["ТипСообщения"];
                item.NodeQueue = (string)reader["ОчередьСообщенийУзла"];
                item.BrokerQueue = (string)reader["ОчередьСообщенийБрокера"];
                item.UseVersioning = (bool)reader["Версионирование"];

                list.Add(item);
            }

            return list;
        }
        public List<NodeSubscription> SelectNodeSubscriptions(in TablePart subscriptions, in Guid uuid)
        {
            List<NodeSubscription> list = new List<NodeSubscription>();

            string NODE_SUBSCRIPTIONS_SELECT_SCRIPT = new QueryBuilder(_provider)
                .BuildPublicationNodeSubscriptionsSelectScript(in subscriptions);

            QueryExecutor executor = new QueryExecutor(_provider, in _connectionString);

            Dictionary<string, object> parameters = new Dictionary<string, object>()
            {
                { "uuid", uuid.ToByteArray() }
            };

            foreach (IDataReader reader in executor.ExecuteReader(NODE_SUBSCRIPTIONS_SELECT_SCRIPT, 10, parameters))
            {
                NodeSubscription item = new NodeSubscription();

                item.MessageType = (string)reader["ТипСообщения"];
                item.NodeQueue = (string)reader["ОчередьСообщенийУзла"];
                item.BrokerQueue = (string)reader["ОчередьСообщенийБрокера"];
                item.UseVersioning = (bool)reader["Версионирование"];

                list.Add(item);
            }

            return list;
        }
    }
}