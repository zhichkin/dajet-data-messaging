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
                        IsMarkedForDeletion = (bool)reader["ПометкаУдаления"]
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
                NodePublication item = new NodePublication
                {
                    MessageType = (string)reader["ТипСообщения"],
                    NodeQueue = (string)reader["ОчередьСообщенийУзла"],
                    BrokerQueue = (string)reader["ОчередьСообщенийБрокера"]
                };

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
                NodeSubscription item = new NodeSubscription
                {
                    MessageType = (string)reader["ТипСообщения"],
                    NodeQueue = (string)reader["ОчередьСообщенийУзла"],
                    BrokerQueue = (string)reader["ОчередьСообщенийБрокера"]
                };

                list.Add(item);
            }

            return list;
        }

        public void SelectMainNode(in string publicationName, out PublicationNode node)
        {
            if (!new MetadataService()
                .UseDatabaseProvider(_provider)
                .UseConnectionString(_connectionString)
                .TryOpenInfoBase(out InfoBase infoBase, out string message))
            {
                throw new Exception(message);
            }

            // Выполнить поиск плана обмена
            string metadataName = "ПланОбмена." + publicationName;
            ApplicationObject metadata = infoBase.GetApplicationObjectByName(metadataName);
            if (!(metadata is Publication publication))
            {
                throw new Exception($"План обмена \"{publicationName}\" не найден.");
            }

            // Выполнить поиск табличной части "ИсходящиеСообщения"
            TablePart publications = null;
            for (int i = 0; i < publication.TableParts.Count; i++)
            {
                if (publication.TableParts[i].Name == "ИсходящиеСообщения")
                {
                    publications = publication.TableParts[i];
                    break;
                }
            }
            if (publications == null)
            {
                throw new Exception($"Табличная часть \"ИсходящиеСообщения\" плана обмена \"{publicationName}\" не найдена.");
            }

            // Выполнить поиск табличной части "ВходящиеСообщения"
            TablePart subscriptions = null;
            for (int i = 0; i < publication.TableParts.Count; i++)
            {
                if (publication.TableParts[i].Name == "ВходящиеСообщения")
                {
                    subscriptions = publication.TableParts[i];
                    break;
                }
            }
            if (subscriptions == null)
            {
                throw new Exception($"Табличная часть \"ВходящиеСообщения\" плана обмена \"{publicationName}\" не найдена.");
            }

            // Получить данные плана обмена
            Select(in publication);

            if (publication.Publisher == null ||
                publication.Publisher.Uuid == Guid.Empty ||
                string.IsNullOrWhiteSpace(publication.Publisher.Code))
            {
                throw new Exception($"Главный узел плана обмена \"{publicationName}\" не найден или его код не указан.");
            }

            // Получить данные главного узла плана обмена (publisher)
            node = Select(in publication, publication.Publisher.Uuid);
            if (node == null)
            {
                throw new Exception($"Главный узел {{{publication.Publisher.Uuid}}} плана обмена \"{publicationName}\" не найден.");
            }
            node.Publications = SelectNodePublications(in publications, node.Uuid);
            node.Subscriptions = SelectNodeSubscriptions(in subscriptions, node.Uuid);
        }

        public void SelectMessagingSettings(in string publicationName, out MessagingSettings settings)
        {
            settings = new MessagingSettings();

            if (!new MetadataService()
                .UseDatabaseProvider(_provider)
                .UseConnectionString(_connectionString)
                .TryOpenInfoBase(out InfoBase infoBase, out string message))
            {
                throw new Exception(message);
            }

            settings.YearOffset = infoBase.YearOffset;

            // Выполнить поиск плана обмена
            string metadataName = "ПланОбмена." + publicationName;
            ApplicationObject metadata = infoBase.GetApplicationObjectByName(metadataName);
            if (!(metadata is Publication publication))
            {
                throw new Exception($"План обмена \"{publicationName}\" не найден.");
            }

            // Выполнить поиск табличной части "ИсходящиеСообщения"
            TablePart publications = null;
            for (int i = 0; i < publication.TableParts.Count; i++)
            {
                if (publication.TableParts[i].Name == "ИсходящиеСообщения")
                {
                    publications = publication.TableParts[i];
                    break;
                }
            }
            if (publications == null)
            {
                throw new Exception($"Табличная часть \"ИсходящиеСообщения\" плана обмена \"{publicationName}\" не найдена.");
            }

            // Выполнить поиск табличной части "ВходящиеСообщения"
            TablePart subscriptions = null;
            for (int i = 0; i < publication.TableParts.Count; i++)
            {
                if (publication.TableParts[i].Name == "ВходящиеСообщения")
                {
                    subscriptions = publication.TableParts[i];
                    break;
                }
            }
            if (subscriptions == null)
            {
                throw new Exception($"Табличная часть \"ВходящиеСообщения\" плана обмена \"{publicationName}\" не найдена.");
            }

            // Получить данные плана обмена
            Select(in publication);

            if (publication.Publisher == null ||
                publication.Publisher.Uuid == Guid.Empty ||
                string.IsNullOrWhiteSpace(publication.Publisher.Code))
            {
                throw new Exception($"Главный узел плана обмена \"{publicationName}\" не найден или его код не указан.");
            }
            settings.Publication = publication;

            // Получить данные главного узла плана обмена (publisher)
            settings.MainNode = Select(in publication, publication.Publisher.Uuid);
            if (settings.MainNode == null)
            {
                throw new Exception($"Главный узел {{{publication.Publisher.Uuid}}} плана обмена \"{publicationName}\" не найден.");
            }
            settings.MainNode.Publications = SelectNodePublications(in publications, settings.MainNode.Uuid);
            settings.MainNode.Subscriptions = SelectNodeSubscriptions(in subscriptions, settings.MainNode.Uuid);

            // Получить метаданные исходящей очереди сообщений
            settings.OutgoingQueue = GetOutgoingQueueMetadata(in infoBase, settings.MainNode);

            // Получить метаданные входящей очереди сообщений
            settings.IncomingQueue = GetIncomingQueueMetadata(in infoBase, settings.MainNode);

            // Валидировать интерфейс данных
            DbInterfaceValidator validator = new DbInterfaceValidator();
            int version = GetOutgoingInterfaceVersion(in validator, settings.OutgoingQueue);
            ValidateIncomingInterface(in validator, settings.IncomingQueue);

            // Выполнить конфигурирование объектов СУБД при необходимости
            IQueueConfigurator configurator = new DbQueueConfigurator(version, _provider, _connectionString);
            ConfigureOutgoingQueue(in configurator, settings.OutgoingQueue);
            ConfigureIncomingQueue(in configurator, settings.IncomingQueue);
        }
        
        private ApplicationObject GetOutgoingQueueMetadata(in InfoBase infoBase, in PublicationNode node)
        {
            string OUTGOING_QUEUE_NAME = $"РегистрСведений.{node.NodeOutgoingQueue}";

            ApplicationObject queue = infoBase.GetApplicationObjectByName(OUTGOING_QUEUE_NAME);

            if (queue == null)
            {
                throw new Exception($"Исходящая очередь \"{OUTGOING_QUEUE_NAME}\" не найдена.");
            }

            return queue;
        }
        private int GetOutgoingInterfaceVersion(in DbInterfaceValidator validator, in ApplicationObject queue)
        {
            int version = validator.GetOutgoingInterfaceVersion(in queue);

            if (version < 1)
            {
                throw new Exception($"Интерфейс данных исходящей очереди не поддерживается.");
            }

            return version;
        }
        private void ConfigureOutgoingQueue(in IQueueConfigurator configurator, in ApplicationObject queue)
        {
            configurator.ConfigureOutgoingMessageQueue(in queue, out List<string> errors);

            if (errors.Count > 0)
            {
                string message = string.Empty;

                foreach (string error in errors)
                {
                    message += error + Environment.NewLine;
                }

                throw new Exception(message);
            }
        }

        private static ApplicationObject GetIncomingQueueMetadata(in InfoBase infoBase, in PublicationNode node)
        {
            string INCOMING_QUEUE_NAME = $"РегистрСведений.{node.NodeIncomingQueue}";

            ApplicationObject queue = infoBase.GetApplicationObjectByName(INCOMING_QUEUE_NAME);

            if (queue == null)
            {
                throw new Exception($"Входящая очередь \"{INCOMING_QUEUE_NAME}\" не найдена.");
            }

            return queue;
        }
        private static void ValidateIncomingInterface(in DbInterfaceValidator validator, in ApplicationObject queue)
        {
            int version = validator.GetIncomingInterfaceVersion(in queue);

            if (version < 1)
            {
                throw new Exception($"Интерфейс данных входящей очереди не поддерживается.");
            }
        }
        private static void ConfigureIncomingQueue(in IQueueConfigurator configurator, in ApplicationObject queue)
        {
            configurator.ConfigureIncomingMessageQueue(in queue, out List<string> errors);

            if (errors.Count > 0)
            {
                string message = string.Empty;

                foreach (string error in errors)
                {
                    message += error + Environment.NewLine;
                }

                throw new Exception(message);
            }
        }
    }
}