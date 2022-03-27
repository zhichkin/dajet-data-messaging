using Microsoft.Data.SqlClient;
using Microsoft.Extensions.Options;
using System.Data;
using System.Threading;

namespace DaJet.Data.Messaging.SqlServer
{
    public sealed class MsMessageConsumer : IDbMessageConsumer
    {
        private readonly DatabaseConsumerOptions _options;
        public MsMessageConsumer(IOptions<DatabaseConsumerOptions> options)
        {
            _options = options.Value;
        }
        private string BuildSelectScript()
        {
            string OUTGOING_QUEUE_SELECT_SCRIPT =
                "WITH cte AS (SELECT TOP (@MessageCount) " +
                "{НомерСообщения} AS [MessageNumber], {Заголовки} AS [Headers], " +
                "{ТипСообщения} AS [MessageType], {ТелоСообщения} AS [MessageBody] " +
                "FROM {TABLE_NAME} WITH (ROWLOCK, READPAST) " +
                "ORDER BY {НомерСообщения} ASC) " +
                "DELETE cte OUTPUT " +
                "deleted.[MessageNumber], deleted.[Headers], " +
                "deleted.[MessageType], deleted.[MessageBody];";

            OUTGOING_QUEUE_SELECT_SCRIPT = OUTGOING_QUEUE_SELECT_SCRIPT.Replace("{TABLE_NAME}", _options.QueueTableName);

            foreach (var column in _options.TableColumns)
            {
                if (column.Key == "НомерСообщения")
                {
                    OUTGOING_QUEUE_SELECT_SCRIPT = OUTGOING_QUEUE_SELECT_SCRIPT.Replace("{НомерСообщения}", column.Value);
                }
                else if (column.Key == "Заголовки")
                {
                    OUTGOING_QUEUE_SELECT_SCRIPT = OUTGOING_QUEUE_SELECT_SCRIPT.Replace("{Заголовки}", column.Value);
                }
                else if (column.Key == "ТипСообщения")
                {
                    OUTGOING_QUEUE_SELECT_SCRIPT = OUTGOING_QUEUE_SELECT_SCRIPT.Replace("{ТипСообщения}", column.Value);
                }
                else if (column.Key == "ТелоСообщения")
                {
                    OUTGOING_QUEUE_SELECT_SCRIPT = OUTGOING_QUEUE_SELECT_SCRIPT.Replace("{ТелоСообщения}", column.Value);
                }
            }

            return OUTGOING_QUEUE_SELECT_SCRIPT;
        }
        private void MapDataToMessage(in SqlDataReader reader, in DatabaseMessage message)
        {
            message.MessageNumber = reader.IsDBNull("MessageNumber") ? 0L : (long)reader.GetDecimal("MessageNumber");
            message.Headers = reader.IsDBNull("Headers") ? string.Empty : reader.GetString("Headers");
            message.MessageType = reader.IsDBNull("MessageType") ? string.Empty : reader.GetString("MessageType");
            message.MessageBody = reader.IsDBNull("MessageBody") ? string.Empty : reader.GetString("MessageBody");
        }
        private void ConfigureCommand(in SqlCommand command)
        {
            command.CommandType = CommandType.Text;
            command.CommandTimeout = 60; // seconds
            command.CommandText = BuildSelectScript();

            command.Parameters
                .Add("MessageCount", SqlDbType.Int)
                .Value = _options.MessagesPerTransaction;
        }
        public void Consume(in IDbMessageHandler handler, CancellationToken token)
        {
            int consumed;

            DatabaseMessage message = new DatabaseMessage();

            using (SqlConnection connection = new SqlConnection(_options.ConnectionString))
            {
                connection.Open();

                using (SqlCommand command = connection.CreateCommand())
                {
                    ConfigureCommand(in command);

                    do
                    {
                        consumed = 0;

                        using (SqlTransaction transaction = connection.BeginTransaction())
                        {
                            command.Transaction = transaction;

                            using (SqlDataReader reader = command.ExecuteReader())
                            {
                                while (reader.Read())
                                {
                                    consumed++;

                                    MapDataToMessage(in reader, in message);

                                    handler.Handle(in message);
                                }
                                reader.Close();
                            }

                            if (consumed > 0)
                            {
                                if (handler.Confirm())
                                {
                                    transaction.Commit();
                                }
                                else
                                {
                                    consumed = 0;
                                }
                            }
                        }
                    }
                    while (consumed > 0 && !token.IsCancellationRequested);
                }
            }
        }
    }
}