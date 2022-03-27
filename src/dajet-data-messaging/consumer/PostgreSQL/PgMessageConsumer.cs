using Microsoft.Extensions.Options;
using Npgsql;
using NpgsqlTypes;
using System.Data;
using System.Threading;

namespace DaJet.Data.Messaging.PostgreSQL
{
    public sealed class PgMessageConsumer : IDbMessageConsumer
    {
        private readonly DatabaseConsumerOptions _options;
        public PgMessageConsumer(IOptions<DatabaseConsumerOptions> options)
        {
            _options = options.Value;
        }
        private string BuildSelectScript()
        {
            string OUTGOING_QUEUE_SELECT_SCRIPT =
                "WITH cte AS (SELECT {НомерСообщения} FROM {TABLE_NAME} " +
                "ORDER BY {НомерСообщения} ASC LIMIT @MessageCount) " +
                "DELETE FROM {TABLE_NAME} t USING cte " +
                "WHERE t.{НомерСообщения} = cte.{НомерСообщения} " +
                "RETURNING t.{НомерСообщения} AS \"MessageNumber\", CAST(t.{Заголовки} AS text) AS \"Headers\", " +
                "CAST(t.{ТипСообщения} AS varchar) AS \"MessageType\", CAST(t.{ТелоСообщения} AS text) AS \"MessageBody\";";

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
        private void MapDataToMessage(in NpgsqlDataReader reader, in DatabaseMessage message)
        {
            message.MessageNumber = reader.IsDBNull("MessageNumber") ? 0L : (long)reader.GetDecimal("MessageNumber");
            message.Headers = reader.IsDBNull("Headers") ? string.Empty : reader.GetString("Headers");
            message.MessageType = reader.IsDBNull("MessageType") ? string.Empty : reader.GetString("MessageType");
            message.MessageBody = reader.IsDBNull("MessageBody") ? string.Empty : reader.GetString("MessageBody");
        }
        private void ConfigureCommand(in NpgsqlCommand command)
        {
            command.CommandType = CommandType.Text;
            command.CommandTimeout = 60; // seconds
            command.CommandText = BuildSelectScript();

            command.Parameters
                .Add("MessageCount", NpgsqlDbType.Integer)
                .Value = _options.MessagesPerTransaction;
        }
        public void Consume(in IDbMessageHandler handler, CancellationToken token)
        {
            int consumed;

            DatabaseMessage message = new DatabaseMessage();

            using (NpgsqlConnection connection = new NpgsqlConnection(_options.ConnectionString))
            {
                connection.Open();

                using (NpgsqlCommand command = connection.CreateCommand())
                {
                    ConfigureCommand(in command);

                    do
                    {
                        consumed = 0;

                        using (NpgsqlTransaction transaction = connection.BeginTransaction())
                        {
                            command.Transaction = transaction;

                            using (NpgsqlDataReader reader = command.ExecuteReader())
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