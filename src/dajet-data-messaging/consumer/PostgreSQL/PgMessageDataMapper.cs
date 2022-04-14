using Microsoft.Extensions.Options;
using Npgsql;
using NpgsqlTypes;
using System;
using System.Data;
using System.Data.Common;

namespace DaJet.Data.Messaging.PostgreSQL
{
    public sealed class PgMessageDataMapper : IMessageDataMapper
    {
        private readonly DatabaseConsumerOptions _consumerOptions;
        private readonly DatabaseProducerOptions _producerOptions;
        public PgMessageDataMapper(IOptions<DatabaseConsumerOptions> consumerOptions, IOptions<DatabaseProducerOptions> producerOptions)
        {
            _consumerOptions = consumerOptions?.Value;
            _producerOptions = producerOptions?.Value;
        }
        
        public void ConfigureSelectCommand(in DbCommand command)
        {
            command.CommandType = CommandType.Text;
            command.CommandTimeout = 60; // seconds
            command.CommandText = BuildSelectScript();

            command.Parameters.Clear();

            NpgsqlParameter parameter = new NpgsqlParameter("MessageCount", NpgsqlDbType.Integer)
            {
                Value = _consumerOptions.MessagesPerTransaction
            };

            command.Parameters.Add(parameter);
        }
        public void MapDataToMessage(in DbDataReader reader, in DatabaseMessage message)
        {
            message.MessageNumber = reader.IsDBNull("MessageNumber") ? 0L : (long)reader.GetDecimal("MessageNumber");
            message.Headers = reader.IsDBNull("Headers") ? string.Empty : reader.GetString("Headers");
            message.MessageType = reader.IsDBNull("MessageType") ? string.Empty : reader.GetString("MessageType");
            message.MessageBody = reader.IsDBNull("MessageBody") ? string.Empty : reader.GetString("MessageBody");
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

            OUTGOING_QUEUE_SELECT_SCRIPT = OUTGOING_QUEUE_SELECT_SCRIPT.Replace("{TABLE_NAME}", _consumerOptions.QueueTable);

            foreach (var column in _consumerOptions.TableColumns)
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

        public void ConfigureInsertCommand(in DbCommand command, in DatabaseMessage message)
        {
            command.CommandType = CommandType.Text;
            command.CommandTimeout = 10; // seconds
            command.CommandText = BuildInsertScript();

            command.Parameters.Clear();

            command.Parameters.Add(new NpgsqlParameter("Заголовки", NpgsqlDbType.Varchar) { Value = message.Headers });
            command.Parameters.Add(new NpgsqlParameter("Отправитель", NpgsqlDbType.Varchar) { Value = string.Empty });
            command.Parameters.Add(new NpgsqlParameter("ТипСообщения", NpgsqlDbType.Varchar) { Value = message.MessageType });
            command.Parameters.Add(new NpgsqlParameter("ТелоСообщения", NpgsqlDbType.Varchar) { Value = message.MessageBody });
            command.Parameters.Add(new NpgsqlParameter("ДатаВремя", NpgsqlDbType.Timestamp)
            {
                Value = DateTime.Now.AddYears(_producerOptions.YearOffset)
            });
            command.Parameters.Add(new NpgsqlParameter("ОписаниеОшибки", NpgsqlDbType.Varchar) { Value = string.Empty });
            command.Parameters.Add(new NpgsqlParameter("КоличествоОшибок", NpgsqlDbType.Integer) { Value = 0 });
        }
        private string BuildInsertScript()
        {
            string script =
                "INSERT INTO {TABLE_NAME} " +
                "({НомерСообщения}, {Заголовки}, {Отправитель}, {ТипСообщения}, " +
                "{ТелоСообщения}, {ДатаВремя}, {ОписаниеОшибки}, {КоличествоОшибок}) " +
                "SELECT CAST(nextval('{SEQUENCE_NAME}') AS numeric(19,0)), " +
                "CAST(@Заголовки AS mvarchar), CAST(@Отправитель AS mvarchar), CAST(@ТипСообщения AS mvarchar), " +
                "CAST(@ТелоСообщения AS mvarchar), @ДатаВремя, CAST(@ОписаниеОшибки AS mvarchar), @КоличествоОшибок;";

            script = script.Replace("{TABLE_NAME}", _producerOptions.QueueTableName);
            script = script.Replace("{SEQUENCE_NAME}", _producerOptions.SequenceObject);

            foreach (var column in _producerOptions.TableColumns)
            {
                script = script.Replace($"{{{column.Key}}}", column.Value);
            }

            return script;
        }
    }
}