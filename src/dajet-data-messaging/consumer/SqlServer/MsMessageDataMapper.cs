using Microsoft.Data.SqlClient;
using Microsoft.Extensions.Options;
using System;
using System.Data;
using System.Data.Common;

namespace DaJet.Data.Messaging.SqlServer
{
    public sealed class MsMessageDataMapper : IMessageDataMapper
    {
        private readonly DatabaseConsumerOptions _consumerOptions;
        private readonly DatabaseProducerOptions _producerOptions;
        public MsMessageDataMapper(IOptions<DatabaseConsumerOptions> consumerOptions, IOptions<DatabaseProducerOptions> producerOptions)
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

            SqlParameter parameter = new SqlParameter("MessageCount", SqlDbType.Int)
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
                "WITH cte AS (SELECT TOP (@MessageCount) " +
                "{НомерСообщения} AS [MessageNumber], {Заголовки} AS [Headers], " +
                "{ТипСообщения} AS [MessageType], {ТелоСообщения} AS [MessageBody] " +
                "FROM {TABLE_NAME} WITH (ROWLOCK, READPAST) " +
                "ORDER BY {НомерСообщения} ASC) " +
                "DELETE cte OUTPUT " +
                "deleted.[MessageNumber], deleted.[Headers], " +
                "deleted.[MessageType], deleted.[MessageBody];";

            OUTGOING_QUEUE_SELECT_SCRIPT = OUTGOING_QUEUE_SELECT_SCRIPT.Replace("{TABLE_NAME}", _consumerOptions.QueueTableName);

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

            command.Parameters.Add(new SqlParameter("Заголовки", SqlDbType.NVarChar) { Value = message.Headers });
            command.Parameters.Add(new SqlParameter("Отправитель", SqlDbType.NVarChar) { Value = string.Empty });
            command.Parameters.Add(new SqlParameter("ТипСообщения", SqlDbType.NVarChar) { Value = message.MessageType });
            command.Parameters.Add(new SqlParameter("ТелоСообщения", SqlDbType.NVarChar) { Value = message.MessageBody });
            command.Parameters.Add(new SqlParameter("ДатаВремя", SqlDbType.DateTime2) { Value = DateTime.Now });
            command.Parameters.Add(new SqlParameter("ОписаниеОшибки", SqlDbType.NVarChar) { Value = string.Empty });
            command.Parameters.Add(new SqlParameter("КоличествоОшибок", SqlDbType.Int) { Value = 0 });
        }
        private string BuildInsertScript()
        {
            string script =
                "INSERT {TABLE_NAME} " +
                "({НомерСообщения}, {Заголовки}, {Отправитель}, {ТипСообщения}, {ТелоСообщения}, {ДатаВремя}, {ОписаниеОшибки}, {КоличествоОшибок}) " +
                "SELECT NEXT VALUE FOR {SEQUENCE_NAME}, " +
                "@Заголовки, @Отправитель, @ТипСообщения, @ТелоСообщения, @ДатаВремя, @ОписаниеОшибки, @КоличествоОшибок;";

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