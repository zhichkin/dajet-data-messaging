using DaJet.Metadata;
using DaJet.Metadata.Model;
using Microsoft.Data.SqlClient;
using System;
using System.Collections.Generic;
using System.Data;

namespace DaJet.Data.Messaging
{
    public sealed class MsMessageConsumer : IDisposable
    {
        private SqlCommand _command;
        private SqlDataReader _reader;
        private SqlConnection _connection;
        private SqlTransaction _transaction;
        private readonly int _YearOffset;
        private string OUTGOING_QUEUE_SELECT_SCRIPT;
        public MsMessageConsumer(in string connectionString, in ApplicationObject queue, int yearOffset = 0)
        {
            _YearOffset = yearOffset;
            ConnectionString = connectionString;
            Initialize(in queue);
        }
        public string ConnectionString { get; }
        private void Initialize(in ApplicationObject queue)
        {
            OUTGOING_QUEUE_SELECT_SCRIPT =
                new QueryBuilder(DatabaseProvider.SQLServer)
                .BuildOutgoingQueueSelectScript(in queue);

            try
            {
                _connection = new SqlConnection(ConnectionString);
                _connection.Open();

                _command = _connection.CreateCommand();
                _command.CommandType = CommandType.Text;
                _command.CommandText = OUTGOING_QUEUE_SELECT_SCRIPT;
                _command.CommandTimeout = 60; // seconds
            }
            catch
            {
                Dispose();
                throw;
            }
        }
        private IEnumerable<SqlDataReader> SelectDataRows()
        {
            using (_transaction = _connection.BeginTransaction())
            {
                _command.Transaction = _transaction;

                using (_reader = _command.ExecuteReader())
                {
                    while (_reader.Read())
                    {
                        yield return _reader;
                    }
                }

                _transaction.Commit();
            }
        }
        public IEnumerable<OutgoingMessage> Select()
        {
            OutgoingMessage message = new OutgoingMessage();

            foreach (SqlDataReader reader in SelectDataRows())
            {
                message.MessageNumber = reader.IsDBNull("НомерСообщения") ? 0 : (long)reader.GetDecimal("НомерСообщения");
                message.Uuid = reader.IsDBNull("Идентификатор") ? Guid.Empty : new Guid((byte[])reader["Идентификатор"]);
                message.Headers = reader.IsDBNull("Заголовки") ? string.Empty : reader.GetString("Заголовки");
                message.Sender = reader.IsDBNull("Отправитель") ? string.Empty : reader.GetString("Отправитель");
                message.Recipients = reader.IsDBNull("Получатели") ? string.Empty : reader.GetString("Получатели");
                message.OperationType = reader.IsDBNull("ТипОперации") ? string.Empty : reader.GetString("ТипОперации");
                message.MessageType = reader.IsDBNull("ТипСообщения") ? string.Empty : reader.GetString("ТипСообщения");
                message.MessageBody = reader.IsDBNull("ТелоСообщения") ? string.Empty : reader.GetString("ТелоСообщения");
                message.DateTimeStamp = reader.IsDBNull("ДатаВремя") ? DateTime.MinValue : reader.GetDateTime("ДатаВремя").AddYears(-_YearOffset);

                yield return message;
            }
        }
        public void Dispose()
        {
            if (_connection == null)
            {
                throw new ObjectDisposedException(nameof(MsMessageConsumer));
            }

            _reader?.Dispose();
            _reader = null;

            _command?.Dispose();
            _command = null;

            _transaction?.Dispose();
            _transaction = null;

            _connection?.Dispose();
            _connection = null;
        }
    }
}