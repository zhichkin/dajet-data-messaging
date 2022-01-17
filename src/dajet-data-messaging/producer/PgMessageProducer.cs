using DaJet.Metadata;
using DaJet.Metadata.Model;
using Npgsql;
using NpgsqlTypes;
using System;
using System.Data;

namespace DaJet.Data.Messaging
{
    public sealed class PgMessageProducer : IMessageProducer
    {
        private NpgsqlCommand _command;
        private NpgsqlConnection _connection;
        private NpgsqlTransaction _transaction;
        private readonly int _YearOffset;
        private readonly string _connectionString;
        private string INCOMING_QUEUE_INSERT_SCRIPT;
        public PgMessageProducer(in string connectionString, in ApplicationObject queue, int yearOffset = 0)
        {
            _YearOffset = yearOffset;
            _connectionString = connectionString;
            Initialize(in queue);
        }
        private void Initialize(in ApplicationObject queue)
        {
            INCOMING_QUEUE_INSERT_SCRIPT =
                new QueryBuilder(DatabaseProvider.PostgreSQL)
                .BuildIncomingQueueInsertScript(in queue);

            try
            {
                _connection = new NpgsqlConnection(_connectionString);
                _connection.Open();

                _command = _connection.CreateCommand();
                _command.CommandType = CommandType.Text;
                _command.CommandText = INCOMING_QUEUE_INSERT_SCRIPT;
                _command.CommandTimeout = 10; // seconds

                ConfigureCommandParameters();
            }
            catch
            {
                Dispose();
                throw;
            }
        }
        private void ConfigureCommandParameters()
        {
            _command.Parameters.Add("Заголовки", NpgsqlDbType.Varchar);
            _command.Parameters.Add("Отправитель", NpgsqlDbType.Varchar);
            _command.Parameters.Add("ТипСообщения", NpgsqlDbType.Varchar);
            _command.Parameters.Add("ТелоСообщения", NpgsqlDbType.Varchar);
            _command.Parameters.Add("ДатаВремя", NpgsqlDbType.Timestamp);
            _command.Parameters.Add("ТипОперации", NpgsqlDbType.Varchar);
            _command.Parameters.Add("ОписаниеОшибки", NpgsqlDbType.Varchar);
            _command.Parameters.Add("КоличествоОшибок", NpgsqlDbType.Integer);
        }
        public void Insert(in IncomingMessage message)
        {
            _command.Parameters["Заголовки"].Value = message.Headers;
            _command.Parameters["Отправитель"].Value = message.Sender;
            _command.Parameters["ТипСообщения"].Value = message.MessageType;
            _command.Parameters["ТелоСообщения"].Value = message.MessageBody;
            _command.Parameters["ДатаВремя"].Value = message.DateTimeStamp.AddYears(_YearOffset);
            _command.Parameters["ТипОперации"].Value = message.OperationType;
            _command.Parameters["ОписаниеОшибки"].Value = message.ErrorDescription;
            _command.Parameters["КоличествоОшибок"].Value = message.ErrorCount;

            _ = _command.ExecuteNonQuery();
        }
        public void TxBegin()
        {
            _transaction = _connection.BeginTransaction();
            _command.Transaction = _transaction;
        }
        public void TxCommit()
        {
            _transaction.Commit();
        }
        public void Dispose()
        {
            if (_connection == null)
            {
                throw new ObjectDisposedException(nameof(PgMessageProducer));
            }

            _command?.Dispose();
            _command = null;

            _transaction?.Dispose();
            _transaction = null;

            _connection?.Dispose();
            _connection = null;
        }
    }
}