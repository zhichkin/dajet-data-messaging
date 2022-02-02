using DaJet.Metadata;
using DaJet.Metadata.Model;
using Microsoft.Data.SqlClient;
using System;
using System.Data;

namespace DaJet.Data.Messaging
{
    public sealed class MsMessageProducer : IMessageProducer
    {
        private const string DATABASE_INTERFACE_IS_NOT_SUPPORTED_ERROR
            = "Интерфейс данных входящей очереди не поддерживается.";

        private int _version;
        private SqlCommand _command;
        private SqlConnection _connection;
        private SqlTransaction _transaction;
        private readonly string _connectionString;
        private string INCOMING_QUEUE_INSERT_SCRIPT;
        private IncomingMessageDataMapper _message;
        public MsMessageProducer(in string connectionString, in ApplicationObject queue)
        {
            _connectionString = connectionString;
            InitializeVersion(in queue);
            BuildInsertScript(in queue);
            InitializeDataAccessObjects();
        }
        private void InitializeVersion(in ApplicationObject queue)
        {
            DbInterfaceValidator validator = new DbInterfaceValidator();

            _version = validator.GetIncomingInterfaceVersion(in queue);

            if (_version < 1)
            {
                throw new Exception(DATABASE_INTERFACE_IS_NOT_SUPPORTED_ERROR);
            }
        }
        private void BuildInsertScript(in ApplicationObject queue)
        {
            _message = IncomingMessageDataMapper.Create(_version);

            if (_message == null)
            {
                throw new Exception(DATABASE_INTERFACE_IS_NOT_SUPPORTED_ERROR);
            }

            INCOMING_QUEUE_INSERT_SCRIPT =
                new QueryBuilder(DatabaseProvider.SQLServer)
                .BuildIncomingQueueInsertScript(in queue, in _message);
        }
        private void InitializeDataAccessObjects()
        {
            try
            {
                _connection = new SqlConnection(_connectionString);
                _connection.Open();

                _command = _connection.CreateCommand();
                _command.CommandType = CommandType.Text;
                _command.CommandText = INCOMING_QUEUE_INSERT_SCRIPT;
                _command.CommandTimeout = 10; // seconds

                _message.ConfigureCommandParameters(in _command);
            }
            catch
            {
                Dispose();
                throw;
            }
        }
        public void Insert(in IncomingMessageDataMapper message)
        {
            _message.SetMessageData(in message, in _command);

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
                throw new ObjectDisposedException(nameof(MsMessageProducer));
            }

            _command?.Dispose();
            _command = null;

            _transaction?.Dispose();
            _transaction = null;

            _connection?.Dispose();
            _connection = null;

            _message = null;
        }
    }
}