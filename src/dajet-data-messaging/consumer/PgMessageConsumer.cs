using DaJet.Metadata;
using DaJet.Metadata.Model;
using Npgsql;
using NpgsqlTypes;
using System;
using System.Collections.Generic;
using System.Data;

namespace DaJet.Data.Messaging
{
    public sealed class PgMessageConsumer : IMessageConsumer
    {
        private const string DATABASE_INTERFACE_IS_NOT_SUPPORTED_ERROR
            = "Интерфейс данных исходящей очереди не поддерживается.";

        private int _version;
        private NpgsqlCommand _command;
        private NpgsqlDataReader _reader;
        private NpgsqlConnection _connection;
        private NpgsqlTransaction _transaction;
        private int _recordsAffected;
        private readonly string _connectionString;
        private string OUTGOING_QUEUE_SELECT_SCRIPT;
        private OutgoingMessageDataMapper _message;
        public PgMessageConsumer(in string connectionString, in ApplicationObject queue)
        {
            _connectionString = connectionString;
            InitializeVersion(in queue);
            BuildSelectScript(in queue);
            InitializeDataAccessObjects();
        }
        private void InitializeVersion(in ApplicationObject queue)
        {
            DbInterfaceValidator validator = new DbInterfaceValidator();

            _version = validator.GetOutgoingInterfaceVersion(in queue);

            if (_version < 1)
            {
                throw new Exception(DATABASE_INTERFACE_IS_NOT_SUPPORTED_ERROR);
            }
        }
        private void BuildSelectScript(in ApplicationObject queue)
        {
            _message = OutgoingMessageDataMapper.Create(_version);

            if (_message == null)
            {
                throw new Exception(DATABASE_INTERFACE_IS_NOT_SUPPORTED_ERROR);
            }

            OUTGOING_QUEUE_SELECT_SCRIPT =
                new QueryBuilder(DatabaseProvider.PostgreSQL)
                .BuildOutgoingQueueSelectScript(in queue, _message);
        }
        private void InitializeDataAccessObjects()
        {
            try
            {
                _connection = new NpgsqlConnection(_connectionString);
                _connection.Open();

                _command = _connection.CreateCommand();
                _command.CommandType = CommandType.Text;
                _command.CommandText = OUTGOING_QUEUE_SELECT_SCRIPT;
                _command.CommandTimeout = 60; // seconds

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
           _command.Parameters.Add("MessageCount", NpgsqlDbType.Integer);
        }
        public int RecordsAffected { get { return _recordsAffected; } }
        private IEnumerable<NpgsqlDataReader> SelectDataRows(int limit = 1000)
        {
            _recordsAffected = 0;

            _command.Parameters["MessageCount"].Value = limit;

            using (_reader = _command.ExecuteReader())
            {
                while (_reader.Read())
                {
                    _recordsAffected++;

                    yield return _reader;
                }
                _reader.Close();
            }
        }
        public IEnumerable<OutgoingMessageDataMapper> Select(int limit = 1000)
        {
            foreach (NpgsqlDataReader reader in SelectDataRows(limit))
            {
                _message.GetMessageData(in reader, in _message);

                yield return _message;
            }
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
                throw new ObjectDisposedException(nameof(PgMessageConsumer));
            }

            _reader?.Dispose();
            _reader = null;

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