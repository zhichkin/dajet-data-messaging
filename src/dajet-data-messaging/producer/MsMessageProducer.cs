using DaJet.Metadata;
using DaJet.Metadata.Model;
using Microsoft.Data.SqlClient;
using System;
using System.Data;

namespace DaJet.Data.Messaging
{
    public sealed class MsMessageProducer : IMessageProducer
    {
        private SqlCommand _command;
        private SqlConnection _connection;
        private SqlTransaction _transaction;
        private readonly int _YearOffset;
        private readonly string _connectionString;
        private string INCOMING_QUEUE_INSERT_SCRIPT;
        public MsMessageProducer(in string connectionString, in ApplicationObject queue, int yearOffset = 0)
        {
            _YearOffset = yearOffset;
            _connectionString = connectionString;
            Initialize(in queue);
        }
        private void Initialize(in ApplicationObject queue)
        {
            INCOMING_QUEUE_INSERT_SCRIPT =
                new QueryBuilder(DatabaseProvider.SQLServer)
                .BuildIncomingQueueInsertScript(in queue);

            try
            {
                _connection = new SqlConnection(_connectionString);
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
            _command.Parameters.Add("Заголовки", SqlDbType.NVarChar);
            _command.Parameters.Add("Отправитель", SqlDbType.NVarChar);
            _command.Parameters.Add("ТипСообщения", SqlDbType.NVarChar);
            _command.Parameters.Add("ТелоСообщения", SqlDbType.NVarChar);
            _command.Parameters.Add("ДатаВремя", SqlDbType.DateTime2);
            _command.Parameters.Add("ТипОперации", SqlDbType.NVarChar);
            _command.Parameters.Add("ОписаниеОшибки", SqlDbType.NVarChar);
            _command.Parameters.Add("КоличествоОшибок", SqlDbType.Int);
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
                throw new ObjectDisposedException(nameof(MsMessageProducer));
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