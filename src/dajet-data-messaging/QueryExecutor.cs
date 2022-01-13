using DaJet.Metadata;
using Microsoft.Data.SqlClient;
using Npgsql;
using System;
using System.Collections.Generic;
using System.Data;
using System.Data.Common;

namespace DaJet.Data
{
    public sealed class QueryExecutor
    {
        private readonly DatabaseProvider _provider;
        private readonly string _connectionString;
        public QueryExecutor(DatabaseProvider provider, in string connectionString)
        {
            _provider = provider;
            _connectionString = connectionString;
        }
        private DbConnection GetDbConnection()
        {
            if (_provider == DatabaseProvider.SQLServer)
            {
                return new SqlConnection(_connectionString);
            }
            return new NpgsqlConnection(_connectionString);
        }
        public T ExecuteScalar<T>(in string script, int timeout)
        {
            T result = default;

            using (DbConnection connection = GetDbConnection())
            {
                connection.Open();

                using (DbCommand command = connection.CreateCommand())
                {
                    command.CommandType = CommandType.Text;
                    command.CommandText = script;
                    command.CommandTimeout = timeout; // seconds

                    object value = command.ExecuteScalar();

                    if (value != null)
                    {
                        result = (T)value;
                    }
                }
            }

            return result;
        }
        public void ExecuteNonQuery(in string script, int timeout)
        {
            using (DbConnection connection = GetDbConnection())
            {
                connection.Open();

                using (DbCommand command = connection.CreateCommand())
                {
                    command.CommandType = CommandType.Text;
                    command.CommandText = script;
                    command.CommandTimeout = timeout;

                    _ = command.ExecuteNonQuery();
                }
            }
        }
        public void TxExecuteNonQuery(in List<string> scripts, int timeout)
        {
            using (DbConnection connection = GetDbConnection())
            {
                connection.Open();

                using (DbTransaction transaction = connection.BeginTransaction(IsolationLevel.Serializable))
                {
                    using (DbCommand command = connection.CreateCommand())
                    {
                        command.Connection = connection;
                        command.Transaction = transaction;
                        command.CommandType = CommandType.Text;
                        command.CommandTimeout = timeout;

                        try
                        {
                            foreach (string script in scripts)
                            {
                                command.CommandText = script;

                                _ = command.ExecuteNonQuery();
                            }

                            transaction.Commit();
                        }
                        catch (Exception error)
                        {
                            try
                            {
                                transaction.Rollback();
                            }
                            finally
                            {
                                throw error;
                            }
                        }
                    }
                }
            }
        }
        public IEnumerable<IDataReader> ExecuteReader(string script, int timeout)
        {
            using (DbConnection connection = GetDbConnection())
            {
                connection.Open();

                using (DbCommand command = connection.CreateCommand())
                {
                    command.CommandType = CommandType.Text;
                    command.CommandText = script;
                    command.CommandTimeout = timeout;
                    
                    // TODO: configure parameters
                    //command.Parameters.AddWithValue("PageSize", size);
                    //command.Parameters.AddWithValue("PageNumber", page);
                    //ConfigureQueryParameters(command, Options.Filter);

                    using (IDataReader reader = command.ExecuteReader())
                    {
                        while (reader.Read())
                        {
                            yield return reader;
                        }
                        reader.Close();
                    }
                }
            }
        }
    }
}