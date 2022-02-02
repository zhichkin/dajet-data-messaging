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
        public IEnumerable<IDataReader> ExecuteReader(string script, int timeout, Dictionary<string, object> parameters)
        {
            using (DbConnection connection = GetDbConnection())
            {
                connection.Open();

                using (DbCommand command = connection.CreateCommand())
                {
                    command.CommandType = CommandType.Text;
                    command.CommandText = script;
                    command.CommandTimeout = timeout;

                    ConfigureQueryParameters(in command, in parameters);

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
        private void ConfigureQueryParameters(in DbCommand command, in Dictionary<string, object> parameters)
        {
            if (command is SqlCommand ms_command)
            {
                foreach (var parameter in parameters)
                {
                    ms_command.Parameters.AddWithValue(parameter.Key, parameter.Value);
                }
            }
            else if(command is NpgsqlCommand pg_command)
            {
                foreach (var parameter in parameters)
                {
                    pg_command.Parameters.AddWithValue(parameter.Key, parameter.Value);
                }
            }
        }
    }
}