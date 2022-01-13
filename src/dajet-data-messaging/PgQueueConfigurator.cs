﻿using DaJet.Metadata;
using DaJet.Metadata.Model;
using Microsoft.Data.SqlClient;
using Npgsql;
using System;
using System.Collections.Generic;
using System.ComponentModel.DataAnnotations.Schema;
using System.Data;
using System.Data.Common;
using System.Linq;
using System.Reflection;
using System.Threading;
using System.Threading.Tasks;

namespace DaJet.Data.Messaging
{
    public sealed class PgQueueConfigurator : IDatabaseConfigurator
    {
        public string ConnectionString { get; private set; }
        public IDatabaseConfigurator UseConnectionString(string connectionString)
        {
            ConnectionString = connectionString;
            return this;
        }

        #region "CONFIGURE INCOMING QUEUE"

        public void ConfigureIncomingMessageQueue(in InfoBase infoBase, in ApplicationObject queue, out List<string> errors)
        {
            errors = new List<string>();

            if (!IncomingQueueSequenceExists())
            {
                ConfigureIncomingQueue(queue);
            }
        }

        #region "MS incoming queue setup scripts"

        private const string MS_INCOMING_SEQUENCE_EXISTS_SCRIPT = "SELECT 1 FROM sys.sequences WHERE name = 'DaJetIncomingQueueSequence';";

        private const string MS_DROP_INCOMING_SEQUENCE_SCRIPT =
            "IF EXISTS(SELECT 1 FROM sys.sequences WHERE name = N'DaJetIncomingQueueSequence') " +
            "BEGIN DROP SEQUENCE DaJetIncomingQueueSequence; END;";

        private const string MS_CREATE_INCOMING_SEQUENCE_SCRIPT =
            "IF NOT EXISTS(SELECT 1 FROM sys.sequences WHERE name = N'DaJetIncomingQueueSequence') " +
            "BEGIN CREATE SEQUENCE DaJetIncomingQueueSequence AS numeric(19,0) START WITH 1 INCREMENT BY 1; END;";

        private const string MS_DROP_INCOMING_TRIGGER_SCRIPT =
            "IF OBJECT_ID('DaJetIncomingQueue_INSTEAD_OF_INSERT', 'TR') IS NOT NULL " +
            "BEGIN DROP TRIGGER DaJetIncomingQueue_INSTEAD_OF_INSERT; END;";

        private const string MS_CREATE_INCOMING_TRIGGER_SCRIPT =
            "CREATE TRIGGER DaJetIncomingQueue_INSTEAD_OF_INSERT ON {TABLE_NAME} INSTEAD OF INSERT NOT FOR REPLICATION AS " +
            "INSERT {TABLE_NAME} " +
            "({НомерСообщения}, {Идентификатор}, {Отправитель}, {ТипОперации}, {ТипСообщения}, {ТелоСообщения}, {ДатаВремя}, {ОписаниеОшибки}, {КоличествоОшибок}) " +
            "SELECT NEXT VALUE FOR DaJetIncomingQueueSequence, " +
            "i.{Идентификатор}, i.{Отправитель}, i.{ТипОперации}, i.{ТипСообщения}, i.{ТелоСообщения}, i.{ДатаВремя}, i.{ОписаниеОшибки}, i.{КоличествоОшибок} " +
            "FROM inserted AS i;";

        private const string MS_ENABLE_INCOMING_TRIGGER_SCRIPT = "ENABLE TRIGGER DaJetIncomingQueue_INSTEAD_OF_INSERT ON {TABLE_NAME};";

        private const string MS_ENUMERATE_INCOMING_QUEUE_SCRIPT =
            "SELECT {НомерСообщения} AS [НомерСообщения], {Идентификатор} AS [Идентификатор], " +
            "NEXT VALUE FOR DaJetIncomingQueueSequence OVER(ORDER BY {НомерСообщения} ASC, {Идентификатор} ASC) AS [НомерСообщения] " +
            "INTO #{TABLE_NAME}_EnumCopy " +
            "FROM {TABLE_NAME} WITH (TABLOCKX, HOLDLOCK); " +
            "UPDATE T SET T.{НомерСообщения} = C.[НомерСообщения] FROM {TABLE_NAME} AS T " +
            "INNER JOIN #{TABLE_NAME}_EnumCopy AS C ON T.{НомерСообщения} = C.[НомерСообщения] AND T.{Идентификатор} = C.[Идентификатор];";

        private const string MS_DROP_INCOMING_ENUMERATION_TABLE = "DROP TABLE #{TABLE_NAME}_EnumCopy;";

        #endregion

        #region "PG incoming queue setup scripts"

        private const string PG_INCOMING_SEQUENCE_EXISTS_SCRIPT =
            "SELECT 1 FROM information_schema.sequences WHERE LOWER(sequence_name) = LOWER('DaJetIncomingQueueSequence');";

        private const string PG_DROP_INCOMING_SEQUENCE_SCRIPT = "DROP SEQUENCE IF EXISTS DaJetIncomingQueueSequence;";

        private const string PG_CREATE_INCOMING_SEQUENCE_SCRIPT =
            "CREATE SEQUENCE IF NOT EXISTS DaJetIncomingQueueSequence AS bigint INCREMENT BY 1 START WITH 1 CACHE 1;";

        private const string PG_ENUMERATE_INCOMING_QUEUE_SCRIPT =
            "LOCK TABLE {TABLE_NAME} IN ACCESS EXCLUSIVE MODE; " +
            "WITH cte AS (SELECT {НомерСообщения}, {Идентификатор}, nextval('DaJetIncomingQueueSequence') AS msgno " +
            "FROM {TABLE_NAME} ORDER BY {НомерСообщения} ASC, {Идентификатор} ASC) " +
            "UPDATE {TABLE_NAME} SET {НомерСообщения} = CAST(cte.msgno AS numeric(19, 0)) " +
            "FROM cte WHERE {TABLE_NAME}.{НомерСообщения} = cte.{НомерСообщения} AND {TABLE_NAME}.{Идентификатор} = cte.{Идентификатор};";

        private const string PG_DROP_INCOMING_FUNCTION_SCRIPT = "DROP FUNCTION IF EXISTS DaJetIncomingQueue_before_insert_function;";

        private const string PG_CREATE_INCOMING_FUNCTION_SCRIPT =
            "CREATE OR REPLACE FUNCTION DaJetIncomingQueue_before_insert_function() RETURNS trigger AS $$ BEGIN " +
            "NEW.{НомерСообщения} := CAST(nextval('DaJetIncomingQueueSequence') AS numeric(19,0)); RETURN NEW; END $$ LANGUAGE 'plpgsql';";

        private const string PG_DROP_INCOMING_TRIGGER_SCRIPT = "DROP TRIGGER IF EXISTS DaJetIncomingQueue_before_insert_trigger ON {TABLE_NAME};";

        private const string PG_CREATE_INCOMING_TRIGGER_SCRIPT =
            "CREATE TRIGGER DaJetIncomingQueue_before_insert_trigger BEFORE INSERT ON {TABLE_NAME} FOR EACH ROW " +
            "EXECUTE PROCEDURE DaJetIncomingQueue_before_insert_function();";

        #endregion

        public bool IncomingQueueSequenceExists()
        {
            if (DatabaseProvider == DatabaseProvider.SQLServer)
            {
                return MS_IncomingQueueSequenceExists();
            }
            else
            {
                return PG_IncomingQueueSequenceExists();
            }
        }
        private bool MS_IncomingQueueSequenceExists()
        {
            int result = ExecuteScalar<int>(MS_INCOMING_SEQUENCE_EXISTS_SCRIPT);
            return (result == 1);
        }
        private bool PG_IncomingQueueSequenceExists()
        {
            int result = ExecuteScalar<int>(PG_INCOMING_SEQUENCE_EXISTS_SCRIPT);
            return (result == 1);
        }

        public void DropIncomingQueueSequence()
        {
            if (DatabaseProvider == DatabaseProvider.SQLServer)
            {
                ExecuteNonQuery(MS_DROP_INCOMING_SEQUENCE_SCRIPT);
            }
            else
            {
                ExecuteNonQuery(PG_DROP_INCOMING_SEQUENCE_SCRIPT);
            }
        }
        public void DropIncomingQueueTrigger(ApplicationObject queue)
        {
            if (DatabaseProvider == DatabaseProvider.SQLServer)
            {
                ExecuteNonQuery(MS_DROP_INCOMING_TRIGGER_SCRIPT);
            }
            else
            {
                ExecuteNonQuery(ConfigureDatabaseScript(PG_DROP_INCOMING_TRIGGER_SCRIPT, typeof(IncomingMessage), queue));
                ExecuteNonQuery(PG_DROP_INCOMING_FUNCTION_SCRIPT);
            }
        }

        public void ConfigureIncomingQueue(ApplicationObject queue)
        {
            Type template = typeof(IncomingMessage);

            if (DatabaseProvider == DatabaseProvider.SQLServer)
            {
                MS_ConfigureIncomingQueue(queue, template);
            }
            else
            {
                PG_ConfigureIncomingQueue(queue, template);
            }
        }
        private void MS_ConfigureIncomingQueue(ApplicationObject queue, Type template)
        {
            List<string> scripts = new List<string>();

            scripts.Add(MS_CREATE_INCOMING_SEQUENCE_SCRIPT);
            scripts.Add(ConfigureDatabaseScript(MS_ENUMERATE_INCOMING_QUEUE_SCRIPT, template, queue));
            scripts.Add(ConfigureDatabaseScript(MS_DROP_INCOMING_ENUMERATION_TABLE, template, queue));

            TxExecuteNonQuery(scripts);
        }
        private void PG_ConfigureIncomingQueue(ApplicationObject queue, Type template)
        {
            List<string> scripts = new List<string>();

            scripts.Add(PG_CREATE_INCOMING_SEQUENCE_SCRIPT);
            scripts.Add(ConfigureDatabaseScript(PG_ENUMERATE_INCOMING_QUEUE_SCRIPT, template, queue));

            TxExecuteNonQuery(scripts);
        }

        #endregion

        #region "CONFIGURE OUTGOING QUEUE"

        public void ConfigureOutgoingMessageQueue(in InfoBase infoBase, in ApplicationObject queue, out List<string> errors)
        {
            errors = new List<string>();

            if (!OutgoingQueueSequenceExists())
            {
                ConfigureOutgoingQueue(queue);
            }
        }

        #endregion

        public int YearOffset { get; private set; } = 0;




        public bool TryOpenInfoBase(out InfoBase infoBase, out string errorMessage)
        {
            IMetadataService metadataService = new MetadataService()
                .UseDatabaseProvider(DatabaseProvider.SQLServer)
                .UseConnectionString(ConnectionString);

            errorMessage = string.Empty;

            try
            {
                infoBase = metadataService.OpenInfoBase();
                YearOffset = infoBase.YearOffset;
            }
            catch (Exception error)
            {
                infoBase = null;
                errorMessage = ExceptionHelper.GetErrorText(error);
            }

            return (errorMessage == string.Empty);
        }
        public ApplicationObject FindMetadataObjectByName(InfoBase infoBase, string metadataName)
        {
            string[] names = metadataName.Split('.', StringSplitOptions.RemoveEmptyEntries);
            if (names.Length != 2)
            {
                //throw new ArgumentException($"Bad metadata object name format: {nameof(metadataName)}");
                return null;
            }
            string baseType = names[0];
            string typeName = names[1];

            ApplicationObject metaObject = null;
            Dictionary<Guid, ApplicationObject> collection = null;

            if (baseType == "Справочник") collection = infoBase.Catalogs;
            else if (baseType == "Документ") collection = infoBase.Documents;
            else if (baseType == "ПланОбмена") collection = infoBase.Publications;
            else if (baseType == "РегистрСведений") collection = infoBase.InformationRegisters;
            else if (baseType == "РегистрНакопления") collection = infoBase.AccumulationRegisters;

            if (collection == null)
            {
                //throw new ArgumentException($"Collection \"{baseType}\" for metadata object \"{metadataName}\" is not found.");
                return null;
            }

            metaObject = collection.Values.Where(o => o.Name == typeName).FirstOrDefault();

            //if (metaObject == null)
            //{
            //    throw new ArgumentException($"Metadata object \"{metadataName}\" is not found.");
            //}

            return metaObject;
        }
        private DatabaseField GetDatabaseField(ApplicationObject queue, string propertyName)
        {
            foreach (MetadataProperty property in queue.Properties)
            {
                if (property.Name != propertyName)
                {
                    continue;
                }

                if (property.Fields.Count > 0)
                {
                    return property.Fields[0];
                }
            }
            return null;
        }

        public ApplicationObject GetIncomingQueueMetadata(InfoBase infoBase)
        {
            Type template = typeof(IncomingMessage);

            TableAttribute table = template.GetCustomAttribute<TableAttribute>();
            if (table == null || string.IsNullOrWhiteSpace(table.Name))
            {
                //ShowErrorMessage($"TableAttribute is not defined for template type \"{template.FullName}\".");
                return null;
            }

            ApplicationObject queue = FindMetadataObjectByName(infoBase, table.Name);
            if (queue == null)
            {
                //ShowErrorMessage($"Metadata object \"{table.Name}\" is not found.");
                return null;
            }

            IDatabaseInterfaceValidator validator = new InterfaceValidator();
            if (!validator.IncomingInterfaceIsValid(queue, out List<string> errors))
            {
                //ShowErrorMessage($"{table.Name}");
                //foreach (string error in errors)
                //{
                //    ShowErrorMessage(error);
                //}
                return null;
            }

            BuildIncomingQueueInsertScript(queue, template);

            return queue;
        }
        public ApplicationObject GetOutgoingQueueMetadata(InfoBase infoBase)
        {
            Type template = typeof(OutgoingMessage);

            TableAttribute table = template.GetCustomAttribute<TableAttribute>();
            if (table == null || string.IsNullOrWhiteSpace(table.Name))
            {
                //ShowErrorMessage($"TableAttribute is not defined for template type \"{template.FullName}\".");
                return null;
            }

            ApplicationObject queue = FindMetadataObjectByName(infoBase, table.Name);
            if (queue == null)
            {
                //ShowErrorMessage($"Metadata object \"{table.Name}\" is not found.");
                return null;
            }

            IDatabaseInterfaceValidator validator = new InterfaceValidator();
            if (!validator.OutgoingInterfaceIsValid(queue, out List<string> errors))
            {
                //ShowErrorMessage($"{table.Name}");
                //foreach (string error in errors)
                //{
                //    ShowErrorMessage(error);
                //}
                return null;
            }

            BuildOutgoingQueueSelectScript(queue, template, 1000);

            return queue;
        }

        private DbConnection GetDbConnection()
        {
            if (DatabaseProvider == DatabaseProvider.SQLServer)
            {
                return new SqlConnection(ConnectionString);
            }
            return new NpgsqlConnection(ConnectionString);
        }
        private T ExecuteScalar<T>(string script)
        {
            T result = default(T);
            using (DbConnection connection = GetDbConnection())
            using (DbCommand command = connection.CreateCommand())
            {
                command.CommandType = CommandType.Text;
                command.CommandTimeout = 10; // seconds
                command.CommandText = script;
                
                connection.Open();
                object value = command.ExecuteScalar();
                if (value != null)
                {
                    result = (T)value;
                }
            }
            return result;
        }
        private void ExecuteNonQuery(string script)
        {
            using (DbConnection connection = GetDbConnection())
            using (DbCommand command = connection.CreateCommand())
            {
                command.CommandType = CommandType.Text;
                command.CommandTimeout = 10; // seconds
                command.CommandText = script;

                connection.Open();
                _ = command.ExecuteNonQuery();
            }
        }
        private void TxExecuteNonQuery(List<string> scripts)
        {
            using (DbConnection connection = GetDbConnection())
            {
                connection.Open();

                using (DbTransaction transaction = connection.BeginTransaction(IsolationLevel.Serializable))
                using (DbCommand command = connection.CreateCommand())
                {
                    command.Connection = connection;
                    command.Transaction = transaction;
                    command.CommandType = CommandType.Text;
                    command.CommandTimeout = 30; // seconds

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
        private string ConfigureDatabaseScript(string scriptTemplate, Type queueTemplate, ApplicationObject queue)
        {
            string script = scriptTemplate.Replace("{TABLE_NAME}", queue.TableName);

            foreach (PropertyInfo info in queueTemplate.GetProperties())
            {
                ColumnAttribute column = info.GetCustomAttribute<ColumnAttribute>();
                if (column == null)
                {
                    continue;
                }

                DatabaseField field = GetDatabaseField(queue, column.Name);
                if (field == null)
                {
                    continue;
                }

                script = script.Replace($"{{{column.Name}}}", field.Name);
            }

            return script;
        }

        #region "Outgoing Queue Setup"

        #region "MS outgoing queue setup scripts"

        private const string MS_OUTGOING_SEQUENCE_EXISTS_SCRIPT = "SELECT 1 FROM sys.sequences WHERE name = 'DaJetOutgoingQueueSequence';";

        private const string MS_DROP_OUTGOING_SEQUENCE_SCRIPT =
            "IF EXISTS(SELECT 1 FROM sys.sequences WHERE name = N'DaJetOutgoingQueueSequence') " +
            "BEGIN DROP SEQUENCE DaJetOutgoingQueueSequence; END;";

        private const string MS_CREATE_OUTGOING_SEQUENCE_SCRIPT =
            "IF NOT EXISTS(SELECT 1 FROM sys.sequences WHERE name = 'DaJetOutgoingQueueSequence') " +
            "BEGIN CREATE SEQUENCE DaJetOutgoingQueueSequence AS numeric(19,0) START WITH 1 INCREMENT BY 1; END;";

        private const string MS_DROP_OUTGOING_TRIGGER_SCRIPT =
            "IF OBJECT_ID('DaJetOutgoingQueue_INSTEAD_OF_INSERT', 'TR') IS NOT NULL " +
            "BEGIN DROP TRIGGER DaJetOutgoingQueue_INSTEAD_OF_INSERT END;";

        private const string MS_CREATE_OUTGOING_TRIGGER_SCRIPT =
            "CREATE TRIGGER DaJetOutgoingQueue_INSTEAD_OF_INSERT ON {TABLE_NAME} INSTEAD OF INSERT NOT FOR REPLICATION AS " +
            "INSERT {TABLE_NAME} " +
            "({НомерСообщения}, {Идентификатор}, {Отправитель}, {Получатели}, {ТипСообщения}, {ТелоСообщения}, {ДатаВремя}, {ТипОперации}) " +
            "SELECT NEXT VALUE FOR DaJetOutgoingQueueSequence, " +
            "i.{Идентификатор}, i.{Отправитель}, i.{Получатели}, i.{ТипСообщения}, i.{ТелоСообщения}, i.{ДатаВремя}, i.{ТипОперации} " +
            "FROM inserted AS i;";

        private const string MS_ENABLE_OUTGOING_TRIGGER_SCRIPT = "ENABLE TRIGGER DaJetOutgoingQueue_INSTEAD_OF_INSERT ON {TABLE_NAME};";

        private const string MS_ENUMERATE_OUTGOING_QUEUE_SCRIPT =
            "SELECT {НомерСообщения} AS [НомерСообщения], {Идентификатор} AS [Идентификатор], " +
            "NEXT VALUE FOR DaJetOutgoingQueueSequence OVER(ORDER BY {НомерСообщения} ASC, {Идентификатор} ASC) AS [НомерСообщения] " +
            "INTO #{TABLE_NAME}_EnumCopy " +
            "FROM {TABLE_NAME} WITH (TABLOCKX, HOLDLOCK); " +
            "UPDATE T SET T.{НомерСообщения} = C.[НомерСообщения] FROM {TABLE_NAME} AS T " +
            "INNER JOIN #{TABLE_NAME}_EnumCopy AS C ON T.{НомерСообщения} = C.[НомерСообщения] AND T.{Идентификатор} = C.[Идентификатор];";

        private const string MS_DROP_OUTGOING_ENUMERATION_TABLE = "DROP TABLE #{TABLE_NAME}_EnumCopy;";

        #endregion

        #region "PG outgoing queue setup scripts"

        private const string PG_OUTGOING_SEQUENCE_EXISTS_SCRIPT =
            "SELECT 1 FROM information_schema.sequences WHERE LOWER(sequence_name) = LOWER('DaJetOutgoingQueueSequence');";

        private const string PG_DROP_OUTGOING_SEQUENCE_SCRIPT = "DROP SEQUENCE IF EXISTS DaJetOutgoingQueueSequence;";

        private const string PG_CREATE_OUTGOING_SEQUENCE_SCRIPT =
            "CREATE SEQUENCE IF NOT EXISTS DaJetOutgoingQueueSequence AS bigint INCREMENT BY 1 START WITH 1 CACHE 1;";

        private const string PG_ENUMERATE_OUTGOING_QUEUE_SCRIPT =
            "LOCK TABLE {TABLE_NAME} IN ACCESS EXCLUSIVE MODE; " +
            "WITH cte AS (SELECT {НомерСообщения}, {Идентификатор}, nextval('DaJetOutgoingQueueSequence') AS msgno " +
            "FROM {TABLE_NAME} ORDER BY {НомерСообщения} ASC, {Идентификатор} ASC) " +
            "UPDATE {TABLE_NAME} SET {НомерСообщения} = CAST(cte.msgno AS numeric(19, 0)) " +
            "FROM cte WHERE {TABLE_NAME}.{НомерСообщения} = cte.{НомерСообщения} AND {TABLE_NAME}.{Идентификатор} = cte.{Идентификатор};";

        private const string PG_CREATE_OUTGOING_FUNCTION_SCRIPT =
            "CREATE OR REPLACE FUNCTION DaJetOutgoingQueue_before_insert_function() RETURNS trigger AS $$ BEGIN " +
            "NEW.{НомерСообщения} := CAST(nextval('DaJetOutgoingQueueSequence') AS numeric(19,0)); RETURN NEW; END $$ LANGUAGE 'plpgsql';";

        private const string PG_DROP_OUTGOING_TRIGGER_SCRIPT = "DROP TRIGGER IF EXISTS DaJetOutgoingQueue_before_insert_trigger ON {TABLE_NAME};";

        private const string PG_CREATE_OUTGOING_TRIGGER_SCRIPT =
            "CREATE TRIGGER DaJetOutgoingQueue_before_insert_trigger BEFORE INSERT ON {TABLE_NAME} FOR EACH ROW " +
            "EXECUTE PROCEDURE DaJetOutgoingQueue_before_insert_function();";

        #endregion

        public bool OutgoingQueueSequenceExists()
        {
            if (DatabaseProvider == DatabaseProvider.SQLServer)
            {
                return MS_OutgoingQueueSequenceExists();
            }
            else
            {
                return PG_OutgoingQueueSequenceExists();
            }
        }
        private bool MS_OutgoingQueueSequenceExists()
        {
            int result = ExecuteScalar<int>(MS_OUTGOING_SEQUENCE_EXISTS_SCRIPT);
            return (result == 1);
        }
        private bool PG_OutgoingQueueSequenceExists()
        {
            int result = ExecuteScalar<int>(PG_OUTGOING_SEQUENCE_EXISTS_SCRIPT);
            return (result == 1);
        }

        public void DropOutgoingQueueSequence()
        {
            if (DatabaseProvider == DatabaseProvider.SQLServer)
            {
                ExecuteNonQuery(MS_DROP_OUTGOING_SEQUENCE_SCRIPT);
            }
            else
            {
                ExecuteNonQuery(PG_DROP_OUTGOING_SEQUENCE_SCRIPT);
            }
        }

        public void ConfigureOutgoingQueue(ApplicationObject queue)
        {
            Type template = typeof(OutgoingMessage);

            if (DatabaseProvider == DatabaseProvider.SQLServer)
            {
                MS_ConfigureOutgoingQueue(queue, template);
            }
            else
            {
                PG_ConfigureOutgoingQueue(queue, template);
            }
        }
        private void MS_ConfigureOutgoingQueue(ApplicationObject queue, Type template)
        {
            List<string> scripts = new List<string>();

            scripts.Add(MS_CREATE_OUTGOING_SEQUENCE_SCRIPT);
            scripts.Add(ConfigureDatabaseScript(MS_ENUMERATE_OUTGOING_QUEUE_SCRIPT, template, queue));
            scripts.Add(ConfigureDatabaseScript(MS_DROP_OUTGOING_ENUMERATION_TABLE, template, queue));
            scripts.Add(ConfigureDatabaseScript(MS_DROP_OUTGOING_TRIGGER_SCRIPT, template, queue));
            scripts.Add(ConfigureDatabaseScript(MS_CREATE_OUTGOING_TRIGGER_SCRIPT, template, queue));
            scripts.Add(ConfigureDatabaseScript(MS_ENABLE_OUTGOING_TRIGGER_SCRIPT, template, queue));

            TxExecuteNonQuery(scripts);
        }
        private void PG_ConfigureOutgoingQueue(ApplicationObject queue, Type template)
        {
            List<string> scripts = new List<string>();

            scripts.Add(PG_CREATE_OUTGOING_SEQUENCE_SCRIPT);
            scripts.Add(ConfigureDatabaseScript(PG_ENUMERATE_OUTGOING_QUEUE_SCRIPT, template, queue));
            scripts.Add(ConfigureDatabaseScript(PG_CREATE_OUTGOING_FUNCTION_SCRIPT, template, queue));
            scripts.Add(ConfigureDatabaseScript(PG_DROP_OUTGOING_TRIGGER_SCRIPT, template, queue));
            scripts.Add(ConfigureDatabaseScript(PG_CREATE_OUTGOING_TRIGGER_SCRIPT, template, queue));

            TxExecuteNonQuery(scripts);
        }

        #endregion

        #region "Incoming Queue Setup"

        

        #endregion



        #region "Incoming Queue Insert Scripts"

        private const string MS_INCOMING_QUEUE_INSERT_SCRIPT_TEMPLATE =
            "INSERT {TABLE_NAME} " +
            "({НомерСообщения}, {Идентификатор}, {Отправитель}, {ТипОперации}, {ТипСообщения}, {ТелоСообщения}, {ДатаВремя}, {ОписаниеОшибки}, {КоличествоОшибок}) " +
            "SELECT NEXT VALUE FOR DaJetIncomingQueueSequence, " +
            "@Идентификатор, @Отправитель, @ТипОперации, @ТипСообщения, @ТелоСообщения, @ДатаВремя, @ОписаниеОшибки, @КоличествоОшибок;";

        private const string PG_INCOMING_QUEUE_INSERT_SCRIPT_TEMPLATE =
            "INSERT INTO {TABLE_NAME} " +
            "({НомерСообщения}, {Идентификатор}, {Отправитель}, {ТипОперации}, {ТипСообщения}, {ТелоСообщения}, {ДатаВремя}, {ОписаниеОшибки}, {КоличествоОшибок}) " +
            "SELECT CAST(nextval('DaJetIncomingQueueSequence') AS numeric(19,0)), " +
            "@Идентификатор, CAST(@Отправитель AS mvarchar), CAST(@ТипОперации AS mvarchar), CAST(@ТипСообщения AS mvarchar), " +
            "CAST(@ТелоСообщения AS mvarchar), @ДатаВремя, CAST(@ОписаниеОшибки AS mvarchar), @КоличествоОшибок;";

        private string MS_INCOMING_QUEUE_INSERT_SCRIPT = string.Empty;
        private string PG_INCOMING_QUEUE_INSERT_SCRIPT = string.Empty;
        public string IncomingQueueInsertScript
        {
            get
            {
                if (DatabaseProvider == DatabaseProvider.SQLServer)
                {
                    return MS_INCOMING_QUEUE_INSERT_SCRIPT;
                }
                else
                {
                    return PG_INCOMING_QUEUE_INSERT_SCRIPT;
                }
            }
        }
        private void BuildIncomingQueueInsertScript(ApplicationObject queue, Type template)
        {
            if (DatabaseProvider == DatabaseProvider.SQLServer)
            {
                MS_INCOMING_QUEUE_INSERT_SCRIPT = ConfigureDatabaseScript(MS_INCOMING_QUEUE_INSERT_SCRIPT_TEMPLATE, template, queue);
            }
            else
            {
                PG_INCOMING_QUEUE_INSERT_SCRIPT = ConfigureDatabaseScript(PG_INCOMING_QUEUE_INSERT_SCRIPT_TEMPLATE, template, queue);
            }
        }

        #endregion

        #region "Outgoing Queue Select Scripts"

        private const string MS_OUTGOING_QUEUE_SELECT_SCRIPT_TEMPLATE =
            "WITH cte AS (SELECT TOP ({MESSAGE_COUNT}) " +
            "{НомерСообщения} AS [НомерСообщения], {Идентификатор} AS [Идентификатор], {ДатаВремя} AS [ДатаВремя], {Отправитель} AS [Отправитель], " +
            "{Получатели} AS [Получатели], {ТипОперации} AS [ТипОперации], {ТипСообщения} AS [ТипСообщения], {ТелоСообщения} AS [ТелоСообщения] " +
            "FROM {TABLE_NAME} WITH (ROWLOCK, READPAST) ORDER BY {НомерСообщения} ASC, {Идентификатор} ASC) " +
            "DELETE cte OUTPUT deleted.[НомерСообщения], deleted.[Идентификатор], deleted.[ДатаВремя], deleted.[Отправитель], " +
            "deleted.[Получатели], deleted.[ТипОперации], deleted.[ТипСообщения], deleted.[ТелоСообщения];";

        private const string PG_OUTGOING_QUEUE_SELECT_SCRIPT_TEMPLATE =
            "WITH cte AS (SELECT {НомерСообщения}, {Идентификатор} FROM {TABLE_NAME} ORDER BY {НомерСообщения} ASC, {Идентификатор} ASC LIMIT {MESSAGE_COUNT}) " +
            "DELETE FROM {TABLE_NAME} t USING cte WHERE t.{НомерСообщения} = cte.{НомерСообщения} AND t.{Идентификатор} = cte.{Идентификатор} " +
            "RETURNING t.{НомерСообщения} AS \"НомерСообщения\", t.{Идентификатор} AS \"Идентификатор\", " +
            "t.{ДатаВремя} AS \"ДатаВремя\", CAST(t.{Отправитель} AS varchar) AS \"Отправитель\", " +
            "CAST(t.{Получатели} AS varchar) AS \"Получатели\", CAST(t.{ТипОперации} AS varchar) AS \"ТипОперации\", " +
            "CAST(t.{ТипСообщения} AS varchar) AS \"ТипСообщения\", CAST(t.{ТелоСообщения} AS text) AS \"ТелоСообщения\";";

        private string MS_OUTGOING_QUEUE_SELECT_SCRIPT = string.Empty;
        private string PG_OUTGOING_QUEUE_SELECT_SCRIPT = string.Empty;
        public string OutgoingQueueSelectScript
        {
            get
            {
                if (DatabaseProvider == DatabaseProvider.SQLServer)
                {
                    return MS_OUTGOING_QUEUE_SELECT_SCRIPT;
                }
                else
                {
                    return PG_OUTGOING_QUEUE_SELECT_SCRIPT;
                }
            }
        }
        private void BuildOutgoingQueueSelectScript(ApplicationObject queue, Type template, int messageCount)
        {
            if (messageCount == 0)
            {
                messageCount = 1000;
            }

            if (DatabaseProvider == DatabaseProvider.SQLServer)
            {
                MS_OUTGOING_QUEUE_SELECT_SCRIPT = ConfigureDatabaseScript(MS_OUTGOING_QUEUE_SELECT_SCRIPT_TEMPLATE, template, queue);
                MS_OUTGOING_QUEUE_SELECT_SCRIPT = MS_OUTGOING_QUEUE_SELECT_SCRIPT.Replace("{MESSAGE_COUNT}", messageCount.ToString());
            }
            else
            {
                PG_OUTGOING_QUEUE_SELECT_SCRIPT = ConfigureDatabaseScript(PG_OUTGOING_QUEUE_SELECT_SCRIPT_TEMPLATE, template, queue);
                PG_OUTGOING_QUEUE_SELECT_SCRIPT = PG_OUTGOING_QUEUE_SELECT_SCRIPT.Replace("{MESSAGE_COUNT}", messageCount.ToString());
            }
        }

        #endregion

        
        

        private void ValidateAndConfigureOutgoingDatabaseInterface()
        {
            if (!TryOpenInfoBase(out InfoBase infoBase, out string errorMessage))
            {
                throw new Exception($"Failed to load 1C metadata:\n{errorMessage}");
            }

            ApplicationObject queue = GetOutgoingQueueMetadata(infoBase);
            if (queue == null)
            {
                throw new Exception($"1C metadata for the outgoing queue is not found.");
            }

            if (!OutgoingQueueSequenceExists())
            {
                ConfigureOutgoingQueue(queue);
            }
        }
        private void ValidateAndConfigureOutgoingDatabaseInterfaceWithRetry(int retries, TimeSpan delay, CancellationToken cancellationToken, out List<string> errors)
        {
            int attempt = -1;
            bool forever = (retries < 0);
            errors = new List<string>();

            while (forever || attempt < retries) // 0 retries are also valid
            {
                if (cancellationToken != null && cancellationToken.IsCancellationRequested)
                {
                    break;
                }

                if (!forever)
                {
                    attempt++;
                }

                try
                {
                    ValidateAndConfigureOutgoingDatabaseInterface();
                    return;
                }
                catch (Exception error)
                {
                    if (!forever)
                    {
                        if (attempt > 0)
                        {
                            errors.Add($"Validate and configure outgoing database queue error:\n{ExceptionHelper.GetErrorText(error)}");
                        }
                        else
                        {
                            errors.Add($"{attempt}. retry. Validate and configure outgoing database queue error:\n{ExceptionHelper.GetErrorText(error)}");
                        }
                    }
                }
                
                Task.Delay(delay, cancellationToken).Wait();
            }
        }
    }
}