using DaJet.Metadata;
using DaJet.Metadata.Model;
using System;
using System.Collections.Generic;

namespace DaJet.Data.Messaging.V11
{
    public sealed class MsQueueConfigurator : IQueueConfigurator
    {
        private readonly QueryBuilder _builder;
        private readonly QueryExecutor _executor;
        public MsQueueConfigurator(in string connectionString)
        {
            _builder = new QueryBuilder(DatabaseProvider.SQLServer);
            _executor = new QueryExecutor(DatabaseProvider.SQLServer, in connectionString);
        }

        #region "CONFIGURE SEQUENCE"

        private const string INCOMING_SEQUENCE_EXISTS_SCRIPT = "SELECT 1 FROM sys.sequences WHERE name = 'DaJetIncomingQueueSequence';";

        private const string CREATE_INCOMING_SEQUENCE_SCRIPT =
            "IF NOT EXISTS(SELECT 1 FROM sys.sequences WHERE name = 'DaJetIncomingQueueSequence') " +
            "BEGIN CREATE SEQUENCE DaJetIncomingQueueSequence AS numeric(19,0) START WITH 1 INCREMENT BY 1; END;";

        private const string OUTGOING_SEQUENCE_EXISTS_SCRIPT = "SELECT 1 FROM sys.sequences WHERE name = 'DaJetOutgoingQueueSequence';";

        private const string CREATE_OUTGOING_SEQUENCE_SCRIPT =
            "IF NOT EXISTS(SELECT 1 FROM sys.sequences WHERE name = 'DaJetOutgoingQueueSequence') " +
            "BEGIN CREATE SEQUENCE DaJetOutgoingQueueSequence AS numeric(19,0) START WITH 1 INCREMENT BY 1; END;";

        private bool IncomingSequenceExists()
        {
            return (_executor.ExecuteScalar<int>(INCOMING_SEQUENCE_EXISTS_SCRIPT, 10) == 1);
        }
        private bool OutgoingSequenceExists()
        {
            return (_executor.ExecuteScalar<int>(OUTGOING_SEQUENCE_EXISTS_SCRIPT, 10) == 1);
        }

        #endregion

        #region "ENUMERATE QUEUE SCRIPTS"

        private const string ENUMERATE_INCOMING_QUEUE_SCRIPT =
            "SELECT {МоментВремени} AS [МоментВремени], {Идентификатор} AS [Идентификатор], " +
            "NEXT VALUE FOR DaJetIncomingQueueSequence OVER(ORDER BY {МоментВремени} ASC, {Идентификатор} ASC) AS [НомерСообщения] " +
            "INTO #{TABLE_NAME}_EnumCopy " +
            "FROM {TABLE_NAME} WITH (TABLOCKX, HOLDLOCK); " +
            "UPDATE T SET T.{МоментВремени} = C.[НомерСообщения] FROM {TABLE_NAME} AS T " +
            "INNER JOIN #{TABLE_NAME}_EnumCopy AS C ON T.{МоментВремени} = C.[МоментВремени] AND T.{Идентификатор} = C.[Идентификатор];";

        private const string ENUMERATE_OUTGOING_QUEUE_SCRIPT =
            "SELECT {МоментВремени} AS [МоментВремени], {Идентификатор} AS [Идентификатор], " +
            "NEXT VALUE FOR DaJetOutgoingQueueSequence OVER(ORDER BY {МоментВремени} ASC, {Идентификатор} ASC) AS [НомерСообщения] " +
            "INTO #{TABLE_NAME}_EnumCopy " +
            "FROM {TABLE_NAME} WITH (TABLOCKX, HOLDLOCK); " +
            "UPDATE T SET T.{МоментВремени} = C.[НомерСообщения] FROM {TABLE_NAME} AS T " +
            "INNER JOIN #{TABLE_NAME}_EnumCopy AS C ON T.{МоментВремени} = C.[МоментВремени] AND T.{Идентификатор} = C.[Идентификатор];";

        private const string DROP_ENUMERATION_TABLE = "DROP TABLE #{TABLE_NAME}_EnumCopy;";

        #endregion

        #region "CONFIGURE INCOMING QUEUE"

        public void ConfigureIncomingMessageQueue(in ApplicationObject queue, out List<string> errors)
        {
            errors = new List<string>();

            try
            {
                if (!IncomingSequenceExists())
                {
                    ConfigureIncomingQueue(in queue);
                }
            }
            catch (Exception error)
            {
                errors.Add(ExceptionHelper.GetErrorText(error));
            }
        }
        private void ConfigureIncomingQueue(in ApplicationObject queue)
        {
            List<string> templates = new List<string>()
            {
                CREATE_INCOMING_SEQUENCE_SCRIPT,
                ENUMERATE_INCOMING_QUEUE_SCRIPT,
                DROP_ENUMERATION_TABLE
            };

            _builder.ConfigureScripts(in templates, in queue, out List<string> scripts);

            _executor.TxExecuteNonQuery(in scripts, 60);
        }

        #endregion

        #region "CONFIGURE OUTGOING QUEUE"

        private const string OUTGOING_TRIGGER_EXISTS = "SELECT 1 FROM sys.triggers WHERE name = '{TRIGGER_NAME}';";

        private const string DROP_OUTGOING_TRIGGER_SCRIPT =
            "IF OBJECT_ID('{TRIGGER_NAME}', 'TR') IS NOT NULL BEGIN DROP TRIGGER {TRIGGER_NAME} END;";

        private const string CREATE_OUTGOING_TRIGGER_SCRIPT =
            "CREATE TRIGGER {TRIGGER_NAME} ON {TABLE_NAME} INSTEAD OF INSERT NOT FOR REPLICATION AS " +
            "INSERT {TABLE_NAME} " +
            "({МоментВремени}, {Идентификатор}, {Заголовки}, {Отправитель}, {Получатели}, " +
            "{ТипСообщения}, {ТелоСообщения}, {ДатаВремя}, {Ссылка}, {ТипОперации}) " +
            "SELECT NEXT VALUE FOR DaJetOutgoingQueueSequence, " +
            "i.{Идентификатор}, i.{Заголовки}, i.{Отправитель}, i.{Получатели}, " +
            "i.{ТипСообщения}, i.{ТелоСообщения}, i.{ДатаВремя}, i.{Ссылка}, i.{ТипОперации} " +
            "FROM inserted AS i;";

        private const string ENABLE_OUTGOING_TRIGGER_SCRIPT = "ENABLE TRIGGER {TRIGGER_NAME} ON {TABLE_NAME};";

        public void ConfigureOutgoingMessageQueue(in ApplicationObject queue, out List<string> errors)
        {
            errors = new List<string>();

            try
            {
                if (!OutgoingSequenceExists() || !OutgoingTriggerExists(in queue))
                {
                    ConfigureOutgoingQueue(in queue);
                }
            }
            catch (Exception error)
            {
                errors.Add(ExceptionHelper.GetErrorText(error));
            }
        }
        private bool OutgoingTriggerExists(in ApplicationObject queue)
        {
            List<string> templates = new List<string>()
            {
                OUTGOING_TRIGGER_EXISTS
            };

            _builder.ConfigureScripts(in templates, in queue, out List<string> scripts);

            return (_executor.ExecuteScalar<int>(scripts[0], 10) == 1);
        }
        private void ConfigureOutgoingQueue(in ApplicationObject queue)
        {
            List<string> templates = new List<string>()
            {
                CREATE_OUTGOING_SEQUENCE_SCRIPT,
                ENUMERATE_OUTGOING_QUEUE_SCRIPT,
                DROP_ENUMERATION_TABLE,
                DROP_OUTGOING_TRIGGER_SCRIPT,
                CREATE_OUTGOING_TRIGGER_SCRIPT,
                ENABLE_OUTGOING_TRIGGER_SCRIPT
            };

            _builder.ConfigureScripts(in templates, in queue, out List<string> scripts);

            _executor.TxExecuteNonQuery(in scripts, 60);
        }

        #endregion
    }
}