using DaJet.Metadata;
using DaJet.Metadata.Model;
using System;
using System.Collections.Generic;

namespace DaJet.Data.Messaging.V12
{
    public sealed class PgQueueConfigurator : IQueueConfigurator
    {
        private readonly QueryBuilder _builder;
        private readonly QueryExecutor _executor;
        public PgQueueConfigurator(in string connectionString)
        {
            _builder = new QueryBuilder(DatabaseProvider.PostgreSQL);
            _executor = new QueryExecutor(DatabaseProvider.PostgreSQL, in connectionString);
        }

        #region "CONFIGURE SEQUENCE"

        private const string OUTGOING_SEQUENCE_EXISTS_SCRIPT =
            "SELECT 1 FROM information_schema.sequences WHERE LOWER(sequence_name) = LOWER('DaJetOutgoingQueueSequence');";

        private const string CREATE_OUTGOING_SEQUENCE_SCRIPT =
            "CREATE SEQUENCE IF NOT EXISTS DaJetOutgoingQueueSequence AS bigint INCREMENT BY 1 START WITH 1 CACHE 1;";

        private const string INCOMING_SEQUENCE_EXISTS_SCRIPT =
            "SELECT 1 FROM information_schema.sequences WHERE LOWER(sequence_name) = LOWER('DaJetIncomingQueueSequence');";

        private const string CREATE_INCOMING_SEQUENCE_SCRIPT =
            "CREATE SEQUENCE IF NOT EXISTS DaJetIncomingQueueSequence AS bigint INCREMENT BY 1 START WITH 1 CACHE 1;";

        private bool OutgoingSequenceExists()
        {
            return (_executor.ExecuteScalar<int>(OUTGOING_SEQUENCE_EXISTS_SCRIPT, 10) == 1);
        }
        private bool IncomingSequenceExists()
        {
            return (_executor.ExecuteScalar<int>(INCOMING_SEQUENCE_EXISTS_SCRIPT, 10) == 1);
        }

        #endregion

        #region "ENUMERATE QUEUE SCRIPTS"

        private const string ENUMERATE_OUTGOING_QUEUE_SCRIPT =
            "LOCK TABLE {TABLE_NAME} IN ACCESS EXCLUSIVE MODE; " +
            "WITH cte AS (SELECT {МоментВремени}, {Идентификатор}, nextval('DaJetOutgoingQueueSequence') AS msgno " +
            "FROM {TABLE_NAME} ORDER BY {МоментВремени} ASC, {Идентификатор} ASC) " +
            "UPDATE {TABLE_NAME} SET {МоментВремени} = CAST(cte.msgno AS numeric(19, 0)) " +
            "FROM cte WHERE {TABLE_NAME}.{МоментВремени} = cte.{МоментВремени} AND {TABLE_NAME}.{Идентификатор} = cte.{Идентификатор};";

        private const string ENUMERATE_INCOMING_QUEUE_SCRIPT =
            "LOCK TABLE {TABLE_NAME} IN ACCESS EXCLUSIVE MODE; " +
            "WITH cte AS (SELECT {МоментВремени}, {Идентификатор}, nextval('DaJetIncomingQueueSequence') AS msgno " +
            "FROM {TABLE_NAME} ORDER BY {МоментВремени} ASC, {Идентификатор} ASC) " +
            "UPDATE {TABLE_NAME} SET {МоментВремени} = CAST(cte.msgno AS numeric(19, 0)) " +
            "FROM cte WHERE {TABLE_NAME}.{МоментВремени} = cte.{МоментВремени} AND {TABLE_NAME}.{Идентификатор} = cte.{Идентификатор};";

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
            List<string> templates = new List<string>
            {
                CREATE_INCOMING_SEQUENCE_SCRIPT,
                ENUMERATE_INCOMING_QUEUE_SCRIPT
            };

            _builder.ConfigureScripts(in templates, in queue, out List<string> scripts);

            _executor.TxExecuteNonQuery(in scripts, 60);
        }

        #endregion

        #region "CONFIGURE OUTGOING QUEUE"

        private const string OUTGOING_TRIGGER_EXISTS =
            "SELECT 1 FROM information_schema.triggers WHERE LOWER(trigger_name) = LOWER('{TRIGGER_NAME}');";

        private const string CREATE_OUTGOING_FUNCTION_SCRIPT =
            "CREATE OR REPLACE FUNCTION {FUNCTION_NAME} RETURNS trigger AS $$ BEGIN " +
            "NEW.{МоментВремени} := CAST(nextval('DaJetOutgoingQueueSequence') AS numeric(19,0)); RETURN NEW; END $$ LANGUAGE 'plpgsql';";

        private const string DROP_OUTGOING_TRIGGER_SCRIPT = "DROP TRIGGER IF EXISTS {TRIGGER_NAME} ON {TABLE_NAME};";

        private const string CREATE_OUTGOING_TRIGGER_SCRIPT =
            "CREATE TRIGGER {TRIGGER_NAME} BEFORE INSERT ON {TABLE_NAME} FOR EACH ROW EXECUTE PROCEDURE {FUNCTION_NAME};";

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
            List<string> templates = new List<string>
            {
                CREATE_OUTGOING_SEQUENCE_SCRIPT,
                ENUMERATE_OUTGOING_QUEUE_SCRIPT,
                CREATE_OUTGOING_FUNCTION_SCRIPT,
                DROP_OUTGOING_TRIGGER_SCRIPT,
                CREATE_OUTGOING_TRIGGER_SCRIPT
            };

            _builder.ConfigureScripts(in templates, in queue, out List<string> scripts);

            _executor.TxExecuteNonQuery(in scripts, 60);
        }

        #endregion
    }
}