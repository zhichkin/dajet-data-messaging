using DaJet.Metadata;
using DaJet.Metadata.Model;
using System;
using System.Collections.Generic;

namespace DaJet.Data
{
    public sealed class QueryBuilder
    {
        private const string QUEUE_NAME_PLACEHOLDER = "{QUEUE_NAME}";
        private const string TABLE_NAME_PLACEHOLDER = "{TABLE_NAME}";
        private const string TRIGGER_NAME_PLACEHOLDER = "{TRIGGER_NAME}";
        private const string SEQUENCE_NAME_PLACEHOLDER = "{SEQUENCE_NAME}";
        private const string QUEUE_TRIGGER_NAME_TEMPLATE = "DaJet_{QUEUE_NAME}_insert";
        private const string QUEUE_SEQUENCE_NAME_TEMPLATE = "DaJet_{QUEUE_NAME}_sequence";

        private readonly DatabaseProvider _provider;
        public QueryBuilder(DatabaseProvider provider)
        {
            _provider = provider;
        }
        private string CreateSequenceName(in ApplicationObject queue)
        {
            return QUEUE_SEQUENCE_NAME_TEMPLATE.Replace(QUEUE_NAME_PLACEHOLDER, queue.Name);
        }
        private string CreateInsertTriggerName(in ApplicationObject queue)
        {
            return QUEUE_TRIGGER_NAME_TEMPLATE.Replace(QUEUE_NAME_PLACEHOLDER, queue.Name);
        }
        private Dictionary<string, string> GetScriptConfigurationValues(in ApplicationObject queue)
        {
            string TABLE_NAME = queue.TableName;
            string TRIGGER_NAME = CreateInsertTriggerName(in queue);
            string SEQUENCE_NAME = CreateSequenceName(in queue);

            Dictionary<string, string> values = new Dictionary<string, string>()
            {
                { TABLE_NAME_PLACEHOLDER, TABLE_NAME },
                { TRIGGER_NAME_PLACEHOLDER, TRIGGER_NAME },
                { SEQUENCE_NAME_PLACEHOLDER, SEQUENCE_NAME }
            };

            foreach (MetadataProperty property in queue.Properties)
            {
                values.Add($"{{{property.Name}}}", property.Fields[0].Name);
            }

            return values;
        }
        private string ConfigureScript(in string template, in Dictionary<string, string> values)
        {
            string script = template;

            foreach (var item in values)
            {
                script = script.Replace(item.Key, item.Value);
            }

            return script;
        }
        public void ConfigureScripts(in List<string> templates, in ApplicationObject queue, out List<string> scripts)
        {
            scripts = new List<string>();
            
            Dictionary<string, string> values = GetScriptConfigurationValues(in queue);

            for (int i = 0; i < templates.Count; i++)
            {
                scripts.Add(ConfigureScript(templates[i], in values));
            }
        }

        #region "INCOMING QUEUE INSERT SCRIPTS"

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
                if (_provider == DatabaseProvider.SQLServer)
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
            if (_provider == DatabaseProvider.SQLServer)
            {
                MS_INCOMING_QUEUE_INSERT_SCRIPT = ConfigureScript(MS_INCOMING_QUEUE_INSERT_SCRIPT_TEMPLATE, template, queue);
            }
            else
            {
                PG_INCOMING_QUEUE_INSERT_SCRIPT = ConfigureScript(PG_INCOMING_QUEUE_INSERT_SCRIPT_TEMPLATE, template, queue);
            }
        }

        #endregion

        #region "OUTGOING QUEUE SELECT SCRIPTS"

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
                if (_provider == DatabaseProvider.SQLServer)
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

            if (_provider == DatabaseProvider.SQLServer)
            {
                MS_OUTGOING_QUEUE_SELECT_SCRIPT = ConfigureScript(MS_OUTGOING_QUEUE_SELECT_SCRIPT_TEMPLATE, template, queue);
                MS_OUTGOING_QUEUE_SELECT_SCRIPT = MS_OUTGOING_QUEUE_SELECT_SCRIPT.Replace("{MESSAGE_COUNT}", messageCount.ToString());
            }
            else
            {
                PG_OUTGOING_QUEUE_SELECT_SCRIPT = ConfigureScript(PG_OUTGOING_QUEUE_SELECT_SCRIPT_TEMPLATE, template, queue);
                PG_OUTGOING_QUEUE_SELECT_SCRIPT = PG_OUTGOING_QUEUE_SELECT_SCRIPT.Replace("{MESSAGE_COUNT}", messageCount.ToString());
            }
        }

        #endregion
    }
}