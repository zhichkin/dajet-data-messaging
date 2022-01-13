using DaJet.Metadata;
using DaJet.Metadata.Model;
using System.Collections.Generic;

namespace DaJet.Data
{
    public sealed class QueryBuilder
    {
        #region "CONSTANTS"

        private const string QUEUE_NAME_PLACEHOLDER = "{QUEUE_NAME}";
        private const string TABLE_NAME_PLACEHOLDER = "{TABLE_NAME}";
        private const string TRIGGER_NAME_PLACEHOLDER = "{TRIGGER_NAME}";
        private const string SEQUENCE_NAME_PLACEHOLDER = "{SEQUENCE_NAME}";
        private const string FUNCTION_NAME_PLACEHOLDER = "{FUNCTION_NAME}";
        private const string MESSAGE_COUNT_PLACEHOLDER = "{MESSAGE_COUNT}";
        private const string QUEUE_TRIGGER_NAME_TEMPLATE = "tr_dajet_{QUEUE_NAME}_insert";
        private const string QUEUE_SEQUENCE_NAME_TEMPLATE = "so_dajet_{QUEUE_NAME}";
        private const string QUEUE_FUNCTION_NAME_TEMPLATE = "fn_dajet_{QUEUE_NAME}_insert()";

        #endregion

        private readonly DatabaseProvider _provider;
        public QueryBuilder(DatabaseProvider provider)
        {
            _provider = provider;
        }

        #region "QUEUE CONFIGURATION SCRIPTS"

        private string CreateSequenceName(in ApplicationObject queue)
        {
            return QUEUE_SEQUENCE_NAME_TEMPLATE.Replace(QUEUE_NAME_PLACEHOLDER, queue.Name).ToLower();
        }
        private string CreateFunctionName(in ApplicationObject queue)
        {
            return QUEUE_FUNCTION_NAME_TEMPLATE.Replace(QUEUE_NAME_PLACEHOLDER, queue.Name).ToLower();
        }
        private string CreateInsertTriggerName(in ApplicationObject queue)
        {
            return QUEUE_TRIGGER_NAME_TEMPLATE.Replace(QUEUE_NAME_PLACEHOLDER, queue.Name).ToLower();
        }
        private Dictionary<string, string> GetScriptConfigurationValues(in ApplicationObject queue)
        {
            string TABLE_NAME = queue.TableName;
            string TRIGGER_NAME = CreateInsertTriggerName(in queue);
            string SEQUENCE_NAME = CreateSequenceName(in queue);
            string FUNCTION_NAME = CreateFunctionName(in queue);

            Dictionary<string, string> values = new Dictionary<string, string>()
            {
                { TABLE_NAME_PLACEHOLDER, TABLE_NAME },
                { TRIGGER_NAME_PLACEHOLDER, TRIGGER_NAME },
                { SEQUENCE_NAME_PLACEHOLDER, SEQUENCE_NAME },
                { FUNCTION_NAME_PLACEHOLDER, FUNCTION_NAME },
                { MESSAGE_COUNT_PLACEHOLDER, 1000.ToString() }
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

        #endregion

        #region "INCOMING QUEUE INSERT SCRIPTS"

        private const string MS_INCOMING_QUEUE_INSERT_SCRIPT_TEMPLATE =
            "INSERT {TABLE_NAME} " +
            "({НомерСообщения}, {Идентификатор}, {Заголовки}, {Отправитель}, {ТипОперации}, {ТипСообщения}, {ТелоСообщения}, {ДатаВремя}, {ОписаниеОшибки}, {КоличествоОшибок}) " +
            "SELECT NEXT VALUE FOR {SEQUENCE_NAME}, " +
            "@Идентификатор, @Заголовки, @Отправитель, @ТипОперации, @ТипСообщения, @ТелоСообщения, @ДатаВремя, @ОписаниеОшибки, @КоличествоОшибок;";

        private const string PG_INCOMING_QUEUE_INSERT_SCRIPT_TEMPLATE =
            "INSERT INTO {TABLE_NAME} " +
            "({НомерСообщения}, {Идентификатор}, {Заголовки}, {Отправитель}, {ТипОперации}, {ТипСообщения}, {ТелоСообщения}, {ДатаВремя}, {ОписаниеОшибки}, {КоличествоОшибок}) " +
            "SELECT CAST(nextval('{SEQUENCE_NAME}') AS numeric(19,0)), " +
            "@Идентификатор, CAST(@Заголовки AS mvarchar), CAST(@Отправитель AS mvarchar), CAST(@ТипОперации AS mvarchar), CAST(@ТипСообщения AS mvarchar), " +
            "CAST(@ТелоСообщения AS mvarchar), @ДатаВремя, CAST(@ОписаниеОшибки AS mvarchar), @КоличествоОшибок;";

        public string BuildIncomingQueueInsertScript(in ApplicationObject queue)
        {
            List<string> templates;

            if (_provider == DatabaseProvider.SQLServer)
            {
                templates = new List<string>() { MS_INCOMING_QUEUE_INSERT_SCRIPT_TEMPLATE };
            }
            else
            {
                templates = new List<string>() { PG_INCOMING_QUEUE_INSERT_SCRIPT_TEMPLATE };
            }

            ConfigureScripts(in templates, in queue, out List<string> scripts);

            return scripts[0];
        }

        #endregion

        #region "OUTGOING QUEUE SELECT SCRIPTS"

        private const string MS_OUTGOING_QUEUE_SELECT_SCRIPT_TEMPLATE =
            "WITH cte AS (SELECT TOP ({MESSAGE_COUNT}) " +
            "{НомерСообщения} AS [НомерСообщения], {Идентификатор} AS [Идентификатор], {Заголовки} AS [Заголовки], {Отправитель} AS [Отправитель], " +
            "{Получатели} AS [Получатели], {ТипОперации} AS [ТипОперации], {ТипСообщения} AS [ТипСообщения], {ТелоСообщения} AS [ТелоСообщения], {ДатаВремя} AS [ДатаВремя] " +
            "FROM {TABLE_NAME} WITH (ROWLOCK, READPAST) ORDER BY {НомерСообщения} ASC, {Идентификатор} ASC) " +
            "DELETE cte OUTPUT deleted.[НомерСообщения], deleted.[Идентификатор], deleted.[Заголовки], deleted.[Отправитель], " +
            "deleted.[Получатели], deleted.[ТипОперации], deleted.[ТипСообщения], deleted.[ТелоСообщения], deleted.[ДатаВремя];";

        private const string PG_OUTGOING_QUEUE_SELECT_SCRIPT_TEMPLATE =
            "WITH cte AS (SELECT {НомерСообщения}, {Идентификатор} FROM {TABLE_NAME} ORDER BY {НомерСообщения} ASC, {Идентификатор} ASC LIMIT {MESSAGE_COUNT}) " +
            "DELETE FROM {TABLE_NAME} t USING cte WHERE t.{НомерСообщения} = cte.{НомерСообщения} AND t.{Идентификатор} = cte.{Идентификатор} " +
            "RETURNING t.{НомерСообщения} AS \"НомерСообщения\", t.{Идентификатор} AS \"Идентификатор\", t.{Заголовки} AS \"Заголовки\", " +
            "CAST(t.{Отправитель} AS varchar) AS \"Отправитель\", CAST(t.{Получатели} AS varchar) AS \"Получатели\", " +
            "CAST(t.{ТипОперации} AS varchar) AS \"ТипОперации\", CAST(t.{ТипСообщения} AS varchar) AS \"ТипСообщения\", " +
            "CAST(t.{ТелоСообщения} AS text) AS \"ТелоСообщения\", t.{ДатаВремя} AS \"ДатаВремя\";";

        public string BuildOutgoingQueueSelectScript(in ApplicationObject queue)
        {
            List<string> templates;

            if (_provider == DatabaseProvider.SQLServer)
            {
                templates = new List<string>() { MS_OUTGOING_QUEUE_SELECT_SCRIPT_TEMPLATE };
            }
            else
            {
                templates = new List<string>() { PG_OUTGOING_QUEUE_SELECT_SCRIPT_TEMPLATE };
            }

            ConfigureScripts(in templates, in queue, out List<string> scripts);

            return scripts[0];
        }

        #endregion
    }
}