﻿using DaJet.Data.Messaging;
using DaJet.Metadata;
using DaJet.Metadata.Model;
using System;
using System.Collections.Generic;
using System.ComponentModel.DataAnnotations;
using System.Text;

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
        private const string QUEUE_TRIGGER_NAME_TEMPLATE = "{QUEUE_NAME}_tr_insert";
        private const string QUEUE_SEQUENCE_NAME_TEMPLATE = "{QUEUE_NAME}_so";
        private const string QUEUE_FUNCTION_NAME_TEMPLATE = "{QUEUE_NAME}_fn_insert()";

        #endregion

        private readonly DatabaseProvider _provider;
        public QueryBuilder(DatabaseProvider provider)
        {
            _provider = provider;
        }

        #region "QUEUE CONFIGURATION SCRIPTS"

        private string CreateSequenceName(in ApplicationObject queue)
        {
            return QUEUE_SEQUENCE_NAME_TEMPLATE.Replace(QUEUE_NAME_PLACEHOLDER, queue.TableName).ToLower();
        }
        private string CreateFunctionName(in ApplicationObject queue)
        {
            return QUEUE_FUNCTION_NAME_TEMPLATE.Replace(QUEUE_NAME_PLACEHOLDER, queue.TableName).ToLower();
        }
        private string CreateInsertTriggerName(in ApplicationObject queue)
        {
            return QUEUE_TRIGGER_NAME_TEMPLATE.Replace(QUEUE_NAME_PLACEHOLDER, queue.TableName).ToLower();
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
                { FUNCTION_NAME_PLACEHOLDER, FUNCTION_NAME }
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

        public string BuildIncomingQueueInsertScript(in ApplicationObject queue, in IncomingMessageDataMapper message)
        {
            List<string> templates;

            templates = new List<string>() { message.GetInsertScript(_provider) };

            ConfigureScripts(in templates, in queue, out List<string> scripts);

            return scripts[0];
        }

        #endregion

        #region "OUTGOING QUEUE SELECT SCRIPTS"

        public string BuildOutgoingQueueSelectScript(in ApplicationObject queue, in OutgoingMessageDataMapper message)
        {
            List<string> templates;

            templates = new List<string>() { message.GetSelectDataRowsScript(_provider) };

            ConfigureScripts(in templates, in queue, out List<string> scripts);

            return scripts[0];
        }

        #endregion

        #region "PUBLICATION SELECT SCRIPTS"

        private const string MS_PUBLICATION_SELECT_TEMPLATE =
            "SELECT _IDRRef AS [Ссылка], _Code AS [Код], _Description AS [Наименование], " +
            "CAST(_Marked AS bit) AS [ПометкаУдаления], _PredefinedID AS [ЭтотУзел] " +
            "FROM {TABLE_NAME};";

        private const string PG_PUBLICATION_SELECT_TEMPLATE =
            "SELECT _idrref AS \"Ссылка\", CAST(_code AS varchar) AS \"Код\", CAST(_description AS varchar) AS \"Наименование\", " +
            "_marked AS \"ПометкаУдаления\", _predefinedid AS \"ЭтотУзел\" " +
            "FROM {TABLE_NAME};";

        private const string MS_PUBLICATION_NODE_SELECT_TEMPLATE =
            "SELECT _IDRRef AS [Ссылка], _Code AS [Код], _Description AS [Наименование], CAST(_Marked AS bit) AS [ПометкаУдаления], " +
            "{СерверБрокера} AS [СерверБрокера], {ВходящаяОчередьУзла} AS [ВходящаяОчередьУзла], {ИсходящаяОчередьУзла} AS [ИсходящаяОчередьУзла], " +
            "{ВходящаяОчередьБрокера} AS [ВходящаяОчередьБрокера], {ИсходящаяОчередьБрокера} AS [ИсходящаяОчередьБрокера] " +
            "FROM {TABLE_NAME} WHERE _IDRRef = @uuid;";

        private const string MS_PUBLICATION_NODE_PUBLICATIONS_SELECT_TEMPLATE =
            "SELECT {ТипСообщения} AS [ТипСообщения], " +
            "{ОчередьСообщенийУзла} AS [ОчередьСообщенийУзла], {ОчередьСообщенийБрокера} AS [ОчередьСообщенийБрокера] " +
            "FROM {TABLE_NAME} WHERE {Ссылка} = @uuid ORDER BY {НомерСтроки} ASC;";

        private const string MS_PUBLICATION_NODE_SUBSCRIPTIONS_SELECT_TEMPLATE =
            "SELECT {ТипСообщения} AS [ТипСообщения], " +
            "{ОчередьСообщенийУзла} AS [ОчередьСообщенийУзла], " +
            "{ОчередьСообщенийБрокера} AS [ОчередьСообщенийБрокера] " +
            "FROM {TABLE_NAME} WHERE {Ссылка} = @uuid ORDER BY {НомерСтроки} ASC;";

        private const string PG_PUBLICATION_NODE_SELECT_TEMPLATE =
            "SELECT _idrref AS \"Ссылка\", CAST(_code AS varchar) AS \"Код\", CAST(_description AS varchar) AS \"Наименование\", " +
            "_marked AS \"ПометкаУдаления\", CAST({СерверБрокера} AS varchar) AS \"СерверБрокера\", " +
            "CAST({ВходящаяОчередьУзла} AS varchar) AS \"ВходящаяОчередьУзла\", CAST({ИсходящаяОчередьУзла} AS varchar) AS \"ИсходящаяОчередьУзла\", " +
            "CAST({ВходящаяОчередьБрокера} AS varchar) AS \"ВходящаяОчередьБрокера\", CAST({ИсходящаяОчередьБрокера} AS varchar) AS \"ИсходящаяОчередьБрокера\" " +
            "FROM {TABLE_NAME} WHERE _idrref = @uuid;";

        private const string PG_PUBLICATION_NODE_PUBLICATIONS_SELECT_TEMPLATE =
            "SELECT CAST({ТипСообщения} AS varchar) AS \"ТипСообщения\", " +
            "CAST({ОчередьСообщенийУзла} AS varchar) AS \"ОчередьСообщенийУзла\", " +
            "CAST({ОчередьСообщенийБрокера} AS varchar) AS \"ОчередьСообщенийБрокера\" " +
            "FROM {TABLE_NAME} WHERE {Ссылка} = @uuid ORDER BY {НомерСтроки} ASC;";

        private const string PG_PUBLICATION_NODE_SUBSCRIPTIONS_SELECT_TEMPLATE =
            "SELECT CAST({ТипСообщения} AS varchar) AS \"ТипСообщения\", " +
            "CAST({ОчередьСообщенийУзла} AS varchar) AS \"ОчередьСообщенийУзла\", " +
            "CAST({ОчередьСообщенийБрокера} AS varchar) AS \"ОчередьСообщенийБрокера\" " +
            "FROM {TABLE_NAME} WHERE {Ссылка} = @uuid ORDER BY {НомерСтроки} ASC;";

        public string BuildPublicationSelectScript(in Publication publication)
        {
            List<string> templates;

            if (_provider == DatabaseProvider.SQLServer)
            {
                templates = new List<string>() { MS_PUBLICATION_SELECT_TEMPLATE };
            }
            else
            {
                templates = new List<string>() { PG_PUBLICATION_SELECT_TEMPLATE };
            }

            ConfigureScripts(in templates, publication, out List<string> scripts);

            return scripts[0];
        }
        public string BuildPublicationNodeSelectScript(in Publication publication)
        {
            List<string> templates;

            if (_provider == DatabaseProvider.SQLServer)
            {
                templates = new List<string>() { MS_PUBLICATION_NODE_SELECT_TEMPLATE };
            }
            else
            {
                templates = new List<string>() { PG_PUBLICATION_NODE_SELECT_TEMPLATE };
            }

            ConfigureScripts(in templates, publication, out List<string> scripts);

            return scripts[0];
        }
        public string BuildPublicationNodePublicationsSelectScript(in TablePart publications)
        {
            List<string> templates;

            if (_provider == DatabaseProvider.SQLServer)
            {
                templates = new List<string>() { MS_PUBLICATION_NODE_PUBLICATIONS_SELECT_TEMPLATE };
            }
            else
            {
                templates = new List<string>() { PG_PUBLICATION_NODE_PUBLICATIONS_SELECT_TEMPLATE };
            }

            ConfigureScripts(in templates, publications, out List<string> scripts);

            return scripts[0];
        }
        public string BuildPublicationNodeSubscriptionsSelectScript(in TablePart subscriptions)
        {
            List<string> templates;

            if (_provider == DatabaseProvider.SQLServer)
            {
                templates = new List<string>() { MS_PUBLICATION_NODE_SUBSCRIPTIONS_SELECT_TEMPLATE };
            }
            else
            {
                templates = new List<string>() { PG_PUBLICATION_NODE_SUBSCRIPTIONS_SELECT_TEMPLATE };
            }

            ConfigureScripts(in templates, subscriptions, out List<string> scripts);

            return scripts[0];
        }

        #endregion

        public string BuildSelectMessagesScript(in ApplicationObject queue)
        {
            StringBuilder script = new StringBuilder();
            StringBuilder select = new StringBuilder();
            StringBuilder output = new StringBuilder();
            StringBuilder orderby = new StringBuilder();

            script.Append("WITH cte AS (SELECT TOP (@MessageCount) ");

            MetadataProperty property;
            for (int i = 0; i < queue.Properties.Count; i++)
            {
                property = queue.Properties[i];

                if (select.Length > 0) { select.Append(", "); }
                select.Append(property.Fields[0].Name).Append(" AS [").Append(property.Name).Append("]");

                if (output.Length > 0) { output.Append(", "); }
                output.Append("deleted.[").Append(property.Name).Append("]");

                if (property.Purpose == PropertyPurpose.Dimension)
                {
                    if (orderby.Length > 0) { orderby.Append(", "); }
                    orderby.Append(property.Fields[0].Name).Append(" ASC");
                }
            }

            script.Append(select.ToString());
            script.Append(" FROM ").Append(queue.TableName).Append(" WITH (ROWLOCK, READPAST) ORDER BY ");
            script.Append(orderby.ToString()).Append(") DELETE cte OUTPUT ");
            script.Append(output.ToString()).Append(";");

            return script.ToString();
        }
    }
}