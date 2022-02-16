using DaJet.Metadata;
using System;
using System.ComponentModel.DataAnnotations;
using System.ComponentModel.DataAnnotations.Schema;
using System.Data;

namespace DaJet.Data.Messaging.V12
{
    /// <summary>
    /// Табличный интерфейс исходящей очереди сообщений
    /// (непериодический независимый регистр сведений)
    /// </summary>
    [Table("РегистрСведений.ИсходящаяОчередь")] [Version(12)] public sealed class OutgoingMessage : OutgoingMessageDataMapper
    {
        #region "DATA CONTRACT - INSTANCE PROPERTIES"

        /// <summary>
        /// "МоментВремени" Порядковый номер сообщения (может генерироваться средствами СУБД) - numeric(19,0)
        /// </summary>
        [Column("МоментВремени")] [Key]
        [DatabaseGenerated(DatabaseGeneratedOption.None)]
        public long MessageNumber { get; set; } = 0L;
        /// <summary>
        /// "Идентификатор" Уникальный идентификатор сообщения - binary(16)
        /// </summary>
        [Column("Идентификатор")] [Key] public Guid Uuid { get; set; }
        /// <summary>
        /// "Заголовки" Заголовки сообщения в формате JSON { "ключ": "значение" } - nvarchar(max)
        /// </summary>
        [Column("Заголовки")] public string Headers { get; set; } = string.Empty;
        /// <summary>
        /// "ДатаВремя" Время создания сообщения - datetime2
        /// </summary>
        [Column("ДатаВремя")] public DateTime DateTimeStamp { get; set; } = DateTime.Now;
        /// <summary>
        /// "ИдентификаторОбъекта" Уникальный идентификатор объекта 1С в теле сообщения - binary(16)
        /// </summary>
        [Column("Ссылка")] public Guid Reference { get; set; } = Guid.Empty;

        #endregion

        #region "DATA MAPPING - SELECT QUERY"

        private const string MS_OUTGOING_QUEUE_COMPACTION_SELECT_SCRIPT_TEMPLATE =
            "DECLARE @messages TABLE " +
            "(МоментВремени] numeric(19,0), [Идентификатор] binary(16), [ДатаВремя] datetime2, " +
            "[Заголовки] nvarchar(max), [ТипСообщения] nvarchar(1024), [ТелоСообщения] nvarchar(max), [Ссылка] binary(16)); " +
            "WITH cte AS (SELECT TOP (@MessageCount) " +
            "{МоментВремени} AS [МоментВремени], {Идентификатор} AS [Идентификатор], {ДатаВремя} AS [ДатаВремя], " +
            "{Заголовки} AS [Заголовки], {ТипСообщения} AS [ТипСообщения], {ТелоСообщения} AS [ТелоСообщения], {Ссылка} AS [Ссылка] " +
            "FROM {TABLE_NAME} WITH (ROWLOCK, READPAST) ORDER BY {МоментВремени} ASC, {Идентификатор} ASC) " +
            "DELETE cte OUTPUT " +
            "deleted.[МоментВремени], deleted.[Идентификатор], deleted.[ДатаВремя], " +
            "deleted.[Заголовки], deleted.[ТипСообщения], deleted.[ТелоСообщения], deleted.[Ссылка] " +
            "INTO @messages; " +
            "SELECT [МоментВремени], [Идентификатор], [ДатаВремя], [Заголовки], [ТипСообщения], [ТелоСообщения], [Ссылка] " +
            "FROM (SELECT [МоментВремени], [Идентификатор], [ДатаВремя], [Заголовки], [ТипСообщения], [ТелоСообщения], [Ссылка],  " +
            "MIN([МоментВремени]) OVER(PARTITION BY [Ссылка]) AS [Версия] FROM @messages) AS T " +
            "WHERE [ТелоСообщения] <> '' OR [МоментВремени] = [Версия] ORDER BY [МоментВремени] ASC, [Идентификатор] ASC;";

        private const string PG_OUTGOING_QUEUE_COMPACTION_SELECT_SCRIPT_TEMPLATE =
            "WITH " +
            "cte AS (SELECT {МоментВремени}, {Идентификатор} FROM {TABLE_NAME} ORDER BY {МоментВремени} ASC, {Идентификатор} ASC LIMIT @MessageCount), " +
            "del AS (DELETE FROM {TABLE_NAME} t USING cte WHERE t.{МоментВремени} = cte.{МоментВремени} AND t.{Идентификатор} = cte.{Идентификатор} " +
            "RETURNING t.{МоментВремени} AS \"МоментВремени\", t.{Идентификатор} AS \"Идентификатор\", t.{ДатаВремя} AS \"ДатаВремя\", " +
            "CAST(t.{Заголовки} AS text) AS \"Заголовки\", CAST(t.{ТипСообщения} AS varchar) AS \"ТипСообщения\", " +
            "CAST(t.{ТелоСообщения} AS text) AS \"ТелоСообщения\", t.{Ссылка} AS \"Ссылка\"), " +
            "ver AS (SELECT МоментВремени, Идентификатор, ДатаВремя, Заголовки, ТипСообщения, ТелоСообщения, Ссылка, " +
            "MIN(МоментВремени) OVER(PARTITION BY Ссылка) AS \"Версия\" FROM del) " +
            "SELECT МоментВремени, Идентификатор, ДатаВремя, Заголовки, ТипСообщения, ТелоСообщения, Ссылка FROM ver " +
            "WHERE ТелоСообщения <> '' OR МоментВремени = Версия ORDER BY МоментВремени ASC, Идентификатор ASC;";

        public override string GetSelectDataRowsScript(DatabaseProvider provider)
        {
            if (provider == DatabaseProvider.SQLServer)
            {
                return MS_OUTGOING_QUEUE_COMPACTION_SELECT_SCRIPT_TEMPLATE;
            }
            else
            {
                return PG_OUTGOING_QUEUE_COMPACTION_SELECT_SCRIPT_TEMPLATE;
            }
        }
        public override void GetMessageData<T>(in T source, in OutgoingMessageDataMapper target)
        {
            if (!(target is OutgoingMessage message))
            {
                throw new ArgumentOutOfRangeException(nameof(target));
            }

            message.MessageNumber = source.IsDBNull("МоментВремени") ? 0L : (long)source.GetDecimal("МоментВремени");
            message.Uuid = source.IsDBNull("Идентификатор") ? Guid.Empty : new Guid((byte[])source["Идентификатор"]);
            message.Headers = source.IsDBNull("Заголовки") ? string.Empty : source.GetString("Заголовки");
            message.MessageType = source.IsDBNull("ТипСообщения") ? string.Empty : source.GetString("ТипСообщения");
            message.MessageBody = source.IsDBNull("ТелоСообщения") ? string.Empty : source.GetString("ТелоСообщения");
            message.DateTimeStamp = source.IsDBNull("ДатаВремя") ? DateTime.MinValue : source.GetDateTime("ДатаВремя");
            message.Reference = source.IsDBNull("Ссылка") ? Guid.Empty : new Guid((byte[])source["Ссылка"]);
        }

        #endregion
    }
}