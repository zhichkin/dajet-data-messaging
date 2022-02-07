using DaJet.Metadata;
using System;
using System.ComponentModel.DataAnnotations;
using System.ComponentModel.DataAnnotations.Schema;
using System.Data;

namespace DaJet.Data.Messaging.V3
{
    /// <summary>
    /// Табличный интерфейс исходящей очереди сообщений
    /// (непериодический независимый регистр сведений)
    /// </summary>
    [Table("РегистрСведений.ИсходящаяОчередь")] [Version(3)] public sealed class OutgoingMessage : OutgoingMessageDataMapper
    {
        #region "INSTANCE PROPERTIES"

        /// <summary>
        /// "МоментВремени" Порядковый номер сообщения (может генерироваться средствами СУБД) - numeric(19,0)
        /// </summary>
        [Column("МоментВремени")] [Key]
        [DatabaseGenerated(DatabaseGeneratedOption.None)]
        public long MessageNumber { get; set; } = 0L;
        /// <summary>
        /// "Идентификатор" Уникальный идентификатор сообщения - binary(16)
        /// </summary>
        [Column("Идентификатор")] public Guid Uuid { get; set; }
        /// <summary>
        /// "Отправитель" Код или UUID отправителя сообщения - nvarchar(36)
        /// </summary>
        [Column("Отправитель")] public string Sender { get; set; } = string.Empty;
        /// <summary>
        /// "Получатели" Коды или UUID получателей сообщения в формате CSV - nvarchar(max)
        /// </summary>
        [Column("Получатели")] public string Recipients { get; set; } = string.Empty;
        /// <summary>
        /// "ДатаВремя" Время создания сообщения - datetime2
        /// </summary>
        [Column("ДатаВремя")] public DateTime DateTimeStamp { get; set; } = DateTime.Now;
        /// <summary>
        /// "ТипОперации" Тип операции: INSERT, UPDATE, UPSERT или DELETE - nvarchar(6)
        /// </summary>
        [Column("ТипОперации")] public string OperationType { get; set; } = string.Empty;
        /// <summary>
        /// "ИдентификаторОбъекта" Уникальный идентификатор объекта 1С в теле сообщения - binary(16)
        /// </summary>
        [Column("ИдентификаторОбъекта")] public Guid Reference { get; set; } = Guid.Empty;

        #endregion

        #region "DATA MAPPING"

        private const string MS_OUTGOING_QUEUE_COMPACTION_SELECT_SCRIPT_TEMPLATE =
            "DECLARE @messages TABLE " +
            "([МоментВремени] numeric(19,0), [Идентификатор] binary(16), [ДатаВремя] datetime2, " +
            "[Отправитель] nvarchar(36), [Получатели] nvarchar(max), [ТипОперации] nvarchar(6), " +
            "[ТипСообщения] nvarchar(1024), [ТелоСообщения] nvarchar(max), [ИдентификаторОбъекта] binary(16)); " +
            "WITH cte AS (SELECT TOP (@MessageCount) " +
            "{МоментВремени} AS [МоментВремени], {Идентификатор} AS [Идентификатор], {ДатаВремя} AS [ДатаВремя], {Отправитель} AS [Отправитель], " +
            "{Получатели} AS [Получатели], {ТипОперации} AS [ТипОперации], {ТипСообщения} AS [ТипСообщения], {ТелоСообщения} AS [ТелоСообщения], " +
            "{ИдентификаторОбъекта} AS [ИдентификаторОбъекта] " +
            "FROM {TABLE_NAME} WITH (ROWLOCK, READPAST) ORDER BY {МоментВремени} ASC, {Идентификатор} ASC) " +
            "DELETE cte OUTPUT deleted.[МоментВремени], deleted.[Идентификатор], deleted.[ДатаВремя], deleted.[Отправитель], " +
            "deleted.[Получатели], deleted.[ТипОперации], deleted.[ТипСообщения], deleted.[ТелоСообщения], deleted.[ИдентификаторОбъекта] " +
            "INTO @messages; " +
            "SELECT [МоментВремени], [Идентификатор], [ДатаВремя], [Отправитель], [Получатели], " +
            "[ТипОперации], [ТипСообщения], [ТелоСообщения], [ИдентификаторОбъекта] " +
            "FROM (SELECT [МоментВремени], [Идентификатор], [ДатаВремя], [Отправитель], [Получатели], " +
            "[ТипОперации], [ТипСообщения], [ТелоСообщения], [ИдентификаторОбъекта],  " +
            "MIN([МоментВремени]) OVER(PARTITION BY [ИдентификаторОбъекта]) AS[Версия] FROM @messages) AS T " +
            "WHERE [ТелоСообщения] <> '' OR [МоментВремени] = [Версия] ORDER BY [МоментВремени] ASC, [Идентификатор] ASC;";

        private const string PG_OUTGOING_QUEUE_COMPACTION_SELECT_SCRIPT_TEMPLATE =
            "WITH " +
            "cte AS (SELECT {МоментВремени}, {Идентификатор} FROM {TABLE_NAME} ORDER BY {МоментВремени} ASC, {Идентификатор} ASC LIMIT @MessageCount), " +
            "del AS (DELETE FROM {TABLE_NAME} t USING cte WHERE t.{МоментВремени} = cte.{МоментВремени} AND t.{Идентификатор} = cte.{Идентификатор} " +
            "RETURNING t.{МоментВремени} AS \"МоментВремени\", t.{Идентификатор} AS \"Идентификатор\", t.{ДатаВремя} AS \"ДатаВремя\", " +
            "CAST(t.{Отправитель} AS varchar) AS \"Отправитель\", CAST(t.{Получатели} AS varchar) AS \"Получатели\", " +
            "CAST(t.{ТипОперации} AS varchar) AS \"ТипОперации\", CAST(t.{ТипСообщения} AS varchar) AS \"ТипСообщения\", " +
            "CAST(t.{ТелоСообщения} AS text) AS \"ТелоСообщения\", t.{ИдентификаторОбъекта} AS \"ИдентификаторОбъекта\"), " +
            "ver AS (SELECT МоментВремени, Идентификатор, ДатаВремя, Отправитель, Получатели, ТипОперации, ТипСообщения, ТелоСообщения, ИдентификаторОбъекта, " +
            "MIN(МоментВремени) OVER(PARTITION BY ИдентификаторОбъекта) AS \"Версия\" FROM del) " +
            "SELECT МоментВремени, Идентификатор, ДатаВремя, Отправитель, Получатели, ТипОперации, ТипСообщения, ТелоСообщения, ИдентификаторОбъекта FROM ver " +
            "WHERE ТелоСообщения <> '' OR МоментВремени = Версия " +
            "ORDER BY МоментВремени ASC, Идентификатор ASC;";

        public override string GetSelectDataRowsScript(DatabaseProvider provider)
        {
            if (provider == DatabaseProvider.SQLServer)
            {
                //return MS_OUTGOING_QUEUE_SELECT_SCRIPT_TEMPLATE;
                return MS_OUTGOING_QUEUE_COMPACTION_SELECT_SCRIPT_TEMPLATE;
            }
            else
            {
                //return PG_OUTGOING_QUEUE_SELECT_SCRIPT_TEMPLATE;
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
            message.Sender = source.IsDBNull("Отправитель") ? string.Empty : source.GetString("Отправитель");
            message.Recipients = source.IsDBNull("Получатели") ? string.Empty : source.GetString("Получатели");
            message.MessageType = source.IsDBNull("ТипСообщения") ? string.Empty : source.GetString("ТипСообщения");
            message.MessageBody = source.IsDBNull("ТелоСообщения") ? string.Empty : source.GetString("ТелоСообщения");
            message.OperationType = source.IsDBNull("ТипОперации") ? string.Empty : source.GetString("ТипОперации");
            message.DateTimeStamp = source.IsDBNull("ДатаВремя") ? DateTime.MinValue : source.GetDateTime("ДатаВремя");
            message.Reference = source.IsDBNull("ИдентификаторОбъекта") ? Guid.Empty : new Guid((byte[])source["ИдентификаторОбъекта"]);
        }

        #endregion
    }
}