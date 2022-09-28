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
        /// "ТипОперации" Тип операции: INSERT, UPDATE или DELETE - nvarchar(6)
        /// </summary>
        [Column("ТипОперации")] public string OperationType { get; set; } = string.Empty;
        /// <summary>
        /// "ОписаниеОшибки" Описание ошибки, возникшей при обработке сообщения - nvarchar(1024)
        /// </summary>
        [Column("ОписаниеОшибки")] public string ErrorDescription { get; set; } = string.Empty;
        /// <summary>
        /// "КоличествоОшибок" Количество неудачных попыток обработки сообщения - numeric(2,0)
        /// </summary>
        [Column("КоличествоОшибок")] public int ErrorCount { get; set; } = 0;

        #endregion

        #region "DATA MAPPING - SELECT QUERY"

        private const string MS_OUTGOING_QUEUE_COMPACTION_SELECT_SCRIPT_TEMPLATE =
            "WITH cte AS (SELECT TOP (@MessageCount) " +
            "{МоментВремени} AS [МоментВремени], {Идентификатор} AS [Идентификатор], " +
            "{Заголовки} AS [Заголовки], {Отправитель} AS [Отправитель], {Получатели} AS [Получатели], " +
            "{ТипСообщения} AS [ТипСообщения], {ТелоСообщения} AS [ТелоСообщения], " +
            "{ДатаВремя} AS [ДатаВремя], {ТипОперации} AS [ТипОперации], " +
            "{ОписаниеОшибки} AS [ОписаниеОшибки], {КоличествоОшибок} AS [КоличествоОшибок] " +
            "FROM {TABLE_NAME} WITH (ROWLOCK, READPAST) ORDER BY {МоментВремени} ASC, {Идентификатор} ASC) " +
            "DELETE cte OUTPUT deleted.[МоментВремени], deleted.[Идентификатор], " +
            "deleted.[Заголовки], deleted.[Отправитель], deleted.[Получатели], " +
            "deleted.[ТипСообщения], deleted.[ТелоСообщения], deleted.[ДатаВремя], deleted.[ТипОперации], " +
            "deleted.[ОписаниеОшибки], deleted.[КоличествоОшибок];";

        private const string PG_OUTGOING_QUEUE_COMPACTION_SELECT_SCRIPT_TEMPLATE =
            "WITH cte AS (SELECT {МоментВремени}, {Идентификатор} " +
            "FROM {TABLE_NAME} ORDER BY {МоментВремени} ASC, {Идентификатор} ASC LIMIT @MessageCount), " +
            "del AS (DELETE FROM {TABLE_NAME} t USING cte " +
            "WHERE t.{МоментВремени} = cte.{МоментВремени} AND t.{Идентификатор} = cte.{Идентификатор} " +
            "RETURNING t.{МоментВремени}, t.{Идентификатор}, " +
            "t.{Заголовки}, t.{Отправитель}, t.{Получатели}, " +
            "t.{ТипСообщения}, t.{ТелоСообщения}, " +
            "t.{ДатаВремя}, t.{ТипОперации}, t.{ОписаниеОшибки}, t.{КоличествоОшибок}) " +
            "SELECT del.{МоментВремени} AS \"МоментВремени\", del.{Идентификатор} AS \"Идентификатор\", " +
            "CAST(del.{Заголовки} AS text) AS \"Заголовки\", " +
            "CAST(del.{Отправитель} AS varchar) AS \"Отправитель\", CAST(del.{Получатели} AS text) AS \"Получатели\", " +
            "CAST(del.{ТипСообщения} AS varchar) AS \"ТипСообщения\", CAST(del.{ТелоСообщения} AS text) AS \"ТелоСообщения\", " +
            "del.{ДатаВремя} AS \"ДатаВремя\", CAST(del.{ТипОперации} AS varchar) AS \"ТипОперации\", " +
            "CAST(del.{ОписаниеОшибки} AS varchar) AS \"ОписаниеОшибки\", del.{КоличествоОшибок} AS \"КоличествоОшибок\" " +
            "FROM del ORDER BY del.{МоментВремени} ASC;";

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
            message.Sender = source.IsDBNull("Отправитель") ? string.Empty : source.GetString("Отправитель");
            message.Recipients = source.IsDBNull("Получатели") ? string.Empty : source.GetString("Получатели");
            message.Headers = source.IsDBNull("Заголовки") ? string.Empty : source.GetString("Заголовки");
            message.MessageType = source.IsDBNull("ТипСообщения") ? string.Empty : source.GetString("ТипСообщения");
            message.MessageBody = source.IsDBNull("ТелоСообщения") ? string.Empty : source.GetString("ТелоСообщения");
            message.DateTimeStamp = source.IsDBNull("ДатаВремя") ? DateTime.MinValue : source.GetDateTime("ДатаВремя");
            message.OperationType = source.IsDBNull("ТипОперации") ? string.Empty : source.GetString("ТипОперации");
            message.ErrorCount = source.IsDBNull("КоличествоОшибок") ? 0: (int)source.GetDecimal("КоличествоОшибок");
            message.ErrorDescription = source.IsDBNull("ОписаниеОшибки") ? string.Empty : source.GetString("ОписаниеОшибки");
        }

        #endregion
    }
}