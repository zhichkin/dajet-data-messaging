using DaJet.Metadata;
using System;
using System.ComponentModel.DataAnnotations;
using System.ComponentModel.DataAnnotations.Schema;
using System.Data;

namespace DaJet.Data.Messaging.V1
{
    /// <summary>
    /// Табличный интерфейс исходящей очереди сообщений
    /// (непериодический независимый регистр сведений)
    /// </summary>
    [Table("РегистрСведений.ИсходящаяОчередь")] [Version(1)] public sealed class OutgoingMessage : OutgoingMessageDataMapper
    {
        #region "DATA CONTRACT - INSTANCE PROPERTIES"

        /// <summary>
        /// "НомерСообщения" Порядковый номер сообщения (может генерироваться средствами СУБД) - numeric(19,0)
        /// </summary>
        [Column("НомерСообщения", Order = 0, TypeName = "numeric(19,0)")] [Key] public long MessageNumber { get; set; } = 0L;
        /// <summary>
        /// "Идентификатор" Подстраховка на случай дублирования значения в измерении "НомерСообщения".
        /// Требуется для кода на 1С:Предприятие 8 (тип данных - УникальныйИдентификатор). - binary(16)
        /// </summary>
        [Column("Идентификатор", Order = 1, TypeName = "binary(16)")] [Key] public Guid Uuid { get; set; } = Guid.Empty;
        /// <summary>
        /// "Заголовки" Заголовки сообщения в формате JSON { "ключ": "значение" } - nvarchar(max)
        /// </summary>
        [Column("Заголовки", TypeName = "nvarchar(max)")] public string Headers { get; set; } = string.Empty;
        /// <summary>
        /// "ДатаВремя" Время создания сообщения - datetime2
        /// </summary>
        [Column("ДатаВремя", TypeName = "datetime2")] public DateTime DateTimeStamp { get; set; } = DateTime.MinValue;
        /// <summary>
        /// "Ссылка" Уникальный идентификатор объекта 1С в теле сообщения - binary(16)
        /// </summary>
        [Column("Ссылка", TypeName = "binary(16)")] public Guid Reference { get; set; } = Guid.Empty;

        #endregion

        #region "DATA MAPPING - SELECT QUERY"

        private const string MS_OUTGOING_QUEUE_SELECT_SCRIPT_TEMPLATE =
            "WITH cte AS (SELECT TOP (@MessageCount) " +
            "{НомерСообщения} AS [НомерСообщения], {Идентификатор} AS [Идентификатор], {Заголовки} AS [Заголовки], " +
            "{ТипСообщения} AS [ТипСообщения], {ТелоСообщения} AS [ТелоСообщения], {Ссылка} AS [Ссылка], {ДатаВремя} AS [ДатаВремя] " +
            "FROM {TABLE_NAME} WITH (ROWLOCK, READPAST) ORDER BY {НомерСообщения} ASC, {Идентификатор} ASC) " +
            "DELETE cte OUTPUT deleted.[НомерСообщения], deleted.[Идентификатор], deleted.[Заголовки], " +
            "deleted.[ТипСообщения], deleted.[ТелоСообщения], deleted.[Ссылка], deleted.[ДатаВремя];";

        private const string PG_OUTGOING_QUEUE_SELECT_SCRIPT_TEMPLATE =
            "WITH cte AS (SELECT {НомерСообщения}, {Идентификатор} " +
            "FROM {TABLE_NAME} ORDER BY {НомерСообщения} ASC, {Идентификатор} ASC LIMIT @MessageCount), " +
            "del AS (DELETE FROM {TABLE_NAME} t USING cte " +
            "WHERE t.{НомерСообщения} = cte.{НомерСообщения} AND t.{Идентификатор} = cte.{Идентификатор} " +
            "RETURNING t.{НомерСообщения}, t.{Идентификатор}, " +
            "t.{Заголовки}, t.{ТипСообщения}, t.{ТелоСообщения}, " +
            "t.{Ссылка}, t.{ДатаВремя}) " +
            "SELECT del.{НомерСообщения} AS \"НомерСообщения\", del.{Идентификатор} AS \"Идентификатор\", " +
            "CAST(del.{Заголовки} AS text) AS \"Заголовки\", " +
            "CAST(del.{ТипСообщения} AS varchar) AS \"ТипСообщения\", CAST(del.{ТелоСообщения} AS text) AS \"ТелоСообщения\", " +
            "del.{Ссылка} AS \"Ссылка\", del.{ДатаВремя} AS \"ДатаВремя\" " +
            "FROM del ORDER BY del.{НомерСообщения} ASC;";

        public override string GetSelectDataRowsScript(DatabaseProvider provider)
        {
            if (provider == DatabaseProvider.SQLServer)
            {
                return MS_OUTGOING_QUEUE_SELECT_SCRIPT_TEMPLATE;
            }
            else
            {
                return PG_OUTGOING_QUEUE_SELECT_SCRIPT_TEMPLATE;
            }
        }
        public override void GetMessageData<T>(in T source, in OutgoingMessageDataMapper target)
        {
            if (!(target is OutgoingMessage message))
            {
                throw new ArgumentOutOfRangeException(nameof(target));
            }

            message.MessageNumber = source.IsDBNull("НомерСообщения") ? 0L : (long)source.GetDecimal("НомерСообщения");
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