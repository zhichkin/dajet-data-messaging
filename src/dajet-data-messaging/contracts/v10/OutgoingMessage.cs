﻿using DaJet.Metadata;
using System;
using System.ComponentModel.DataAnnotations;
using System.ComponentModel.DataAnnotations.Schema;
using System.Data;

namespace DaJet.Data.Messaging.V10
{
    /// <summary>
    /// Табличный интерфейс исходящей очереди сообщений
    /// (непериодический независимый регистр сведений)
    /// </summary>
    [Table("РегистрСведений.ИсходящаяОчередь")] [Version(10)] public sealed class OutgoingMessage : OutgoingMessageDataMapper
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
        /// "Отправитель" Код или UUID отправителя сообщения - nvarchar(36)
        /// </summary>
        [Column("Отправитель")] public string Sender { get; set; } = string.Empty;
        /// <summary>
        /// "Получатели" Коды или UUID получателей сообщения в формате CSV - nvarchar(max)
        /// </summary>
        [Column("Получатели")] public string Recipients { get; set; } = string.Empty;
        /// <summary>
        /// "ТелоСообщения" Тело сообщения в формате JSON или XML - nvarchar(max)
        /// </summary>
        //[Column("ТелоСообщения")] public string MessageBody { get; set; } = string.Empty;
        /// <summary>
        /// "ДатаВремя" Время создания сообщения - datetime2
        /// </summary>
        [Column("ДатаВремя")] public DateTime DateTimeStamp { get; set; } = DateTime.Now;
        /// <summary>
        /// "ТипОперации" Тип операции: INSERT, UPDATE, UPSERT или DELETE - nvarchar(6)
        /// </summary>
        [Column("ТипОперации")] public string OperationType { get; set; } = string.Empty;

        #endregion

        #region "DATA MAPPING - SELECT QUERY"

        private const string MS_OUTGOING_QUEUE_SELECT_SCRIPT_TEMPLATE =
            "WITH cte AS (SELECT TOP (@MessageCount) " +
            "{МоментВремени} AS [МоментВремени], {Идентификатор} AS [Идентификатор], {ДатаВремя} AS [ДатаВремя], " +
            "{Отправитель} AS [Отправитель], " +
            "{Получатели} AS [Получатели], {ТипОперации} AS [ТипОперации], {ТипСообщения} AS [ТипСообщения], {ТелоСообщения} AS [ТелоСообщения] " +
            "FROM {TABLE_NAME} WITH (ROWLOCK, READPAST) ORDER BY {МоментВремени} ASC, {Идентификатор} ASC) " +
            "DELETE cte OUTPUT deleted.[МоментВремени], deleted.[Идентификатор], deleted.[ДатаВремя], deleted.[Отправитель], " +
            "deleted.[Получатели], deleted.[ТипОперации], deleted.[ТипСообщения], deleted.[ТелоСообщения];";

        private const string PG_OUTGOING_QUEUE_SELECT_SCRIPT_TEMPLATE =
            "WITH cte AS (SELECT {МоментВремени}, {Идентификатор} " +
            "FROM {TABLE_NAME} ORDER BY {МоментВремени} ASC, {Идентификатор} ASC LIMIT @MessageCount), " +
            "del AS (DELETE FROM {TABLE_NAME} t USING cte " +
            "WHERE t.{МоментВремени} = cte.{МоментВремени} AND t.{Идентификатор} = cte.{Идентификатор} " +
            "RETURNING t.{МоментВремени}, t.{Идентификатор}, " +
            "t.{ДатаВремя}, t.{Отправитель}, " +
            "t.{Получатели}, t.{ТипОперации}, " +
            "t.{ТипСообщения}, t.{ТелоСообщения}) " +
            "SELECT del.{МоментВремени} AS \"МоментВремени\", del.{Идентификатор} AS \"Идентификатор\", " +
            "del.{ДатаВремя} AS \"ДатаВремя\", CAST(del.{Отправитель} AS varchar) AS \"Отправитель\", " +
            "CAST(del.{Получатели} AS text) AS \"Получатели\", CAST(del.{ТипОперации} AS varchar) AS \"ТипОперации\", " +
            "CAST(del.{ТипСообщения} AS varchar) AS \"ТипСообщения\", CAST(del.{ТелоСообщения} AS text) AS \"ТелоСообщения\" " +
            "FROM del ORDER BY del.{МоментВремени} ASC;";

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

            message.MessageNumber = source.IsDBNull("МоментВремени") ? 0L : (long)source.GetDecimal("МоментВремени");
            message.Uuid = source.IsDBNull("Идентификатор") ? Guid.Empty : new Guid((byte[])source["Идентификатор"]);
            message.Sender = source.IsDBNull("Отправитель") ? string.Empty : source.GetString("Отправитель");
            message.Recipients = source.IsDBNull("Получатели") ? string.Empty : source.GetString("Получатели");
            message.MessageType = source.IsDBNull("ТипСообщения") ? string.Empty : source.GetString("ТипСообщения");
            message.MessageBody = source.IsDBNull("ТелоСообщения") ? string.Empty : source.GetString("ТелоСообщения");
            message.OperationType = source.IsDBNull("ТипОперации") ? string.Empty : source.GetString("ТипОперации");
            message.DateTimeStamp = source.IsDBNull("ДатаВремя") ? DateTime.MinValue : source.GetDateTime("ДатаВремя");
        }

        #endregion
    }
}