using System;
using System.ComponentModel.DataAnnotations;
using System.ComponentModel.DataAnnotations.Schema;

namespace DaJet.Data.Messaging
{
    /// <summary>
    /// Табличный интерфейс исходящей очереди сообщений
    /// (непериодический независимый регистр сведений)
    /// </summary>
    [Table("РегистрСведений.ИсходящаяОчередь")] public sealed class OutgoingMessage
    {
        /// <summary>
        /// "НомерСообщения" Порядковый номер сообщения (может генерироваться средствами СУБД) - numeric(19,0)
        /// </summary>
        [Column("НомерСообщения")] [Key]
        [DatabaseGenerated(DatabaseGeneratedOption.None)]
        public long MessageNumber { get; set; } = 0L;
        /// <summary>
        /// "Идентификатор" Уникальный идентификатор объекта 1С в теле сообщения - binary(16)
        /// </summary>
        [Column("Идентификатор")] [Key] public Guid Uuid { get; set; } = Guid.Empty;
        /// <summary>
        /// "Заголовки" Заголовки сообщения в формате JSON { "ключ": "значение" } - nvarchar(max)
        /// </summary>
        [Column("Заголовки")] public string Headers { get; set; } = string.Empty;
        /// <summary>
        /// "ТипСообщения" Тип сообщения, например, "Справочник.Номенклатура" - nvarchar(1024)
        /// </summary>
        [Column("ТипСообщения")] public string MessageType { get; set; } = string.Empty;
        /// <summary>
        /// "ТелоСообщения" Тело сообщения в формате JSON или XML - nvarchar(max)
        /// </summary>
        [Column("ТелоСообщения")] public string MessageBody { get; set; } = string.Empty;
        /// <summary>
        /// "Версия" Версия данных тела сообщения (uuid + timestamp in BASE64 format) - nvarchar(48)
        /// </summary>
        [Column("Версия")] public string Version { get; set; } = string.Empty;
        /// <summary>
        /// "ДатаВремя" Время создания сообщения - datetime2
        /// </summary>
        [Column("ДатаВремя")] public DateTime DateTimeStamp { get; set; } = DateTime.MinValue;
    }
}