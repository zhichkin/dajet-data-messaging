using DaJet.Metadata;
using Microsoft.Data.SqlClient;
using Npgsql;
using NpgsqlTypes;
using System;
using System.ComponentModel.DataAnnotations;
using System.ComponentModel.DataAnnotations.Schema;
using System.Data;

namespace DaJet.Data.Messaging.V2
{
    /// <summary>
    /// Табличный интерфейс входящей очереди сообщений
    /// (непериодический независимый регистр сведений)
    /// </summary>
    [Table("РегистрСведений.ВходящаяОчередь")] [Version(2)] public sealed class IncomingMessage : IncomingMessageDataMapper
    {
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
        /// "ТипСообщения" Тип сообщения, например, "Справочник.Номенклатура" - nvarchar(1024)
        /// </summary>
        [Column("ТипСообщения")] public string MessageType { get; set; } = string.Empty;
        /// <summary>
        /// "ТелоСообщения" Тело сообщения в формате JSON или XML - nvarchar(max)
        /// </summary>
        [Column("ТелоСообщения")] public string MessageBody { get; set; } = string.Empty;
        /// <summary>
        /// "ТипОперации" Тип операции: INSERT, UPDATE или DELETE - nvarchar(6)
        /// </summary>
        [Column("ТипОперации")] public string OperationType { get; set; } = string.Empty;
        /// <summary>
        /// "ДатаВремя" Время создания сообщения - datetime2
        /// </summary>
        [Column("ДатаВремя")] public DateTime DateTimeStamp { get; set; } = DateTime.Now;
        /// <summary>
        /// "ОписаниеОшибки" Описание ошибки, возникшей при обработке сообщения - nvarchar(1024)
        /// </summary>
        [Column("ОписаниеОшибки")] public string ErrorDescription { get; set; } = string.Empty;
        /// <summary>
        /// "КоличествоОшибок" Количество неудачных попыток обработки сообщения - numeric(2,0)
        /// </summary>
        [Column("КоличествоОшибок")] public int ErrorCount { get; set; } = 0;

        private const string MS_INCOMING_QUEUE_INSERT_SCRIPT_TEMPLATE =
            "INSERT {TABLE_NAME} " +
            "({МоментВремени}, {Идентификатор}, {Отправитель}, {ТипОперации}, {ТипСообщения}, {ТелоСообщения}, {ДатаВремя}, {ОписаниеОшибки}, {КоличествоОшибок}) " +
            "SELECT NEXT VALUE FOR DaJetIncomingQueueSequence, " +
            "@Идентификатор, @Отправитель, @ТипОперации, @ТипСообщения, @ТелоСообщения, @ДатаВремя, @ОписаниеОшибки, @КоличествоОшибок;";

        private const string PG_INCOMING_QUEUE_INSERT_SCRIPT_TEMPLATE =
            "INSERT INTO {TABLE_NAME} " +
            "({МоментВремени}, {Идентификатор}, {Отправитель}, {ТипОперации}, {ТипСообщения}, {ТелоСообщения}, {ДатаВремя}, {ОписаниеОшибки}, {КоличествоОшибок}) " +
            "SELECT CAST(nextval('DaJetIncomingQueueSequence') AS numeric(19,0)), " +
            "@Идентификатор, CAST(@Отправитель AS mvarchar), CAST(@ТипОперации AS mvarchar), CAST(@ТипСообщения AS mvarchar), " +
            "CAST(@ТелоСообщения AS mvarchar), @ДатаВремя, CAST(@ОписаниеОшибки AS mvarchar), @КоличествоОшибок;";

        public override string GetInsertScript(DatabaseProvider provider)
        {
            if (provider == DatabaseProvider.SQLServer)
            {
                return MS_INCOMING_QUEUE_INSERT_SCRIPT_TEMPLATE;
            }
            else
            {
                return PG_INCOMING_QUEUE_INSERT_SCRIPT_TEMPLATE;
            }
        }
        public override void ConfigureCommandParameters<T>(in T command)
        {
            command.Parameters.Clear();

            if (command is SqlCommand ms)
            {
                ms.Parameters.Add("Идентификатор", SqlDbType.Binary);
                ms.Parameters.Add("Отправитель", SqlDbType.NVarChar);
                ms.Parameters.Add("ТипОперации", SqlDbType.NVarChar);
                ms.Parameters.Add("ТипСообщения", SqlDbType.NVarChar);
                ms.Parameters.Add("ТелоСообщения", SqlDbType.NVarChar);
                ms.Parameters.Add("ДатаВремя", SqlDbType.DateTime2);
                ms.Parameters.Add("ОписаниеОшибки", SqlDbType.NVarChar);
                ms.Parameters.Add("КоличествоОшибок", SqlDbType.Int);
            }
            else if (command is NpgsqlCommand pg)
            {
                pg.Parameters.Add("Идентификатор", NpgsqlDbType.Bytea);
                pg.Parameters.Add("Отправитель", NpgsqlDbType.Varchar);
                pg.Parameters.Add("ТипОперации", NpgsqlDbType.Varchar);
                pg.Parameters.Add("ТипСообщения", NpgsqlDbType.Varchar);
                pg.Parameters.Add("ТелоСообщения", NpgsqlDbType.Varchar);
                pg.Parameters.Add("ДатаВремя", NpgsqlDbType.Timestamp);
                pg.Parameters.Add("ОписаниеОшибки", NpgsqlDbType.Varchar);
                pg.Parameters.Add("КоличествоОшибок", NpgsqlDbType.Integer);
            }
        }
        public override void SetMessageData<T>(in IncomingMessageDataMapper source, in T target)
        {
            if (!(source is IncomingMessage message))
            {
                throw new ArgumentOutOfRangeException(nameof(source));
            }

            target.Parameters["Идентификатор"].Value = message.Uuid.ToByteArray();
            target.Parameters["Отправитель"].Value = message.Sender;
            target.Parameters["ТипОперации"].Value = message.OperationType;
            target.Parameters["ТипСообщения"].Value = message.MessageType;
            target.Parameters["ТелоСообщения"].Value = message.MessageBody;
            target.Parameters["ДатаВремя"].Value = message.DateTimeStamp;
            target.Parameters["ОписаниеОшибки"].Value = message.ErrorDescription;
            target.Parameters["КоличествоОшибок"].Value = message.ErrorCount;
        }
    }
}