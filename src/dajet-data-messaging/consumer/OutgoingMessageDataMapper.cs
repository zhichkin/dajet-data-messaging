using DaJet.Metadata;
using System;
using System.Buffers;
using System.ComponentModel.DataAnnotations.Schema;
using System.Data.Common;
using System.Text;

namespace DaJet.Data.Messaging
{
    public abstract class OutgoingMessageDataMapper
    {
        public static OutgoingMessageDataMapper Create(int version)
        {
            if (version == 1)
            {
                return new V1.OutgoingMessage();
            }
            else if (version == 2)
            {
                return new V2.OutgoingMessage();
            }
            else if (version == 3)
            {
                return new V3.OutgoingMessage();
            }
            return null;
        }

        /// <summary>
        /// "ТипСообщения" Тип сообщения, например, "Справочник.Номенклатура" - nvarchar(1024)
        /// </summary>
        [Column("ТипСообщения")] public string MessageType { get; set; } = string.Empty;
        /// <summary>
        /// "ТелоСообщения" Тело сообщения в формате JSON или XML - nvarchar(max)
        /// </summary>
        [Column("ТелоСообщения")] public string MessageBody { get; set; } = string.Empty;

        /// <summary>
        /// Получает тело сообщения в формате UTF-8 для отправки в сетевой канал (RabbitMQ, Apache Kafka и т.п.)
        /// </summary>
        public ReadOnlyMemory<byte> GetMessageBody()
        {
            if (string.IsNullOrEmpty(MessageBody))
            {
                return ReadOnlyMemory<byte>.Empty;
            }

            byte[] buffer = ArrayPool<byte>.Shared.Rent(MessageBody.Length * 2);

            int encoded = Encoding.UTF8.GetBytes(MessageBody, 0, MessageBody.Length, buffer, 0);

            ReadOnlyMemory<byte> messageBody = new ReadOnlyMemory<byte>(buffer, 0, encoded);

            // FIXME:
            // ReadOnlyMemory references buffer, which will be disposed after Return method is called !!!
            ArrayPool<byte>.Shared.Return(buffer);

            return messageBody;
        }

        public abstract string GetSelectDataRowsScript(DatabaseProvider provider);
        public abstract void GetMessageData<T>(in T source, in OutgoingMessageDataMapper target) where T : DbDataReader;
    }
}