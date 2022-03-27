using DaJet.Metadata;
using System.ComponentModel.DataAnnotations.Schema;
using System.Data.Common;

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
            else if (version == 10)
            {
                return new V10.OutgoingMessage();
            }
            else if (version == 11)
            {
                return new V11.OutgoingMessage();
            }
            else if (version == 12)
            {
                return new V12.OutgoingMessage();
            }
            return null;
        }

        /// <summary>
        /// "ТипСообщения" Тип сообщения, например, "Справочник.Номенклатура" - nvarchar(1024)
        /// </summary>
        [Column("ТипСообщения", TypeName = "nvarchar(1024)")] public string MessageType { get; set; } = string.Empty;
        /// <summary>
        /// "ТелоСообщения" Тело сообщения в формате JSON или XML - nvarchar(max)
        /// </summary>
        [Column("ТелоСообщения", TypeName = "nvarchar(max)")] public string MessageBody { get; set; } = string.Empty;
                
        public abstract string GetSelectDataRowsScript(DatabaseProvider provider);
        public abstract void GetMessageData<T>(in T source, in OutgoingMessageDataMapper target) where T : DbDataReader;
    }
}