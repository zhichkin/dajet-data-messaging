using DaJet.Metadata;
using System.Data.Common;

namespace DaJet.Data.Messaging
{
    public abstract class OutgoingMessageDataMapper
    {
        public abstract string GetSelectDataRowsScript(DatabaseProvider provider);
        public abstract void GetMessageData<T>(in T source, in OutgoingMessageDataMapper target) where T : DbDataReader;
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
    }
}