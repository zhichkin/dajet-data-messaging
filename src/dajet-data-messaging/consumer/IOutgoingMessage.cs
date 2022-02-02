using DaJet.Metadata;
using System.Data.Common;

namespace DaJet.Data.Messaging
{
    public interface IOutgoingMessage
    {
        string GetSelectDataRowsScript(DatabaseProvider provider);
        void GetMessageData<T>(in T source, in IOutgoingMessage target) where T : DbDataReader;
        public static IOutgoingMessage CreateMessage(int version)
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