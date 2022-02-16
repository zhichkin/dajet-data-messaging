using DaJet.Metadata;
using System.Data.Common;

namespace DaJet.Data.Messaging
{
    public abstract class IncomingMessageDataMapper
    {
        public abstract string GetInsertScript(DatabaseProvider provider);
        public abstract void ConfigureCommandParameters<T>(in T command) where T : DbCommand;
        public abstract void SetMessageData<T>(in IncomingMessageDataMapper source, in T target) where T : DbCommand;
        public static IncomingMessageDataMapper Create(int version)
        {
            if (version == 1)
            {
                return new V1.IncomingMessage();
            }
            else if (version == 10)
            {
                return new V10.IncomingMessage();
            }
            else if (version == 11)
            {
                return new V11.IncomingMessage();
            }
            else if (version == 12)
            {
                return new V12.IncomingMessage();
            }
            return null;
        }
    }
}