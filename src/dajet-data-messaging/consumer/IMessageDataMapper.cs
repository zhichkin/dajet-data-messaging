using System.Data.Common;

namespace DaJet.Data.Messaging
{
    public interface IMessageDataMapper
    {
        void ConfigureSelectCommand(in DbCommand command);
        void MapDataToMessage(in DbDataReader reader, in DatabaseMessage message);
        void ConfigureInsertCommand(in DbCommand command, in DatabaseMessage message);
    }
}