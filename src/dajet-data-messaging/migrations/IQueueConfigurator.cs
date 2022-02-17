using DaJet.Metadata.Model;
using System.Collections.Generic;

namespace DaJet.Data.Messaging
{
    public interface IQueueConfigurator
    {
        void ConfigureIncomingMessageQueue(in ApplicationObject queue, out List<string> errors);
        void ConfigureOutgoingMessageQueue(in ApplicationObject queue, out List<string> errors);
    }
}