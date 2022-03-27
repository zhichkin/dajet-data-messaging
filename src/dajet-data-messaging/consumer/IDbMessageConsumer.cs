using System.Threading;

namespace DaJet.Data.Messaging
{
    public interface IDbMessageConsumer
    {
        void Consume(in IDbMessageHandler handler, CancellationToken token);
    }
}