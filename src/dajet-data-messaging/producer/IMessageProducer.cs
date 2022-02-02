using System;

namespace DaJet.Data.Messaging
{
    public interface IMessageProducer : IDisposable
    {
        void TxBegin();
        void TxCommit();
        void Insert(in IncomingMessageDataMapper message);
    }
}