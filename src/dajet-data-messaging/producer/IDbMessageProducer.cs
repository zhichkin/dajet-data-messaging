namespace DaJet.Data.Messaging
{
    public interface IDbMessageProducer
    {
        void Produce(in DatabaseMessage message);
    }
}