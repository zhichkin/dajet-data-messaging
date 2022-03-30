namespace DaJet.Data.Messaging.Handlers
{
    public sealed class DbMessageProducerHandler : DbMessageHandler
    {
        private readonly IDbMessageProducer _producer;
        public DbMessageProducerHandler(IDbMessageProducer producer)
        {
            _producer = producer;
        }
        public override void Handle(in DatabaseMessage message)
        {
            base.Handle(in message);

            _producer.Produce(in message);
        }
    }
}