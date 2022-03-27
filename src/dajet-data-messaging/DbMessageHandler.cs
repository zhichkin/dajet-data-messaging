namespace DaJet.Data.Messaging
{
    public interface IDbMessageHandler
    {
        bool Confirm();
        void Handle(in DatabaseMessage message);
        IDbMessageHandler Use(in IDbMessageHandler handler);
    }
    public abstract class DbMessageHandler : IDbMessageHandler
    {
        private IDbMessageHandler _next;
        public virtual bool Confirm()
        {
            return true;
        }
        public virtual void Handle(in DatabaseMessage message)
        {
            _next?.Handle(in message);
        }
        public IDbMessageHandler Use(in IDbMessageHandler next)
        {
            _next = next;
            return _next;
        }
    }
}