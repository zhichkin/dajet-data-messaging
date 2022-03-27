using System;

namespace DaJet.Data.Messaging.Handlers
{
    public sealed class TestDbMessageHandler : DbMessageHandler
    {
        public override void Handle(in DatabaseMessage message)
        {
            Console.WriteLine($"{nameof(TestDbMessageHandler)}: {message.MessageNumber}");

            base.Handle(in message);
        }
    }
    public sealed class MessageHeadersHandler : DbMessageHandler
    {
        public override void Handle(in DatabaseMessage message)
        {
            Console.WriteLine($"{nameof(MessageHeadersHandler)}: {message.Headers}");

            base.Handle(in message);
        }
    }
    public sealed class MessageTypeHandler : DbMessageHandler
    {
        public override void Handle(in DatabaseMessage message)
        {
            Console.WriteLine($"{nameof(MessageTypeHandler)}: {message.MessageType}");

            base.Handle(in message);
        }
    }
    public sealed class MessageBodyHandler : DbMessageHandler
    {
        public override void Handle(in DatabaseMessage message)
        {
            Console.WriteLine($"{nameof(MessageBodyHandler)}: {message.MessageBody}");

            base.Handle(in message);
        }
    }
}