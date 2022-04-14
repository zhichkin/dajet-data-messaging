using DaJet.Data.Messaging;
using Microsoft.Extensions.Hosting;
using System;
using System.Threading;
using System.Threading.Tasks;

namespace test_app
{
    public sealed class MessageConsumerService : BackgroundService
    {
        private readonly IDbMessageHandler _handler;
        private readonly IDbMessageConsumer _consumer;

        private CancellationToken _cancellationToken;
        public MessageConsumerService(IDbMessageConsumer consumer, IDbMessageHandler handler)
        {
            _handler = handler;
            _consumer = consumer;
        }
        protected override Task ExecuteAsync(CancellationToken cancellationToken)
        {
            _cancellationToken = cancellationToken;
            return Task.Factory.StartNew(DoWork, TaskCreationOptions.LongRunning);
        }
        private void DoWork()
        {
            while (!_cancellationToken.IsCancellationRequested)
            {
                try
                {
                    TryDoWork();

                    Task.Delay(TimeSpan.FromSeconds(10)).Wait(_cancellationToken);
                }
                catch (OperationCanceledException)
                {
                    // do nothing - the wait task has been canceled
                }
                catch (Exception error)
                {
                    Console.WriteLine(error.Message);
                }
            }
        }
        private void TryDoWork()
        {
            _consumer.Consume(in _handler, _cancellationToken);
        }
    }
}