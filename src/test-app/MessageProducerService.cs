using DaJet.Data.Messaging;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using System;
using System.Threading;
using System.Threading.Tasks;
using Handlers = DaJet.Data.Messaging.Handlers;

namespace test_app
{
    public sealed class MessageProducerService : BackgroundService
    {
        private readonly IDbMessageConsumer _consumer;
        private readonly IDbMessageProducer _producer;
        private readonly ILogger<MessageProducerService> _logger;

        private CancellationToken _cancellationToken;
        public MessageProducerService(IDbMessageConsumer consumer, IDbMessageProducer producer, ILogger<MessageProducerService> logger)
        {
            _logger = logger;
            _consumer = consumer;
            _producer = producer;
        }
        protected override Task ExecuteAsync(CancellationToken cancellationToken)
        {
            _cancellationToken = cancellationToken;
            return Task.Factory.StartNew(DoWork, TaskCreationOptions.LongRunning);
        }
        private void DoWork()
        {
            _logger.LogInformation($"[{nameof(MessageProducerService)} {Thread.CurrentThread.ManagedThreadId}] Service is running");

            while (!_cancellationToken.IsCancellationRequested)
            {
                try
                {
                    TryDoWork();

                    _logger.LogInformation($"[{nameof(MessageProducerService)}] Sleep 10 seconds ...");

                    Task.Delay(TimeSpan.FromSeconds(10)).Wait(_cancellationToken);
                }
                catch (OperationCanceledException)
                {
                    // do nothing - the wait task has been canceled
                }
                catch (Exception error)
                {
                    _logger.LogError(error, $"[{nameof(MessageProducerService)}]");
                    _logger.LogTrace(error, String.Empty);
                }
            }

            _logger.LogInformation($"[{nameof(MessageProducerService)}] Service is stopped");
        }
        private void TryDoWork()
        {
            // FIXME

            IDbMessageHandler handler = new Handlers.DbMessageProducerHandler(_producer);
            
            handler
                .Use(new Handlers.TestDbMessageHandler())
                .Use(new Handlers.MessageHeadersHandler())
                .Use(new Handlers.MessageTypeHandler())
                .Use(new Handlers.MessageBodyHandler());

            _consumer.Consume(in handler, _cancellationToken);
        }
    }
}