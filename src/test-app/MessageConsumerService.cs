﻿using DaJet.Data.Messaging;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using System;
using System.Threading;
using System.Threading.Tasks;

namespace test_app
{
    public sealed class MessageConsumerService : BackgroundService
    {
        private readonly IDbMessageHandler _handler;
        private readonly IDbMessageConsumer _consumer;
        private readonly ILogger<MessageConsumerService> _logger;

        private CancellationToken _cancellationToken;
        public MessageConsumerService(IDbMessageConsumer consumer, IDbMessageHandler handler, ILogger<MessageConsumerService> logger)
        {
            _logger = logger;
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
            _logger.LogInformation($"[{nameof(MessageConsumerService)}] Service is running");

            while (!_cancellationToken.IsCancellationRequested)
            {
                try
                {
                    TryDoWork();

                    _logger.LogInformation($"[{nameof(MessageConsumerService)}] Sleep 10 seconds ...");

                    Task.Delay(TimeSpan.FromSeconds(10)).Wait(_cancellationToken);
                }
                catch (OperationCanceledException)
                {
                    // do nothing - the wait task has been canceled
                }
                catch (Exception error)
                {
                    _logger.LogError(error, $"[{nameof(MessageConsumerService)}]");
                    _logger.LogTrace(error, String.Empty);
                }
            }

            _logger.LogInformation($"[{nameof(MessageConsumerService)}] Service is stopped");
        }
        private void TryDoWork()
        {
            _consumer.Consume(in _handler, _cancellationToken);
        }
    }
}