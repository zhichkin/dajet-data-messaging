﻿using System;
using System.Collections.Generic;

namespace DaJet.Data.Messaging
{
    public interface IMessageConsumer : IDisposable
    {
        void TxBegin();
        void TxCommit();
        IEnumerable<OutgoingMessageDataMapper> Select(int limit = 1000);
        int RecordsAffected { get; }
    }
}