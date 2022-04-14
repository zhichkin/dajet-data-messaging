﻿using System.Collections.Generic;

namespace DaJet.Data.Messaging
{
    public sealed class DatabaseProducerOptions
    {
        public string ConnectionString { get; set; }
        public int YearOffset { get; set; }
        public string SequenceObject { get; set; }
        public string QueueTableName { get; set; }
        public Dictionary<string, string> TableColumns { get; } = new Dictionary<string, string>();
    }
}