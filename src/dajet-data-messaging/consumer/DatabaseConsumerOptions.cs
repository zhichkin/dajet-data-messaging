using System.Collections.Generic;

namespace DaJet.Data.Messaging
{
    public sealed class DatabaseConsumerOptions
    {
        public string ConnectionString { get; set; }
        public int YearOffset { get; set; }
        public string QueueTableName { get; set; }
        public Dictionary<string, string> OrderColumns { get; } = new Dictionary<string, string>();
        public Dictionary<string, string> TableColumns { get; } = new Dictionary<string, string>();
        public int MessagesPerTransaction { get; set; } = 1000;
    }
}