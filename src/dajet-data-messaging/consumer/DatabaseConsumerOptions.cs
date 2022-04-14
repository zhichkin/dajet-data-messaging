using DaJet.Metadata;
using System.Collections.Generic;

namespace DaJet.Data.Messaging
{
    public sealed class DatabaseConsumerOptions
    {
        public DatabaseProvider DatabaseProvider { get; set; } = DatabaseProvider.SQLServer;
        public string ConnectionString { get; set; } = string.Empty;
        public int YearOffset { get; set; } = 0;
        public string QueueObject { get; set; } = string.Empty;
        public int MessagesPerTransaction { get; set; } = 1000;

        // ?
        public List<IDbMessageHandler> Handlers { get; set; } = new List<IDbMessageHandler>();

        // DataMapperOptions ?
        public string QueueTable { get; set; } = string.Empty;
        public Dictionary<string, string> OrderColumns { get; } = new Dictionary<string, string>();
        public Dictionary<string, string> TableColumns { get; } = new Dictionary<string, string>();
    }
}