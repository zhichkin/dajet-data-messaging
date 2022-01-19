using System;
using System.Collections.Generic;

namespace DaJet.Data.Messaging
{
    public sealed class PublicationNode
    {
        public Guid Uuid { get; set; }
        public string Code{ get; set; }
        public string Name { get; set; }
        public bool IsActive { get; set; }
        public string BrokerServer { get; set; }
        public string NodeOutgoingQueue { get; set; } // data export
        public string NodeIncomingQueue { get; set; } // data import
        public string BrokerOutgoingQueue { get; set; } // data import
        public string BrokerIncomingQueue { get; set; } // data export
        public List<NodePublication> Publications { get; set; }
        public List<NodeSubscription> Subscriptions { get; set; }
    }
}