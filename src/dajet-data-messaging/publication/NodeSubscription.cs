namespace DaJet.Data.Messaging
{
    public sealed class NodeSubscription
    {
        public string MessageType { get; set; }
        public string NodeQueue { get; set; }
        public string BrokerQueue { get; set; }
        public bool UseVersioning { get; set; }
    }
}