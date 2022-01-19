namespace DaJet.Data.Messaging
{
    public sealed class NodePublication
    {
        public string MessageType { get; set; }
        public string NodeQueue { get; set; }
        public string BrokerQueue { get; set; }
        public bool UseVersioning { get; set; }
    }
}