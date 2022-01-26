using DaJet.Metadata.Model;

namespace DaJet.Data.Messaging
{
    public sealed class MessagingSettings
    {
        public int YearOffset { get; set; }
        public Publication Publication { get; set; }
        public PublicationNode MainNode { get; set; }
        public ApplicationObject OutgoingQueue { get; set; }
        public ApplicationObject IncomingQueue { get; set; }
    }
}