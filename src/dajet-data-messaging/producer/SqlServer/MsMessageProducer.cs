using Microsoft.Data.SqlClient;
using Microsoft.Extensions.Options;

namespace DaJet.Data.Messaging.SqlServer
{
    public sealed class MsMessageProducer : IDbMessageProducer
    {
        private readonly IMessageDataMapper _mapper;
        private readonly DatabaseProducerOptions _options;
        public MsMessageProducer(IOptions<DatabaseProducerOptions> options, IMessageDataMapper mapper)
        {
            _mapper = mapper;
            _options = options.Value;
        }
        public void Produce(in DatabaseMessage message)
        {
            using (SqlConnection connection = new SqlConnection(_options.ConnectionString))
            {
                connection.Open();

                using (SqlCommand command = connection.CreateCommand())
                {
                    _mapper.ConfigureInsertCommand(command, in message);

                    _  = command.ExecuteNonQuery();
                }
            }
        }
    }
}