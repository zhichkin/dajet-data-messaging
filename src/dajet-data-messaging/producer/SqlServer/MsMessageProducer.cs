using Microsoft.Data.SqlClient;
using Microsoft.Extensions.Options;

namespace DaJet.Data.Messaging.SqlServer
{
    public sealed class MsMessageProducer : IDbMessageProducer
    {
        private readonly DatabaseProducerOptions _options;
        private readonly IDataMapperProvider _mapperProvider;
        public MsMessageProducer(IOptions<DatabaseProducerOptions> options, IDataMapperProvider mapperProvider)
        {
            _options = options.Value;
            _mapperProvider = mapperProvider;
        }
        public void Produce(in DatabaseMessage message)
        {
            IMessageDataMapper mapper = _mapperProvider.GetDataMapper<MsMessageProducer>();
            
            using (SqlConnection connection = new SqlConnection(_options.ConnectionString))
            {
                connection.Open();

                using (SqlCommand command = connection.CreateCommand())
                {
                    mapper.ConfigureInsertCommand(command, in message);

                    _  = command.ExecuteNonQuery();
                }
            }
        }
    }
}