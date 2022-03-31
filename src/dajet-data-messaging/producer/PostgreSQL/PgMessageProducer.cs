using Microsoft.Extensions.Options;
using Npgsql;

namespace DaJet.Data.Messaging.PostgreSQL
{
    public sealed class PgMessageProducer : IDbMessageProducer
    {
        private readonly DatabaseProducerOptions _options;
        private readonly IDataMapperProvider _mapperProvider;
        public PgMessageProducer(IOptions<DatabaseProducerOptions> options, IDataMapperProvider mapperProvider)
        {
            _options = options.Value;
            _mapperProvider = mapperProvider;
        }
        public void Produce(in DatabaseMessage message)
        {
            IMessageDataMapper mapper = _mapperProvider.GetDataMapper<PgMessageProducer>();

            using (NpgsqlConnection connection = new NpgsqlConnection(_options.ConnectionString))
            {
                connection.Open();

                using (NpgsqlCommand command = connection.CreateCommand())
                {
                    mapper.ConfigureInsertCommand(command, in message);

                    _  = command.ExecuteNonQuery();
                }
            }
        }
    }
}