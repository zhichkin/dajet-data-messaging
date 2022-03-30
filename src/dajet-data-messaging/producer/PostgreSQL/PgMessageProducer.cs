using Microsoft.Extensions.Options;
using Npgsql;

namespace DaJet.Data.Messaging.PostgreSQL
{
    public sealed class PgMessageProducer : IDbMessageProducer
    {
        private readonly IMessageDataMapper _mapper;
        private readonly DatabaseProducerOptions _options;
        public PgMessageProducer(IOptions<DatabaseProducerOptions> options, IMessageDataMapper mapper)
        {
            _mapper = mapper;
            _options = options.Value;
        }
        public void Produce(in DatabaseMessage message)
        {
            using (NpgsqlConnection connection = new NpgsqlConnection(_options.ConnectionString))
            {
                connection.Open();

                using (NpgsqlCommand command = connection.CreateCommand())
                {
                    _mapper.ConfigureInsertCommand(command, in message);

                    _  = command.ExecuteNonQuery();
                }
            }
        }
    }
}