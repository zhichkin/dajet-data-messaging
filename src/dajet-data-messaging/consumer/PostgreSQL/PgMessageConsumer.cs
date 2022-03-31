using Microsoft.Extensions.Options;
using Npgsql;
using System.Threading;

namespace DaJet.Data.Messaging.PostgreSQL
{
    public sealed class PgMessageConsumer : IDbMessageConsumer
    {
        private readonly DatabaseConsumerOptions _options;
        private readonly IDataMapperProvider _mapperProvider;
        public PgMessageConsumer(IOptions<DatabaseConsumerOptions> options, IDataMapperProvider mapperProvider)
        {
            _options = options.Value;
            _mapperProvider = mapperProvider;
        }
        public void Consume(in IDbMessageHandler handler, CancellationToken token)
        {
            IMessageDataMapper mapper = _mapperProvider.GetDataMapper<PgMessageConsumer>();

            int consumed;

            DatabaseMessage message = new DatabaseMessage();

            using (NpgsqlConnection connection = new NpgsqlConnection(_options.ConnectionString))
            {
                connection.Open();

                using (NpgsqlCommand command = connection.CreateCommand())
                {
                    mapper.ConfigureSelectCommand(command);

                    do
                    {
                        consumed = 0;

                        using (NpgsqlTransaction transaction = connection.BeginTransaction())
                        {
                            command.Transaction = transaction;

                            using (NpgsqlDataReader reader = command.ExecuteReader())
                            {
                                while (reader.Read())
                                {
                                    consumed++;

                                    mapper.MapDataToMessage(reader, in message);

                                    handler.Handle(in message);
                                }
                                reader.Close();
                            }

                            if (consumed > 0)
                            {
                                if (handler.Confirm())
                                {
                                    transaction.Commit();
                                }
                                else
                                {
                                    consumed = 0;
                                }
                            }
                        }
                    }
                    while (consumed > 0 && !token.IsCancellationRequested);
                }
            }
        }
    }
}