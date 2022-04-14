using Microsoft.Extensions.Options;
using Npgsql;
using System.Threading;

namespace DaJet.Data.Messaging.PostgreSQL
{
    public sealed class PgMessageConsumer : IDbMessageConsumer
    {
        private readonly IMessageDataMapper _mapper;
        private readonly DatabaseConsumerOptions _options;
        public PgMessageConsumer(IOptions<DatabaseConsumerOptions> options, IMessageDataMapper mapper)
        {
            _mapper = mapper;
            _options = options.Value;
        }
        public void Consume(in IDbMessageHandler handler, CancellationToken token)
        {
            int consumed;

            DatabaseMessage message = new DatabaseMessage();

            using (NpgsqlConnection connection = new NpgsqlConnection(_options.ConnectionString))
            {
                connection.Open();

                using (NpgsqlCommand command = connection.CreateCommand())
                {
                    _mapper.ConfigureSelectCommand(command);

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

                                    _mapper.MapDataToMessage(reader, in message);

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