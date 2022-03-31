using Microsoft.Data.SqlClient;
using Microsoft.Extensions.Options;
using System.Threading;

namespace DaJet.Data.Messaging.SqlServer
{
    public sealed class MsMessageConsumer : IDbMessageConsumer
    {
        private readonly DatabaseConsumerOptions _options;
        private readonly IDataMapperProvider _mapperProvider;
        public MsMessageConsumer(IOptions<DatabaseConsumerOptions> options, IDataMapperProvider mapperProvider)
        {
            _options = options.Value;
            _mapperProvider = mapperProvider;
        }
        public void Consume(in IDbMessageHandler handler, CancellationToken token)
        {
            IMessageDataMapper mapper = _mapperProvider.GetDataMapper<MsMessageConsumer>();

            int consumed;

            DatabaseMessage message = new DatabaseMessage();

            using (SqlConnection connection = new SqlConnection(_options.ConnectionString))
            {
                connection.Open();

                using (SqlCommand command = connection.CreateCommand())
                {
                    mapper.ConfigureSelectCommand(command);

                    do
                    {
                        consumed = 0;

                        using (SqlTransaction transaction = connection.BeginTransaction())
                        {
                            command.Transaction = transaction;

                            using (SqlDataReader reader = command.ExecuteReader())
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