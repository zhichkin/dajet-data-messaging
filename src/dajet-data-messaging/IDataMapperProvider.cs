using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Options;
using System;
using System.Collections.Generic;

namespace DaJet.Data.Messaging
{
    public interface IDataMapperProvider
    {
        void Register<TConsumer, TDataMapper>() where TConsumer : class where TDataMapper : class, IMessageDataMapper;
        IMessageDataMapper GetDataMapper<TConsumer>() where TConsumer : class;
    }
    public sealed class DataMapperProvider : IDataMapperProvider
    {
        #region "STATIC FACTORY MEMBERS"

        public static readonly Func<IServiceProvider, IDataMapperProvider> Factory = CreateDataMapperProvider;
        private static IDataMapperProvider CreateDataMapperProvider(IServiceProvider serviceProvider)
        {
            IDataMapperProvider mapperProvider = new DataMapperProvider(serviceProvider);

            IOptions<DatabaseConsumerOptions> consumerOptions = serviceProvider.GetService<IOptions<DatabaseConsumerOptions>>();

            if (consumerOptions != null)
            {
                mapperProvider.Register<SqlServer.MsMessageConsumer, SqlServer.MsMessageDataMapper>();
                mapperProvider.Register<PostgreSQL.PgMessageConsumer, PostgreSQL.PgMessageDataMapper>();
            }

            IOptions<DatabaseProducerOptions> producerOptions = serviceProvider.GetService<IOptions<DatabaseProducerOptions>>();

            if (producerOptions != null)
            {
                mapperProvider.Register<SqlServer.MsMessageProducer, SqlServer.MsMessageDataMapper>();
                mapperProvider.Register<PostgreSQL.PgMessageProducer, PostgreSQL.PgMessageDataMapper>();
            }

            return mapperProvider;
        }

        #endregion

        #region "INSTANCE MEMBERS"

        private readonly IServiceProvider _services;
        private readonly Dictionary<Type, Type> _registry = new Dictionary<Type, Type>();
        public DataMapperProvider(IServiceProvider services)
        {
            _services = services;
        }
        public void Register<TConsumer, TDataMapper>() where TConsumer : class where TDataMapper : class, IMessageDataMapper
        {
            if (!_registry.TryAdd(typeof(TConsumer), typeof(TDataMapper)))
            {
                throw new ArgumentException(nameof(TConsumer));
            }
        }
        public IMessageDataMapper GetDataMapper<TConsumer>() where TConsumer : class
        {
            if (!_registry.TryGetValue(typeof(TConsumer), out Type implementation))
            {
                throw new InvalidOperationException($"Data mapper for {nameof(TConsumer)} is not found.");
            }

            return _services.GetRequiredService(implementation) as IMessageDataMapper;
        }

        #endregion
    }
}