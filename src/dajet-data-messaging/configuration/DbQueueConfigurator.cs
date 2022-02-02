using DaJet.Metadata;
using DaJet.Metadata.Model;
using System;
using System.Collections.Generic;

namespace DaJet.Data.Messaging
{
    public sealed class DbQueueConfigurator : IQueueConfigurator
    {
        private readonly int _version;
        private IQueueConfigurator _configurator;
        private readonly string _connectionString;
        private readonly DatabaseProvider _provider;
        public DbQueueConfigurator(int version, DatabaseProvider provider, in string connectionString)
        {
            _version = version;
            _provider = provider;
            _connectionString = connectionString;

            InitializeConfigurator();
        }
        private void InitializeConfigurator()
        {
            if (_version == 1 && _provider == DatabaseProvider.SQLServer)
            {
                _configurator = new V1.MsQueueConfigurator(in _connectionString);
            }
            else if (_version == 1 && _provider == DatabaseProvider.PostgreSQL)
            {
                _configurator = new V1.PgQueueConfigurator(in _connectionString);
            }
            else if (_version == 2 && _provider == DatabaseProvider.SQLServer)
            {
                _configurator = new V2.MsQueueConfigurator(in _connectionString);
            }
            else if (_version == 2 && _provider == DatabaseProvider.PostgreSQL)
            {
                _configurator = new V2.PgQueueConfigurator(in _connectionString);
            }
            else if (_version == 3 && _provider == DatabaseProvider.SQLServer)
            {
                _configurator = new V3.MsQueueConfigurator(in _connectionString);
            }
            else if (_version == 3 && _provider == DatabaseProvider.PostgreSQL)
            {
                _configurator = new V3.PgQueueConfigurator(in _connectionString);
            }
            else
            {
                throw new ArgumentOutOfRangeException(nameof(_version));
            }
        }
        public void ConfigureIncomingMessageQueue(in ApplicationObject queue, out List<string> errors)
        {
            _configurator.ConfigureIncomingMessageQueue(in queue, out errors);
        }
        public void ConfigureOutgoingMessageQueue(in ApplicationObject queue, out List<string> errors)
        {
            _configurator.ConfigureOutgoingMessageQueue(in queue, out errors);
        }
    }
}