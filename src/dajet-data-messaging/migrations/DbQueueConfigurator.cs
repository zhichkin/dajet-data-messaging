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
            if (_provider == DatabaseProvider.SQLServer)
            {
                if (_version == 1)
                {
                    _configurator = new V1.MsQueueConfigurator(in _connectionString);
                }
                else if (_version == 10)
                {
                    _configurator = new V10.MsQueueConfigurator(in _connectionString);
                }
                else if (_version == 11)
                {
                    _configurator = new V11.MsQueueConfigurator(in _connectionString);
                }
                else if (_version == 12)
                {
                    _configurator = new V12.MsQueueConfigurator(in _connectionString);
                }
            }
            else if (_provider == DatabaseProvider.PostgreSQL)
            {
                if (_version == 1)
                {
                    _configurator = new V1.PgQueueConfigurator(in _connectionString);
                }
                else if (_version == 10)
                {
                    _configurator = new V10.PgQueueConfigurator(in _connectionString);
                }
                else if (_version == 11)
                {
                    _configurator = new V11.PgQueueConfigurator(in _connectionString);
                }
                else if (_version == 12)
                {
                    _configurator = new V12.PgQueueConfigurator(in _connectionString);
                }
            }

            if (_configurator == null)
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