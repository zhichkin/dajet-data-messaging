using DaJet.Metadata.Model;
using System;
using System.Collections.Generic;
using System.ComponentModel.DataAnnotations.Schema;
using System.Reflection;

namespace DaJet.Data.Messaging
{
    public sealed class DbInterfaceValidator
    {
        private List<Type> OutgoingMessageVersions { get; } = new List<Type>()
        {
            typeof(V1.OutgoingMessage),
            typeof(V2.OutgoingMessage),
            typeof(V3.OutgoingMessage)
        };
        public int GetIncomingInterfaceVersion(in ApplicationObject queue)
        {
            if (VersionMatches(in queue, typeof(IncomingMessage)))
            {
                return 1;
            }

            return -1;
        }
        public int GetOutgoingInterfaceVersion(in ApplicationObject queue)
        {
            VersionAttribute version;

            foreach (Type message in OutgoingMessageVersions)
            {
                if (VersionMatches(in queue, in message))
                {
                    version = message.GetCustomAttribute<VersionAttribute>();

                    if (version != null)
                    {
                        return version.Version;
                    }
                }
            }
            
            return -1;
        }
        private bool VersionMatches(in ApplicationObject queue, in Type template)
        {
            PropertyInfo[] properties = template.GetProperties();

            if (properties.Length != queue.Properties.Count)
            {
                return false;
            }

            foreach (PropertyInfo property in properties)
            {
                ColumnAttribute column = property.GetCustomAttribute<ColumnAttribute>();

                if (column == null)
                {
                    return false;
                }

                if (!PropertyExists(in queue, column.Name))
                {
                    return false;
                }
            }

            return true;
        }
        private bool PropertyExists(in ApplicationObject queue, string propertyName)
        {
            for (int p = 0; p < queue.Properties.Count; p++)
            {
                if (queue.Properties[p].Name == propertyName)
                {
                    return true;
                }
            }

            return false;
        }
    }
}