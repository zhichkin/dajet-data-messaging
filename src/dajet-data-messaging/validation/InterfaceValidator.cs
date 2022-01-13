using DaJet.Metadata.Model;
using System;
using System.Collections.Generic;
using System.ComponentModel.DataAnnotations.Schema;
using System.Reflection;

namespace DaJet.Data.Messaging
{
    public sealed class InterfaceValidator
    {
        public bool InterfaceIsValid(in ApplicationObject queue, Type template, out List<string> errors)
        {
            errors = new List<string>();

            if (string.IsNullOrWhiteSpace(queue.TableName))
            {
                errors.Add($"The metadata object \"{queue.Name}\" does not have a database table defined.");
            }

            foreach (PropertyInfo info in template.GetProperties())
            {
                ColumnAttribute column = info.GetCustomAttribute<ColumnAttribute>();
                if (column == null)
                {
                    errors.Add($"The property \"{info.Name}\" does not have attribute Column applied.");
                    continue;
                }

                bool found = false;

                foreach (MetadataProperty property in queue.Properties)
                {
                    if (property.Fields.Count == 0)
                    {
                        errors.Add($"The property \"{property.Name}\" does not have a database field defined.");
                    }
                    else if (property.Fields.Count > 1)
                    {
                        errors.Add($"The property \"{property.Name}\" has too many database fields defined.");
                    }
                    else if (string.IsNullOrWhiteSpace(property.Fields[0].Name))
                    {
                        errors.Add($"The property \"{property.Name}\" has empty database field name.");
                    }

                    if (property.Name == column.Name)
                    {
                        found = true; break;
                    }
                }

                if (!found)
                {
                    errors.Add($"The property \"{info.Name}\" [{column.Name}] does not match database interface.");
                }
            }

            return (errors.Count == 0);
        }
    }
}