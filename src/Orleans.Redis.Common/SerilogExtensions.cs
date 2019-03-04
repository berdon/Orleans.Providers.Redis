using System;
using System.Collections.Generic;
using System.Text;
using Serilog;
using Serilog.Core;
using Serilog.Events;

namespace Orleans.Redis.Common
{
    public static class SerilogExtensions
    {
        public static ILogger ForContext<T>(this ILogger logger, IDictionary<string, object> properties, bool destructureObjects = false)
        {
            return logger.ForContext<T>().ForContext(properties, destructureObjects);
        }

        public static ILogger ForContext(this ILogger logger, IDictionary<string, object> properties, bool destructureObjects = false)
        {
            if (properties == null || properties.Count == 0)
            {
                return logger;
            }

            return logger.ForContext(new DictionaryEventEnricher(properties, destructureObjects));
        }

        class DictionaryEventEnricher : ILogEventEnricher
        {
            readonly IDictionary<string, object> _properties;
            readonly bool _destructureObjects;

            public DictionaryEventEnricher(IDictionary<string, object> properties, bool destructureObjects)
            {
                _properties = properties;
                _destructureObjects = destructureObjects;
            }

            public void Enrich(LogEvent logEvent, ILogEventPropertyFactory propertyFactory)
            {
                foreach (var kvp in _properties)
                {
                    logEvent.AddOrUpdateProperty(propertyFactory.CreateProperty(kvp.Key, kvp.Value, _destructureObjects));
                }
            }
        }
    }
}
