using Newtonsoft.Json;
using System;
using System.Collections;
using System.Collections.Generic;
using System.Text;

namespace Orleans.Persistence.Redis.Helpers
{
    /// <summary>
    /// Since order is not guaranteed during serialization force dictionary to be sorted then write json.
    /// </summary>
    internal class UnorderedCollectionConverter : JsonConverter
    {
        public override bool CanConvert(Type objectType)
        {
            var isDict = objectType.IsConstructedGenericType && objectType.GetGenericTypeDefinition() == typeof(Dictionary<,>);
            return isDict;
        }

        public override bool CanRead => false;

        public override void WriteJson(JsonWriter writer, object value, JsonSerializer serializer)
        {
            var sortedDictionary = new SortedDictionary<object, object>();
            var dict = (IDictionary)value;

            foreach (var key in dict.Keys)
            {
                sortedDictionary.Add(key, dict[key]);
            }

            serializer.Serialize(writer, sortedDictionary);
        }

        public override object ReadJson(JsonReader reader, Type objectType, object existingValue, JsonSerializer serializer)
        {
            throw new NotImplementedException();
        }
    }
}
