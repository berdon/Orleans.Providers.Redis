using Newtonsoft.Json;
using Newtonsoft.Json.Converters;
using System;
using System.Collections.Generic;
using System.Text;

namespace Orleans.Persistence.Redis.Helpers
{
    /// <summary>
    /// When serializing dates and deserializing dates offsets can change.
    /// Which then can causes issues with comparing.
    /// To avoid this change to UtcDateTime and grab the ticks.
    /// </summary>
    internal class CustomDateFormatConverter : IsoDateTimeConverter
    {
        public override void WriteJson(JsonWriter writer, object value, JsonSerializer serializer)
        {
            string ticks = "0";

            switch (value)
            {
                case DateTime t1:
                    {
                        ticks = t1.Ticks.ToString();
                        break;
                    }
                case DateTimeOffset t:
                    {
                        ticks = t.UtcDateTime.ToString();
                        break;
                    }
                default:
                    {
                        throw new Exception("Expected date object value.");
                    }
            }

            writer.WriteValue(ticks);
        }
    }
}
