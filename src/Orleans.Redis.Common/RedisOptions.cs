using System;
using System.Collections.Generic;
using System.Text;

namespace Orleans.Configuration
{
    public class RedisOptions
    {
        public string ConnectionString { get; set; }
        public int SyncTimeout { get; set; } = 5000;
        public string ToConfigString()
        {
            return $"{ConnectionString},syncTimeout={SyncTimeout}";
        }
    }
}
