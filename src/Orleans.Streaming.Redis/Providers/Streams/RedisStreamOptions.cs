using Orleans.Redis.Common;
using Orleans.Runtime;
using System;

namespace Orleans.Configuration
{
    public class RedisStreamOptions : RedisOptions
    {
        public TimeSpan OperationTimeout { get; set; } = TimeSpan.FromSeconds(15);
    }

    public class RedisStreamOptionsValidator : IConfigurationValidator
    {
        private readonly RedisStreamOptions options;
        private readonly string name;

        public RedisStreamOptionsValidator(RedisStreamOptions options, string name)
        {
            this.options = options;
            this.name = name;
        }

        public void ValidateConfiguration()
        {
            if (String.IsNullOrEmpty(options.ConnectionString))
                throw new OrleansConfigurationException(
                    $"{nameof(RedisStreamOptions)} on stream provider {this.name} is invalid. {nameof(RedisStreamOptions.ConnectionString)} is invalid");
        }
    }
}