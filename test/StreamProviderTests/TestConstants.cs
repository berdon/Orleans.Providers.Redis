using Orleans.Configuration;

namespace StreamingTests
{
    public class TestConstants
    {
        public const string ValidServiceId = "SomeServiceId";
        public const string ValidClusterId = "SomeClusterId";
        public const string ValidQueueName = "SomeValidQueueName";
        public const string ValidProviderName = "SomeValidProviderName";
        public static readonly RedisStreamOptions ValidRedisStreamOptions = new RedisStreamOptions
        {
            ConnectionString = "127.0.0.1"
        };
        public static readonly HashRingStreamQueueMapperOptions ValidHashRingStreamQueueMapperOptions = new HashRingStreamQueueMapperOptions();
        public static readonly SimpleQueueCacheOptions ValidSimpleQueueCacheOptions = new SimpleQueueCacheOptions();
    }
}
