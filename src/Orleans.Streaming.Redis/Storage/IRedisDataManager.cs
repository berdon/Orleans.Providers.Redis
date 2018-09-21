using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;
using StackExchange.Redis;

namespace Orleans.Streaming.Redis.Storage
{
    internal interface IRedisDataManager
    {
        string QueueName { get; }

        Task AddQueueMessage(byte[] payload);
        Task DeleteQueueMessage(RedisValue value);
        Task<IEnumerable<RedisValue>> GetQueueMessagesAsync(int count);
        Task InitAsync(CancellationToken ct = default);
        Task StopAsync(CancellationToken ct = default);
        Task SubscribeAsync(CancellationToken ct = default);
    }
}