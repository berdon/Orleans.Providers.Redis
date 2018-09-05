using Orleans.Configuration;
using Orleans.Streaming.Redis.Storage;
using Orleans.Streams;
using Serilog;
using StackExchange.Redis;
using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Text;
using System.Threading.Tasks;

namespace Orleans.Providers.Streams.Redis
{
    internal class RedisQueueAdapter : IQueueAdapter
    {
        protected readonly string ServiceId;
        protected readonly ConcurrentDictionary<QueueId, RedisDataManager> Queues = new ConcurrentDictionary<QueueId, RedisDataManager>();

        private readonly RedisStreamOptions _redisStreamOptions;
        private readonly HashRingBasedStreamQueueMapper _streamQueueMapper;
        private readonly ILogger _logger;
        private readonly IRedisDataAdapter _dataAdapter;

        public string Name { get; }
        public bool IsRewindable => false;

        public StreamProviderDirection Direction => StreamProviderDirection.ReadWrite;

        public RedisQueueAdapter(
            RedisStreamOptions options,
            IRedisDataAdapter dataAdapter,
            HashRingBasedStreamQueueMapper streamQueueMapper,
            ILogger logger,
            string serviceId,
            string providerName)
        {
            if (options == null) throw new ArgumentNullException(nameof(options));
            if (string.IsNullOrEmpty(serviceId)) throw new ArgumentNullException(nameof(serviceId));

            _redisStreamOptions = options;
            ServiceId = serviceId;
            Name = providerName;
            _streamQueueMapper = streamQueueMapper;
            _dataAdapter = dataAdapter;
            _logger = logger;
        }

        public IQueueAdapterReceiver CreateReceiver(QueueId queueId)
        {
            return RedisAdapterReceiver.Create(
                _logger,
                queueId,
                ServiceId,
                _redisStreamOptions,
                _dataAdapter);
        }

        public async Task QueueMessageBatchAsync<T>(Guid streamGuid, string streamNamespace, IEnumerable<T> events, StreamSequenceToken token, Dictionary<string, object> requestContext)
        {
            if (token != null)
            {
                throw new ArgumentException("Redis stream provider currently does not support non-null StreamSequenceToken.", nameof(token));
            }

            var queueId = _streamQueueMapper.GetQueueForStream(streamGuid, streamNamespace);

            if (!Queues.TryGetValue(queueId, out var queue))
            {
                var tmpQueue = new RedisDataManager(_redisStreamOptions, _logger, queueId.ToString(), ServiceId);
                await tmpQueue.InitAsync();
                queue = Queues.GetOrAdd(queueId, tmpQueue);
            }

            var redisMessage = _dataAdapter.ToRedisValue(streamGuid, streamNamespace, events, requestContext);
            await queue.AddQueueMessage(redisMessage);
        }
    }
}
