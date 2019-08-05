using Orleans.Configuration;
using Orleans.Redis.Common;
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
        private readonly string ServiceId;
        private readonly string ClusterId;
        private readonly ConcurrentDictionary<QueueId, RedisDataManager> Queues = new ConcurrentDictionary<QueueId, RedisDataManager>();

        private readonly RedisStreamOptions _redisStreamOptions;
        private readonly IConnectionMultiplexerFactory _connectionMultiplexerFactory;
        private readonly IStreamQueueMapper _streamQueueMapper;
        private readonly ILogger _logger;
        private readonly IRedisDataAdapter _dataAdapter;

        public string Name { get; }
        public bool IsRewindable => false;

        public StreamProviderDirection Direction => StreamProviderDirection.ReadWrite;

        public RedisQueueAdapter(
            RedisStreamOptions options,
            IConnectionMultiplexerFactory connectionMultiplexerFactory,
            IRedisDataAdapter dataAdapter,
            IStreamQueueMapper streamQueueMapper,
            ILogger logger,
            string serviceId,
            string clusterId,
            string providerName)
        {
            if (options == null) throw new ArgumentNullException(nameof(options));
            if (connectionMultiplexerFactory == null) throw new ArgumentNullException(nameof(connectionMultiplexerFactory));
            if (dataAdapter == null) throw new ArgumentNullException(nameof(dataAdapter));
            if (streamQueueMapper == null) throw new ArgumentNullException(nameof(streamQueueMapper));
            if (string.IsNullOrEmpty(serviceId)) throw new ArgumentNullException(nameof(serviceId));
            if (string.IsNullOrEmpty(clusterId)) throw new ArgumentNullException(nameof(clusterId));
            if (string.IsNullOrEmpty(providerName)) throw new ArgumentNullException(nameof(providerName));

            _redisStreamOptions = options;
            _connectionMultiplexerFactory = connectionMultiplexerFactory;
            ServiceId = serviceId;
            ClusterId = clusterId;
            Name = providerName;
            _streamQueueMapper = streamQueueMapper;
            _dataAdapter = dataAdapter;
            _logger = logger.ForContext<RedisQueueAdapter>(new Dictionary<string, object>
            {
                { "ServiceId", serviceId },
                { "ProviderName", providerName }
            });
        }

        public IQueueAdapterReceiver CreateReceiver(QueueId queueId)
        {
            return RedisQueueAdapterReceiver.Create(
                _logger,
                queueId,
                ServiceId,
                ClusterId,
                _redisStreamOptions,
                _connectionMultiplexerFactory,
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
                _logger.Debug("Creating RedisDataManager {QueueId} for {StreamNamespace}://{StreamId}", queueId.ToString(), streamNamespace, streamGuid);

                var tmpQueue = new RedisDataManager(_redisStreamOptions, _connectionMultiplexerFactory, _logger, queueId.ToString(), ServiceId, ClusterId);
                await tmpQueue.InitAsync();
                queue = Queues.GetOrAdd(queueId, tmpQueue);
            }

            var redisMessage = _dataAdapter.ToRedisValue(streamGuid, streamNamespace, events, requestContext);
            await queue.AddQueueMessage(redisMessage);
        }
    }
}
