using Orleans.Configuration;
using Orleans.Streaming.Redis.Storage;
using Orleans.Streams;
using Serilog;
using StackExchange.Redis;
using System;
using System.Collections.Generic;
using System.Threading.Tasks;

namespace Orleans.Providers.Streams.Redis
{
    class RedisAdapterReceiver : IQueueAdapterReceiver
    {
        private RedisDataManager _queue;
        private long _lastReadMessage;
        private Task _outstandingTask;
        private readonly ILogger _logger;
        private readonly IRedisDataAdapter _dataAdapter;

        public QueueId Id { get; }

        public static IQueueAdapterReceiver Create(ILogger logger, QueueId queueId, string serviceId, RedisStreamOptions options, IRedisDataAdapter dataAdapter)
        {
            if (queueId == null) throw new ArgumentNullException(nameof(queueId));
            if (string.IsNullOrEmpty(serviceId)) throw new ArgumentNullException(nameof(serviceId));
            if (options == null) throw new ArgumentNullException(nameof(options));
            if (dataAdapter == null) throw new ArgumentNullException(nameof(dataAdapter));

            var queue = new RedisDataManager(options, logger, queueId.ToString(), serviceId);
            return new RedisAdapterReceiver(logger, queueId, queue, dataAdapter);
        }

        private RedisAdapterReceiver(ILogger logger, QueueId queueId, RedisDataManager queue, IRedisDataAdapter dataAdapter)
        {
            if (queueId == null) throw new ArgumentNullException(nameof(queueId));
            if (queue == null) throw new ArgumentNullException(nameof(queue));
            if (dataAdapter == null) throw new ArgumentNullException(nameof(queue));

            Id = queueId;
            _queue = queue ?? throw new ArgumentNullException(nameof(queue));
            _dataAdapter = dataAdapter;
            _logger = logger.ForContext<RedisAdapterReceiver>();
        }

        public Task Initialize(TimeSpan timeout)
        {
            if (_queue != null) // check in case we already shut it down.
            {
                return _queue.InitAsync();
            }

            return Task.CompletedTask;
        }

        public async Task Shutdown(TimeSpan timeout)
        {
            try
            {
                // Await the last storage operation, so after we shutdown and stop this receiver we don't get async operation completions from pending storage operations.
                if (_outstandingTask != null)
                {
                    await _outstandingTask;
                }
            }
            finally
            {
                if (_queue != null)
                {
                    await _queue.StopAsync();
                }

                // Remember that we shut down so we never try to read from the queue again.
                _queue = null;
            }
        }

        public async Task<IList<IBatchContainer>> GetQueueMessagesAsync(int maxCount)
        {
            try
            {
                var queueRef = _queue; // store direct ref, in case we are somehow asked to shutdown while we are receiving.    
                if (queueRef == null) return new List<IBatchContainer>();

                int count = maxCount < 0 || maxCount == QueueAdapterConstants.UNLIMITED_GET_QUEUE_MSG ? RedisDataManager.UnlimitedMessageCount : maxCount;

                var task = queueRef.GetQueueMessagesAsync(count);
                _outstandingTask = task;
                IEnumerable<RedisValue> messages = await task;

                List<IBatchContainer> RedisMessages = new List<IBatchContainer>();
                foreach (var message in messages)
                {
                    IBatchContainer container = _dataAdapter.FromRedisValue(message, _lastReadMessage++);
                    RedisMessages.Add(container);
                }

                return RedisMessages;
            }
            finally
            {
                _outstandingTask = null;
            }
        }

        public Task MessagesDeliveredAsync(IList<IBatchContainer> messages)
        {
            // No op for now as redis really isn't a persistent queue :/
            foreach (var m in messages)
            {
                _logger.Verbose("{SequenceId} delivered", m.SequenceToken);
            }

            return Task.CompletedTask;
        }
    }
}
