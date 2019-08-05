using Orleans.Configuration;
using Orleans.Redis.Common;
using Orleans.Streaming.Redis.Storage;
using Orleans.Streams;
using Serilog;
using StackExchange.Redis;
using System;
using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;

namespace Orleans.Providers.Streams.Redis
{
    class RedisQueueAdapterReceiver : IQueueAdapterReceiver
    {
        private IRedisDataManager _queue;
        private long _lastReadMessage;
        private Task _outstandingTask;
        private readonly ILogger _logger;
        private readonly IRedisDataAdapter _dataAdapter;

        public QueueId Id { get; }

        internal IRedisDataManager TestHook_Queue
        {
            get => _queue;
            set => _queue = value;
        }

        public static IQueueAdapterReceiver Create(
            ILogger logger,
            QueueId queueId,
            string serviceId,
            string clusterId,
            RedisStreamOptions options,
            IConnectionMultiplexerFactory connectionMultiplexerFactory,
            IRedisDataAdapter dataAdapter)
        {
            if (queueId == null) throw new ArgumentNullException(nameof(queueId));
            if (string.IsNullOrEmpty(clusterId)) throw new ArgumentNullException(nameof(clusterId));
            if (options == null) throw new ArgumentNullException(nameof(options));
            if (connectionMultiplexerFactory == null) throw new ArgumentNullException(nameof(connectionMultiplexerFactory));
            if (dataAdapter == null) throw new ArgumentNullException(nameof(dataAdapter));

            var queue = new RedisDataManager(options, connectionMultiplexerFactory, logger, queueId.ToString(), serviceId, clusterId);
            return new RedisQueueAdapterReceiver(logger, queueId, queue, dataAdapter);
        }

        private RedisQueueAdapterReceiver(ILogger logger, QueueId queueId, RedisDataManager queue, IRedisDataAdapter dataAdapter)
        {
            if (queueId == null) throw new ArgumentNullException(nameof(queueId));
            if (queue == null) throw new ArgumentNullException(nameof(queue));
            if (dataAdapter == null) throw new ArgumentNullException(nameof(queue));

            Id = queueId;
            _queue = queue ?? throw new ArgumentNullException(nameof(queue));
            _dataAdapter = dataAdapter;
            _logger = logger.ForContext<RedisQueueAdapterReceiver>();
        }

        public async Task Initialize(TimeSpan timeout)
        {
            using (var cts = new CancellationTokenSource(timeout))
            {
                cts.Token.ThrowIfCancellationRequested();

                if (_queue != null) // check in case we already shut it down.
                {
                    await _queue.InitAsync(cts.Token);

                    cts.Token.ThrowIfCancellationRequested();

                    await _queue.SubscribeAsync(cts.Token);

                    cts.Token.ThrowIfCancellationRequested();
                }
            }
        }

        public async Task Shutdown(TimeSpan timeout)
        {
            using (var cts = new CancellationTokenSource(timeout))
            {
                cts.Token.ThrowIfCancellationRequested();

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
                    cts.Token.ThrowIfCancellationRequested();

                    if (_queue != null)
                    {
                        await _queue.UnsubscribeAsync(cts.Token);
                    }

                    // Remember that we shut down so we never try to read from the queue again.
                    _queue = null;
                }

                cts.Token.ThrowIfCancellationRequested();
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
            if (_logger.IsEnabled(Serilog.Events.LogEventLevel.Verbose))
            {
                foreach (var m in messages)
                {
                    _logger.Verbose("{SequenceId} delivered", m.SequenceToken);
                }
            }

            return Task.CompletedTask;
        }
    }
}
