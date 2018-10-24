using StackExchange.Redis;
using Serilog;
using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Runtime.CompilerServices;
using System.Threading.Tasks;
using System.Linq;
using Orleans.Providers.Streams.Redis;
using Orleans.Configuration;
using Serilog.Core;
using Orleans.Redis.Common;
using System.Threading;

namespace Orleans.Streaming.Redis.Storage
{
    internal class RedisDataManager : IRedisDataManager
    {
        private const int MINIMUM_KEY_LENGTH = 5;
        private const int MAXIMUM_KEY_LENGTH = 256;

        public const int UnlimitedMessageCount = -1;

        public string QueueName { get; private set; }

        private readonly RedisStreamOptions _options;
        private readonly IConnectionMultiplexerFactory _connectionMultiplexerFactory;
        private readonly ILogger _logger;

        private readonly ConcurrentQueue<RedisValue> _queue = new ConcurrentQueue<RedisValue>();
        private readonly RedisChannel _redisChannel;

        private static readonly IEnumerable<RedisValue> EmptyRedisValueEnumerable = Array.Empty<RedisValue>();

        private IConnectionMultiplexer _connectionMultiplexer;

        internal ConcurrentQueue<RedisValue> TestHook_Queue => _queue;

        private RedisDataManager(RedisStreamOptions options, IConnectionMultiplexerFactory connectionMultiplexerFactory, ILogger logger, string queueName)
        {
            queueName = SanitizeQueueName(queueName);
            ValidateQueueName(queueName);

            _options = options;
            _connectionMultiplexerFactory = connectionMultiplexerFactory;
            _logger = logger.ForContext<RedisDataManager>();
            QueueName = queueName;

            _redisChannel = new RedisChannel(QueueName, RedisChannel.PatternMode.Literal);
        }

        public RedisDataManager(RedisStreamOptions options, IConnectionMultiplexerFactory connectionMultiplexerFactory, ILogger loggerFactory, string queueName, string serviceId)
            : this(options, connectionMultiplexerFactory, loggerFactory, serviceId + ":" + queueName) { }

        public async Task InitAsync(CancellationToken ct = default)
        {
            var startTime = DateTimeOffset.UtcNow;

            try
            {
                if (ct.IsCancellationRequested) throw new TaskCanceledException();

                _connectionMultiplexer = await _connectionMultiplexerFactory.CreateAsync(_options.ConnectionString);

                if (ct.IsCancellationRequested) throw new TaskCanceledException();
            }
            catch (Exception exc)
            {
                ReportErrorAndRethrow(exc);
            }
            finally
            {
                CheckAlertSlowAccess(startTime);
            }
        }

        public async Task SubscribeAsync(CancellationToken ct = default)
        {
            var startTime = DateTimeOffset.UtcNow;

            try
            {
                if (ct.IsCancellationRequested) throw new TaskCanceledException();

                var subscription = _connectionMultiplexer.GetSubscriber();
                await subscription.SubscribeAsync(_redisChannel, OnChannelReceivedData);

                if (ct.IsCancellationRequested) throw new TaskCanceledException();
            }
            catch (Exception exc)
            {
                ReportErrorAndRethrow(exc);
            }
            finally
            {
                CheckAlertSlowAccess(startTime);
            }
        }

        public async Task UnsubscribeAsync(CancellationToken ct = default)
        {
            var startTime = DateTimeOffset.UtcNow;

            try
            {
                if (ct.IsCancellationRequested) throw new TaskCanceledException();

                var subscription = _connectionMultiplexer.GetSubscriber();
                await subscription.UnsubscribeAsync(_redisChannel, OnChannelReceivedData);

                if (ct.IsCancellationRequested) throw new TaskCanceledException();
            }
            catch (Exception exc)
            {
                ReportErrorAndRethrow(exc);
            }
            finally
            {
                CheckAlertSlowAccess(startTime);
            }
        }

        public Task<IEnumerable<RedisValue>> GetQueueMessagesAsync(int count)
        {
            var startTime = DateTimeOffset.UtcNow;

            try
            {
                if (_queue.Count <= 0) return Task.FromResult(EmptyRedisValueEnumerable);

                if (count < 0 || count == UnlimitedMessageCount)
                {
                    count = _queue.Count;
                }

                var items = new List<RedisValue>();
                while(items.Count < count && _queue.TryDequeue(out var item))
                {
                    items.Add(item);
                }

                return Task.FromResult(items.AsEnumerable());
            }
            catch (Exception exc)
            {
                ReportErrorAndRethrow(exc);
                throw;  // Can't happen
            }
            finally
            {
                CheckAlertSlowAccess(startTime);
            }
        }

        public async Task AddQueueMessage(byte[] payload)
        {
            var startTime = DateTimeOffset.UtcNow;

            try
            {
                var subscription = _connectionMultiplexer.GetSubscriber();
                await subscription.PublishAsync(_redisChannel, payload);
            }
            catch (Exception exc)
            {
                ReportErrorAndRethrow(exc);
            }
            finally
            {
                CheckAlertSlowAccess(startTime);
            }
        }

        public Task DeleteQueueMessage(RedisValue value)
        {
            // No-op
            return Task.CompletedTask;
        }

        private void ReportErrorAndRethrow(Exception exc, [CallerMemberName] string operation = null)
        {
            _logger.Error(exc, "Error doing {Operation} for Redis storage queue {QueueName}", operation, QueueName);
            throw new AggregateException($"Error doing {operation} for Redis storage queue {QueueName}", exc);
        }

        private void CheckAlertSlowAccess(DateTimeOffset startOperation, [CallerMemberName] string operation = null)
        {
            var timeSpan = DateTime.UtcNow - startOperation;
            if (timeSpan > _options.OperationTimeout)
            {
                _logger.Warning("Slow access to Redis queue {QueueName} for {Operation}, which took {Time}", QueueName, operation, timeSpan);
            }
        }
        private void OnChannelReceivedData(RedisChannel channel, RedisValue value)
        {
            _queue.Enqueue(value);

            if (_logger.IsEnabled(Serilog.Events.LogEventLevel.Verbose))
            {
                _logger.Verbose("{Queue} has {Count} messages", QueueName, _queue.Count);
            }

            // Dequeue until we're below our queue cache limit
            int droppedCount = 0;
            while (_queue.Count > _options.QueueCacheSize && _queue.TryDequeue(out var _))
            {
                droppedCount++;
            }

            if (droppedCount > 0)
            {
                _logger.Warning("Dropped {Count} messages on the floor due to overflowing cache size", droppedCount);
            }
        }

        private static string SanitizeQueueName(string queueName)
        {
            // Nothing for now
            return queueName;
        }

        /// <summary>
        /// https://redis.io/topics/data-types-intro
        /// Redis keys are binary safe, this means that you can use any binary
        /// sequence as a key, from a string like "foo" to the content of a JPEG
        /// file. The empty string is also a valid key.
        /// 
        /// A few other rules about keys:
        /// Very long keys are not a good idea.For instance a key of 1024 bytes
        /// is a bad idea not only memory-wise, but also because the lookup of
        /// the key in the dataset may require several costly key-comparisons.
        /// Even when the task at hand is to match the existence of a large value,
        /// hashing it(for example with SHA1) is a better idea, especially from
        /// the perspective of memory and bandwidth.
        /// 
        /// Very short keys are often not a good idea. There is little point in
        /// writing "u1000flw" as a key if you can instead write
        /// "user:1000:followers". The latter is more readable and the added
        /// space is minor compared to the space used by the key object itself
        /// and the value object. While short keys will obviously consume a bit
        /// less memory, your job is to find the right balance.
        /// 
        /// Try to stick with a schema. For instance "object-type:id" is a good
        /// idea, as in "user:1000". Dots or dashes are often used for multi-word
        /// fields, as in "comment:1234:reply.to" or "comment:1234:reply-to".
        /// 
        /// The maximum allowed key size is 512 MB.
        /// </summary>
        /// <param name="queueName"></param>
        private void ValidateQueueName(string queueName)
        {
            if (!(queueName.Length >= MINIMUM_KEY_LENGTH && queueName.Length <= MAXIMUM_KEY_LENGTH))
            {
                
                throw new ArgumentException("A queue name must be from {MINIMUM_KEY_LENGTH} through {MAXIMUM_KEY_LENGTH} characters long, while your queueName length is {queueName.Length}, queueName is {queueName}.", queueName);
            }
        }
    }
}