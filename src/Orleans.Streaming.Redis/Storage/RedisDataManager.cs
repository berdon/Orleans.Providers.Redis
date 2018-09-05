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

namespace Orleans.Streaming.Redis.Storage
{
    internal class RedisDataManager
    {
        public const int UnlimitedMessageCount = -1;

        public string QueueName { get; private set; }

        private readonly RedisStreamOptions _options;
        private readonly ILogger _logger;

        private readonly ConcurrentQueue<RedisValue> _queue = new ConcurrentQueue<RedisValue>();
        private readonly RedisChannel _redisChannel;

        private static readonly IEnumerable<RedisValue> EmptyRedisValueEnumerable = Array.Empty<RedisValue>();

        private IConnectionMultiplexer _connectionMultiplexer;

        private RedisDataManager(RedisStreamOptions options, ILogger loggerFactory, string queueName)
        {
            queueName = SanitizeQueueName(queueName);
            ValidateQueueName(queueName);

            _options = options;
            _logger = loggerFactory.ForContext<RedisDataManager>();
            QueueName = queueName;

            _redisChannel = new RedisChannel(QueueName, RedisChannel.PatternMode.Literal);
        }

        public RedisDataManager(RedisStreamOptions options, ILogger loggerFactory, string queueName, string serviceId)
            : this(options, loggerFactory, serviceId + "-" + queueName) { }

        public async Task InitAsync()
        {
            var startTime = DateTimeOffset.UtcNow;

            try
            {
                _connectionMultiplexer = await ConnectionMultiplexer.ConnectAsync(_options.ConnectionString);

                var subscription = _connectionMultiplexer.GetSubscriber();
                await subscription.SubscribeAsync(_redisChannel, OnChannelReceivedData);
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

        public async Task StopAsync()
        {
            var startTime = DateTimeOffset.UtcNow;

            try
            {
                var subscription = _connectionMultiplexer.GetSubscriber();
                await subscription.UnsubscribeAsync(_redisChannel, OnChannelReceivedData);

                _connectionMultiplexer.Dispose();
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
            throw new AggregateException($"Error doing {operation} for Redis storage queue {QueueName}");
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
        }

        private static string SanitizeQueueName(string queueName)
        {
            var tmp = queueName;
            tmp = tmp.ToLowerInvariant();
            tmp = tmp
                .Replace('/', '-') // Forward slash
                .Replace('\\', '-') // Backslash
                .Replace('#', '-') // Pound sign
                .Replace('?', '-') // Question mark
                .Replace('&', '-')
                .Replace('+', '-')
                .Replace(':', '-')
                .Replace('.', '-')
                .Replace('%', '-');
            return tmp;
        }

        private void ValidateQueueName(string queueName)
        {
            if (!(queueName.Length >= 3 && queueName.Length <= 63))
            {
                // A queue name must be from 3 through 63 characters long.
                throw new ArgumentException(String.Format("A queue name must be from 3 through 63 characters long, while your queueName length is {0}, queueName is {1}.", queueName.Length, queueName), queueName);
            }

            if (!Char.IsLetterOrDigit(queueName.First()))
            {
                // A queue name must start with a letter or number
                throw new ArgumentException(String.Format("A queue name must start with a letter or number, while your queueName is {0}.", queueName), queueName);
            }

            if (!Char.IsLetterOrDigit(queueName.Last()))
            {
                // The first and last letters in the queue name must be alphanumeric. The dash (-) character cannot be the first or last character.
                throw new ArgumentException(String.Format("The last letter in the queue name must be alphanumeric, while your queueName is {0}.", queueName), queueName);
            }

            if (!queueName.All(c => Char.IsLetterOrDigit(c) || c.Equals('-')))
            {
                // A queue name can only contain letters, numbers, and the dash (-) character.
                throw new ArgumentException(String.Format("A queue name can only contain letters, numbers, and the dash (-) character, while your queueName is {0}.", queueName), queueName);
            }

            if (queueName.Contains("--"))
            {
                // Consecutive dash characters are not permitted in the queue name.
                throw new ArgumentException(String.Format("Consecutive dash characters are not permitted in the queue name, while your queueName is {0}.", queueName), queueName);
            }

            if (queueName.Where(Char.IsLetter).Any(c => !Char.IsLower(c)))
            {
                // All letters in a queue name must be lowercase.
                throw new ArgumentException(String.Format("All letters in a queue name must be lowercase, while your queueName is {0}.", queueName), queueName);
            }
        }
    }
}