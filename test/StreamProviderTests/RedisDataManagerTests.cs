using Moq;
using Orleans.Configuration;
using Orleans.Redis.Common;
using Orleans.Streaming.Redis.Storage;
using Shared;
using Shared.Mocking;
using StackExchange.Redis;
using System;
using System.Collections.Generic;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using Xunit;
using Xunit.Categories;

namespace StreamingTests
{
    [Category("BVT")]
    [Feature("Streaming")]
    public class RedisDataManagerTests
    {
        private static readonly string[] InvalidConcatenatedServiceQueueNames = new[]
        {
            "",
            "1",
            "12",
            new string(' ', 255),
        };
        private static readonly string[] InvalidServiceIds = new[]
        {
            ""
        };
        private const string ExpectedChannelName = TestConstants.ValidServiceId + ":" + TestConstants.ValidQueueName;

        [Fact]
        public void ConstructorThrowsOnInvalidQueueName()
        {
            // The public constructor always prepends the service ID so the
            // minimum length required for the queue name is MAX_KEY_LENGTH
            // minus the service ID length.
            foreach (var invalidQueueName in InvalidConcatenatedServiceQueueNames)
            {
                AssertEx.ThrowsAny<ArgumentException, RedisStreamOptions, string, string>(
                    // A non-0 length string is a valid service name
                    (arg1, arg2, arg3) => new RedisDataManager(TestConstants.ValidRedisStreamOptions, CachedConnectionMultiplexerFactory.Default, null, invalidQueueName, "-"),
                    TestConstants.ValidRedisStreamOptions, (string) null, invalidQueueName);
            }
        }

        [Fact]
        public async Task SubscribeAsyncSubscribesToRedisChannel()
        {
            var (connectionMultiplexer, subscriber, rdm) = MockRedisDataManager();

            subscriber
                .Setup(x => x.SubscribeAsync(ExpectedChannelName, It.IsAny<Action<RedisChannel, RedisValue>>(), It.IsAny<CommandFlags>()))
                .Returns(Task.CompletedTask);

            await rdm.InitAsync();
            await rdm.SubscribeAsync();

            subscriber.Verify(x => x.SubscribeAsync(ExpectedChannelName, It.IsAny<Action<RedisChannel, RedisValue>>(), It.IsAny<CommandFlags>()), Times.Once);
        }

        [Fact]
        public async Task InitAsyncRespectsCancellationTokenTimeout()
        {
            var (connectionMultiplexer, subscriber, rdm) = MockRedisDataManager();

            var ct = new CancellationToken(true);
            await AssertEx.ThrowsAnyAsync<AggregateException>(() => rdm.InitAsync(ct), e => e.InnerException is TaskCanceledException);
        }

        [Fact]
        public async Task SubscribeAsyncRespectsCancellationTokenTimeout()
        {
            var (connectionMultiplexer, subscriber, rdm) = MockRedisDataManager();

            await rdm.InitAsync();

            var ct = new CancellationToken(true);
            await AssertEx.ThrowsAnyAsync<AggregateException>(() => rdm.SubscribeAsync(ct), e => e.InnerException is TaskCanceledException);
        }

        [Fact]
        public async Task StopAsyncRespectsCancellationTokenTimeout()
        {
            var (connectionMultiplexer, subscriber, rdm) = MockRedisDataManager();

            await rdm.InitAsync();

            var ct = new CancellationToken(true);
            await AssertEx.ThrowsAnyAsync<AggregateException>(() => rdm.StopAsync(ct), e => e.InnerException is TaskCanceledException);
        }

        [Fact]
        public async Task StopAsyncWithoutInitAsyncThrowsInvalidOperationException()
        {
            var (connectionMultiplexer, subscriber, rdm) = MockRedisDataManager();

            await AssertEx.ThrowsAnyAsync<InvalidOperationException>(() => rdm.StopAsync());
        }

        [Fact]
        public async Task StopAsyncUnsubscribesFromRedisChannel()
        {
            var (connectionMultiplexer, subscriber, rdm) = MockRedisDataManager();

            subscriber
                .Setup(x => x.UnsubscribeAllAsync(It.IsAny<CommandFlags>()))
                .Returns(Task.CompletedTask);

            await rdm.InitAsync();
            await rdm.SubscribeAsync();
            await rdm.StopAsync();

            subscriber.Verify(x => x.UnsubscribeAllAsync(It.IsAny<CommandFlags>()), Times.Once);
        }

        [Fact]
        public async Task MessagesFromRedisReturnedByGetQueueMessagesAsync()
        {
            var (connectionMultiplexer, subscriber, rdm) = MockRedisDataManager();

            RedisChannel subscribedChannel = (string) null;
            Action<RedisChannel, RedisValue> subscribedHandler = null;
            subscriber
                .Setup(x => x.SubscribeAsync(ExpectedChannelName, It.IsAny<Action<RedisChannel, RedisValue>>(), It.IsAny<CommandFlags>()))
                .Callback((RedisChannel channel, Action<RedisChannel, RedisValue> handler, CommandFlags commandFlags) =>
                {
                    subscribedChannel = channel;
                    subscribedHandler = handler;
                })
                .Returns(Task.CompletedTask);

            subscriber
                .Setup(x => x.UnsubscribeAsync(ExpectedChannelName, It.IsAny<Action<RedisChannel, RedisValue>>(), It.IsAny<CommandFlags>()))
                .Returns(Task.CompletedTask);

            // Initialize
            await rdm.InitAsync();
            await rdm.SubscribeAsync();

            // Send some messages
            var expectedMessages = new List<RedisValue>();
            for (var i = 0; i < 100; i++)
            {
                subscribedHandler.Invoke(subscribedChannel, i);
                expectedMessages.Add(i);
            }

            AssertEx.Equal(expectedMessages, await rdm.GetQueueMessagesAsync(100));

            await rdm.StopAsync();
        }

        [Fact]
        public async Task PublishedMessagesCallToRedisPublishAsync()
        {
            var (connectionMultiplexer, subscriber, rdm) = MockRedisDataManager();

            var actualPublishedMessages = new List<byte[]>();
            subscriber
                .Setup(x => x.PublishAsync(ExpectedChannelName, It.IsAny<RedisValue>(), It.IsAny<CommandFlags>()))
                .Callback((RedisChannel channel, RedisValue value, CommandFlags flags) => actualPublishedMessages.Add(value))
                .ReturnsAsync(0);

            await rdm.InitAsync();
            await rdm.SubscribeAsync();

            var expectedPublishedMessages = new List<byte[]>();
            for (var i = 0; i < 100; i++)
            {
                var message = Encoding.UTF8.GetBytes(i.ToString());
                expectedPublishedMessages.Add(message);
                await rdm.AddQueueMessage(message);
            }

            AssertEx.Equal(expectedPublishedMessages, actualPublishedMessages);

            await rdm.StopAsync();
        }

        /// <summary>
        /// Tests to make sure queued messages can't grown unbounded if they're
        /// never culled.
        /// </summary>
        [Theory]
        [InlineData(10)]
        [InlineData(100)]
        [InlineData(1000)]
        [InlineData(10000)]
        public async Task QueuedMessagesHaveAnUpperBoundLimit(int queueCapacity)
        {
            var (connectionMultiplexer, subscriber, rdm) = MockRedisDataManager(redisStreamOptions: new RedisStreamOptions
            {
                ConnectionString = "",
                QueueCacheSize = queueCapacity
            });

            RedisChannel subscribedChannel = (string)null;
            Action<RedisChannel, RedisValue> subscribedHandler = null;
            subscriber
                .Setup(x => x.SubscribeAsync(ExpectedChannelName, It.IsAny<Action<RedisChannel, RedisValue>>(), It.IsAny<CommandFlags>()))
                .Callback((RedisChannel channel, Action<RedisChannel, RedisValue> handler, CommandFlags commandFlags) =>
                {
                    subscribedChannel = channel;
                    subscribedHandler = handler;
                })
                .Returns(Task.CompletedTask);

            await rdm.InitAsync();
            await rdm.SubscribeAsync();

            var message = new byte[0];

            for(var i = 0; i < queueCapacity + 1; i++)
            {
                subscribedHandler(subscribedChannel, message);
            }

            Assert.Equal(queueCapacity, rdm.TestHook_Queue.Count);
        }

        [Fact]
        public async Task UnsubscribedInstanceDoesntCallRedisSubscribeAsync()
        {
            var (connectionMultiplexer, subscriber, rdm) = MockRedisDataManager();

            subscriber
                .Setup(x => x.SubscribeAsync(ExpectedChannelName, It.IsAny<Action<RedisChannel, RedisValue>>(), It.IsAny<CommandFlags>()))
                .Returns(Task.CompletedTask);

            await rdm.InitAsync();
            await rdm.StopAsync();

            subscriber.Verify(x => x.SubscribeAsync(ExpectedChannelName, It.IsAny<Action<RedisChannel, RedisValue>>(), It.IsAny<CommandFlags>()), Times.Never());
        }

        private (Mock<IConnectionMultiplexer> MockConnectionMultiplexer, Mock<ISubscriber> MockSubscriber, RedisDataManager RedisDataManager) MockRedisDataManager(Mock<IConnectionMultiplexerFactory> connectionMultiplexerFactory = null, RedisStreamOptions redisStreamOptions = null)
        {
            var mockConnectionMultiplexer = new Mock <IConnectionMultiplexer> { DefaultValue = DefaultValue.Mock };
            var mockSubscriber = new Mock<ISubscriber> { DefaultValue = DefaultValue.Mock };
            mockConnectionMultiplexer
                .Setup(x => x.GetSubscriber(It.IsAny<object>()))
                .Returns(mockSubscriber.Object);

            var rdm = new RedisDataManager(
                redisStreamOptions ?? TestConstants.ValidRedisStreamOptions,
                connectionMultiplexerFactory?.Object ?? MockConnectionMultiplexerFactory.Returns(mockConnectionMultiplexer.Object),
                null,
                TestConstants.ValidQueueName,
                TestConstants.ValidServiceId);

            return (mockConnectionMultiplexer, mockSubscriber, rdm);
        }
    }
}
