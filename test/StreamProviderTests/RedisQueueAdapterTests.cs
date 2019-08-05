using Moq;
using Newtonsoft.Json;
using Orleans.Configuration;
using Orleans.Providers.Streams.Common;
using Orleans.Providers.Streams.Redis;
using Orleans.Redis.Common;
using Orleans.Streams;
using Serilog;
using Shared;
using Shared.Mocking;
using StackExchange.Redis;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Xunit;
using Xunit.Categories;

namespace StreamingTests
{
    [Category("BVT")]
    [Feature("Streaming")]
    public class RedisQueueAdapterTests
    {
        private static readonly Guid ValidStreamGuid = Guid.NewGuid();
        private const string ValidStreamNamespace = "SomeValidStreamNamespace";
        private const string ValidServiceId = "SomeValidServiceId";
        private const string ValidClusterId = "SomeValidClusterId";
        private const string ValidProviderName = "SomeValidProviderName";
        private static readonly RedisStreamOptions ValidRedisStreamOptions = new RedisStreamOptions
        {
            ConnectionString = "127.0.0.1"
        };

        [Fact]
        public void NullRedisStreamOptionsParamThrowsInConstructor() => AssertEx.ThrowsAny<ArgumentNullException>(
            () => new RedisQueueAdapter(null, null, null, null, null, null, null, null), e => e.ParamName == "options");

        [Fact]
        public void NullConnectionMultiplexerFactoryParamThrowsInConstructor() => AssertEx.ThrowsAny<ArgumentNullException>(
            () => new RedisQueueAdapter(ValidRedisStreamOptions, null, null, null, null, null, null, null), e => e.ParamName == "connectionMultiplexerFactory");

        [Fact]
        public void NullDataAdapterParamThrowsInConstructor() => AssertEx.ThrowsAny<ArgumentNullException>(
            () => new RedisQueueAdapter(ValidRedisStreamOptions, CachedConnectionMultiplexerFactory.Default, null, null, null, null, null, null), e => e.ParamName == "dataAdapter");

        [Fact]
        public void NullStreamQueueMapperParamThrowsInConstructor() => AssertEx.ThrowsAny<ArgumentNullException>(
            () => new RedisQueueAdapter(ValidRedisStreamOptions, CachedConnectionMultiplexerFactory.Default, Mock.Of<IRedisDataAdapter>(), null, null, null, null, null), e => e.ParamName == "streamQueueMapper");

        [Fact]
        public void NullServiceIdParamThrowsInConstructor() => AssertEx.ThrowsAny<ArgumentNullException>(
            () => new RedisQueueAdapter(ValidRedisStreamOptions, CachedConnectionMultiplexerFactory.Default, Mock.Of<IRedisDataAdapter>(), Mock.Of<IStreamQueueMapper>(), null, null, null, null), e => e.ParamName == "serviceId");
        
        [Fact]
        public void NullClusterIdParamThrowsInConstructor() => AssertEx.ThrowsAny<ArgumentNullException>(
            () => new RedisQueueAdapter(ValidRedisStreamOptions, CachedConnectionMultiplexerFactory.Default, Mock.Of<IRedisDataAdapter>(), Mock.Of<IStreamQueueMapper>(), null, ValidServiceId, null, null), e => e.ParamName == "clusterId");

        [Fact]
        public void NullProviderNameParamThrowsInConstructor() => AssertEx.ThrowsAny<ArgumentNullException>(
            () => new RedisQueueAdapter(ValidRedisStreamOptions, CachedConnectionMultiplexerFactory.Default, Mock.Of<IRedisDataAdapter>(), Mock.Of<IStreamQueueMapper>(), null, ValidServiceId, ValidClusterId, null), e => e.ParamName == "providerName");

        [Fact]
        public async Task PassingNonNullTokenThrownsArgumentException()
        {
            var rqa = new RedisQueueAdapter(ValidRedisStreamOptions, CachedConnectionMultiplexerFactory.Default, Mock.Of<IRedisDataAdapter>(), Mock.Of<IStreamQueueMapper>(), MockLogger(), ValidServiceId, ValidClusterId, ValidProviderName);

            await AssertEx.ThrowsAnyAsync<ArgumentException>(
                () => rqa.QueueMessageBatchAsync(ValidStreamGuid, ValidStreamNamespace, new[] { "Something" }, new EventSequenceTokenV2(0), null),
                e => e.ParamName == "token");
        }

        [Fact]
        public async Task QueueMessageBatchAsyncWithSingleEventPublishesToRedisChannel()
        {
            var logger = new Mock<ILogger>() { DefaultValue = DefaultValue.Mock };
            var mockConnectionMultiplexer = new Mock<IConnectionMultiplexer> { DefaultValue = DefaultValue.Mock };
            var mockSubscriber = new Mock<ISubscriber> { DefaultValue = DefaultValue.Mock };
            mockConnectionMultiplexer.Setup(x => x.GetSubscriber(It.IsAny<object>())).Returns(mockSubscriber.Object);

            var actualMessages = new List<RedisValue>();
            mockSubscriber
                .Setup(x => x.PublishAsync(It.IsAny<RedisChannel>(), It.IsAny<RedisValue>(), It.IsAny<CommandFlags>()))
                .Callback((RedisChannel channel, RedisValue redisValue, CommandFlags commandFlags) => actualMessages.Add(redisValue))
                .ReturnsAsync(0);

            var mockRedisDataAdapter = new Mock<IRedisDataAdapter> { DefaultValue = DefaultValue.Mock };
            mockRedisDataAdapter
                .Setup(x => x.ToRedisValue<string>(ValidStreamGuid, ValidStreamNamespace, It.IsAny<IEnumerable<string>>(), null))
                .Returns((Guid streamGuid, string streamNamespace, IEnumerable<string> events, Dictionary<string, object> requestContext) => JsonConvert.SerializeObject(events));

            var mockQueueStreamMapper = new Mock<IStreamQueueMapper> { DefaultValue = DefaultValue.Mock };
            mockQueueStreamMapper
                .Setup(x => x.GetQueueForStream(ValidStreamGuid, ValidStreamNamespace))
                .Returns(QueueId.GetQueueId(null, 0, 0));

            var rqa = new RedisQueueAdapter(
                ValidRedisStreamOptions,
                MockConnectionMultiplexerFactory.Returns(mockConnectionMultiplexer.Object),
                mockRedisDataAdapter.Object,
                mockQueueStreamMapper.Object,
                logger.Object,
                ValidServiceId,
                ValidClusterId,
                ValidProviderName);

            var expectedMessages = new List<RedisValue>();
            for (var i = 0; i < 100; i++)
            {
                var message = new[] { i.ToString() };
                expectedMessages.Add(JsonConvert.SerializeObject(message));
                await rqa.QueueMessageBatchAsync(ValidStreamGuid, ValidStreamNamespace, message, null, null);
            }

            AssertEx.Equal(expectedMessages, actualMessages);
        }

        [Fact]
        public async Task QueueMessageBatchAsyncWithManyEventsPublishesToRedisChannel()
        {
            var logger = new Mock<ILogger>() { DefaultValue = DefaultValue.Mock };
            var mockConnectionMultiplexer = new Mock<IConnectionMultiplexer> { DefaultValue = DefaultValue.Mock };
            var mockSubscriber = new Mock<ISubscriber> { DefaultValue = DefaultValue.Mock };
            mockConnectionMultiplexer.Setup(x => x.GetSubscriber(It.IsAny<object>())).Returns(mockSubscriber.Object);

            var actualMessages = new List<RedisValue>();
            mockSubscriber
                .Setup(x => x.PublishAsync(It.IsAny<RedisChannel>(), It.IsAny<RedisValue>(), It.IsAny<CommandFlags>()))
                .Callback((RedisChannel channel, RedisValue redisValue, CommandFlags commandFlags) => actualMessages.Add(redisValue))
                .ReturnsAsync(0);

            var mockRedisDataAdapter = new Mock<IRedisDataAdapter> { DefaultValue = DefaultValue.Mock };
            mockRedisDataAdapter
                .Setup(x => x.ToRedisValue<string>(ValidStreamGuid, ValidStreamNamespace, It.IsAny<IEnumerable<string>>(), null))
                .Returns((Guid streamGuid, string streamNamespace, IEnumerable<string> events, Dictionary<string, object> requestContext) => JsonConvert.SerializeObject(events));

            var mockQueueStreamMapper = new Mock<IStreamQueueMapper> { DefaultValue = DefaultValue.Mock };
            mockQueueStreamMapper
                .Setup(x => x.GetQueueForStream(ValidStreamGuid, ValidStreamNamespace))
                .Returns(QueueId.GetQueueId(null, 0, 0));

            var rqa = new RedisQueueAdapter(
                ValidRedisStreamOptions,
                MockConnectionMultiplexerFactory.Returns(mockConnectionMultiplexer.Object),
                mockRedisDataAdapter.Object,
                mockQueueStreamMapper.Object,
                logger.Object,
                ValidServiceId,
                ValidClusterId,
                ValidProviderName);

            var expectedMessages = new List<RedisValue>();
            for (var i = 0; i < 100; i++)
            {
                var message = new string[i].Select((s, j) => j.ToString());
                expectedMessages.Add(JsonConvert.SerializeObject(message));
                await rqa.QueueMessageBatchAsync(ValidStreamGuid, ValidStreamNamespace, message, null, null);
            }

            AssertEx.Equal(expectedMessages, actualMessages);
        }

        private ILogger MockLogger() => (new Mock<ILogger> { DefaultValue = DefaultValue.Mock }).Object;
    }
}
