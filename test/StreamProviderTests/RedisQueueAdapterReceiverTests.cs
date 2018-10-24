using Moq;
using Orleans.Providers.Streams.Redis;
using Orleans.Redis.Common;
using Orleans.Streaming.Redis.Storage;
using Orleans.Streams;
using Serilog;
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
    public class RedisQueueAdapterReceiverTests
    {
        [Fact]
        public async Task InitializeRespectsTimeout()
        {
            var logger = new Mock<ILogger>() { DefaultValue = DefaultValue.Mock };
            var dataAdapter = new Mock<IRedisDataAdapter> { DefaultValue = DefaultValue.Mock };
            var queueId = QueueId.GetQueueId(TestConstants.ValidQueueName, 0, 0);

            var rqar = (RedisQueueAdapterReceiver)RedisQueueAdapterReceiver.Create(
                logger.Object,
                queueId,
                TestConstants.ValidServiceId,
                TestConstants.ValidRedisStreamOptions,
                CachedConnectionMultiplexerFactory.Default,
                dataAdapter.Object);

            var rdm = new Mock<IRedisDataManager> { DefaultValue = DefaultValue.Mock };
            rqar.TestHook_Queue = rdm.Object;

            rdm
                .Setup(x => x.InitAsync(It.IsAny<CancellationToken>()))
                .Callback((CancellationToken ct) => Thread.Sleep(100))
                .Returns(Task.CompletedTask);

            await AssertEx.ThrowsAnyAsync<OperationCanceledException>(() => rqar.Initialize(TimeSpan.FromMilliseconds(0)));
        }

        [Fact]
        public async Task ShutdownRespectsTimeout()
        {
            var logger = new Mock<ILogger>() { DefaultValue = DefaultValue.Mock };
            var dataAdapter = new Mock<IRedisDataAdapter> { DefaultValue = DefaultValue.Mock };
            var queueId = QueueId.GetQueueId(TestConstants.ValidQueueName, 0, 0);

            var rqar = (RedisQueueAdapterReceiver) RedisQueueAdapterReceiver.Create(
                logger.Object,
                queueId,
                TestConstants.ValidServiceId,
                TestConstants.ValidRedisStreamOptions,
                CachedConnectionMultiplexerFactory.Default,
                dataAdapter.Object);

            var rdm = new Mock<IRedisDataManager> { DefaultValue = DefaultValue.Mock };
            rqar.TestHook_Queue = rdm.Object;

            rdm
                .Setup(x => x.UnsubscribeAsync(It.IsAny<CancellationToken>()))
                .Callback((CancellationToken ct) => Thread.Sleep(100))
                .Returns(Task.CompletedTask);

            await AssertEx.ThrowsAnyAsync<OperationCanceledException>(() => rqar.Shutdown(TimeSpan.FromMilliseconds(0)));
        }

        [Fact(Skip = "Incomplete")]
        public Task GetQueueMessagesAsyncReturnsPublishedMessages()
        {
            return Task.CompletedTask;
        }
    }
}
