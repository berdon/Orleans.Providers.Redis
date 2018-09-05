using Orleans.Hosting;
using Serilog;
using System;
using System.Threading.Tasks;
using Xunit;
using Orleans.Configuration;
using Microsoft.Extensions.DependencyInjection;
using System.Linq;
using Microsoft.Extensions.DependencyInjection.Extensions;
using Orleans.Runtime;
using Orleans.Providers;
using Orleans.Storage;
using StackExchange.Redis;
using SharedOrleansUtils;
using Nito.AsyncEx;
using System.Threading;

namespace CoreTests.Integration
{
    [Trait("Category", "BVT")]
    [Trait("Target", "Integration")]
    [Trait("Target", "Orleans.Redis.Streaming")]
    public class OrleansTests
    {
        private const string StreamProviderName = nameof(StreamProviderName);
        private const string StreamStorageName = "PubSubStore";
        private const int DefaultBlockingTimeoutInMs = 5000;
        private static CancellationToken GetDefaultBlockingToken() => new CancellationTokenSource(DefaultBlockingTimeoutInMs).Token;

        [Fact]
        public Task RedisStreamCanSendAndReceiveItem()
        {
            var clusterFixture = new StreamingClusterFixture();

            return clusterFixture.Dispatch(async () =>
            {
                var streamId = Guid.NewGuid();
                var streamNamespace = Guid.NewGuid().ToString();

                var streamSubscriptionAwaiter = await clusterFixture.SubscribeAndGetTaskAwaiter<string>(StreamProviderName, streamId, streamNamespace, 1);
                await clusterFixture.PublishToStream(StreamProviderName, streamId, streamNamespace, "test");
                await streamSubscriptionAwaiter.WaitAsync(GetDefaultBlockingToken());
            });
        }

        [Theory]
        [InlineData(1, 1)]
        [InlineData(100, 100)]
        public Task TwoRedisStreamsWithDifferentStreamIdsOnlyReceiveTheirOwnMessages(int messageCount1, int messageCount2)
        {
            var clusterFixture = new StreamingClusterFixture();

            return clusterFixture.Dispatch(async () =>
            {
                var streamId1 = Guid.NewGuid();
                var streamId2 = Guid.NewGuid();

                var streamNamespace = Guid.NewGuid().ToString();

                var streamSubscriptionAwaiter1 = await clusterFixture.SubscribeAndGetTaskAwaiter<string>(StreamProviderName, streamId1, streamNamespace, messageCount1);
                var streamSubscriptionAwaiter2 = await clusterFixture.SubscribeAndGetTaskAwaiter<string>(StreamProviderName, streamId2, streamNamespace, messageCount2);

                var publishTask1 = Task.Factory.StartNew(async () =>
                {
                    for (var i = 0; i < messageCount1; i++)
                    {
                        await clusterFixture.PublishToStream(StreamProviderName, streamId1, streamNamespace, $"test:{streamId1}-{streamNamespace} message:{i}");
                    }
                });
                var publishTask2 = Task.Factory.StartNew(async () =>
                {
                    for (var i = 0; i < messageCount2; i++)
                    {
                        await clusterFixture.PublishToStream(StreamProviderName, streamId2, streamNamespace, $"test:{streamId2}-{streamNamespace} message:{i}");
                    }
                });

                var items1 = await streamSubscriptionAwaiter1.WaitAsync(GetDefaultBlockingToken());
                var items2 = await streamSubscriptionAwaiter2.WaitAsync(GetDefaultBlockingToken());

                // Wait a little longer just in case something else is published (which would be bad)
                await Task.Delay(100);

                Assert.Equal(messageCount1, items1.Count);
                Assert.Equal(messageCount2, items2.Count);

                Assert.Equal(new object[messageCount1].Select((_, i) => $"test:{streamId1}-{streamNamespace} message:{i}"), items1.Cast<string>());
                Assert.Equal(new object[messageCount2].Select((_, i) => $"test:{streamId2}-{streamNamespace} message:{i}"), items2.Cast<string>());
            });
        }

        [Theory]
        [InlineData(1, 1)]
        [InlineData(100, 100)]
        public Task TwoRedisStreamsWithSameStreamIdsAndDifferentStreamNamespacesOnlyReceiveTheirOwnMessages(int messageCount1, int messageCount2)
        {
            var clusterFixture = new StreamingClusterFixture();

            return clusterFixture.Dispatch(async () =>
            {
                var streamId = Guid.NewGuid();

                var streamNamespace1 = Guid.NewGuid().ToString();
                var streamNamespace2 = Guid.NewGuid().ToString();

                var streamSubscriptionAwaiter1 = await clusterFixture.SubscribeAndGetTaskAwaiter<string>(StreamProviderName, streamId, streamNamespace1, messageCount1);
                var streamSubscriptionAwaiter2 = await clusterFixture.SubscribeAndGetTaskAwaiter<string>(StreamProviderName, streamId, streamNamespace2, messageCount2);

                var publishTask1 = Task.Factory.StartNew(async () =>
                {
                    for (var i = 0; i < messageCount1; i++)
                    {
                        await clusterFixture.PublishToStream(StreamProviderName, streamId, streamNamespace1, $"test:{streamId}-{streamNamespace1} message:{i}");
                    }
                });
                var publishTask2 = Task.Factory.StartNew(async () =>
                {
                    for (var i = 0; i < messageCount2; i++)
                    {
                        await clusterFixture.PublishToStream(StreamProviderName, streamId, streamNamespace2, $"test:{streamId}-{streamNamespace2} message:{i}");
                    }
                });

                var items1 = await streamSubscriptionAwaiter1.WaitAsync(GetDefaultBlockingToken());
                var items2 = await streamSubscriptionAwaiter2.WaitAsync(GetDefaultBlockingToken());

                // Wait a little longer just in case something else is published (which would be bad)
                await Task.Delay(100);

                Assert.Equal(messageCount1, items1.Count);
                Assert.Equal(messageCount2, items2.Count);

                Assert.Equal(new object[messageCount1].Select((_, i) => $"test:{streamId}-{streamNamespace1} message:{i}"), items1.Cast<string>());
                Assert.Equal(new object[messageCount2].Select((_, i) => $"test:{streamId}-{streamNamespace2} message:{i}"), items2.Cast<string>());
            });
        }

        [Theory]
        [InlineData(1, 1)]
        [InlineData(100, 100)]
        public Task TwoRedisStreamsWithDifferentStreamIdsAndDifferentStreamNamespacesOnlyReceiveTheirOwnMessages(int messageCount1, int messageCount2)
        {
            var clusterFixture = new StreamingClusterFixture();

            return clusterFixture.Dispatch(async () =>
            {
                var streamId1 = Guid.NewGuid();
                var streamId2 = Guid.NewGuid();

                var streamNamespace1 = Guid.NewGuid().ToString();
                var streamNamespace2 = Guid.NewGuid().ToString();

                var streamSubscriptionAwaiter1 = await clusterFixture.SubscribeAndGetTaskAwaiter<string>(StreamProviderName, streamId1, streamNamespace1, messageCount1);
                var streamSubscriptionAwaiter2 = await clusterFixture.SubscribeAndGetTaskAwaiter<string>(StreamProviderName, streamId2, streamNamespace2, messageCount2);

                var publishTask1 = Task.Factory.StartNew(async () =>
                {
                    for (var i = 0; i < messageCount1; i++)
                    {
                        await clusterFixture.PublishToStream(StreamProviderName, streamId1, streamNamespace1, $"test:{streamId1}-{streamNamespace1} message:{i}");
                    }
                });
                var publishTask2 = Task.Factory.StartNew(async () =>
                {
                    for (var i = 0; i < messageCount2; i++)
                    {
                        await clusterFixture.PublishToStream(StreamProviderName, streamId2, streamNamespace2, $"test:{streamId2}-{streamNamespace2} message:{i}");
                    }
                });

                var items1 = await streamSubscriptionAwaiter1.WaitAsync(GetDefaultBlockingToken());
                var items2 = await streamSubscriptionAwaiter2.WaitAsync(GetDefaultBlockingToken());

                // Wait a little longer just in case something else is published (which would be bad)
                await Task.Delay(100);

                Assert.Equal(messageCount1, items1.Count);
                Assert.Equal(messageCount2, items2.Count);

                Assert.Equal(new object[messageCount1].Select((_, i) => $"test:{streamId1}-{streamNamespace1} message:{i}"), items1.Cast<string>());
                Assert.Equal(new object[messageCount2].Select((_, i) => $"test:{streamId2}-{streamNamespace2} message:{i}"), items2.Cast<string>());
            });
        }

        [MockStreamStorage(StreamStorageName)]
        private class StreamingClusterFixture : BaseClusterFixture
        {
            private const string LocalRedisConnectionString = "127.0.0.1:6379";

            protected override void OnConfigure(ISiloHostBuilder siloHostBuilder)
            {
                base.OnConfigure(siloHostBuilder);

                siloHostBuilder
                    .AddRedisStreams(StreamProviderName, c =>
                    {
                        c.Configure((OptionsBuilder<RedisStreamOptions> options) => options.Configure(o => o.ConnectionString = LocalRedisConnectionString));
                    });
            }

            protected override void OnConfigureServices(IServiceCollection services)
            {
                base.OnConfigureServices(services);

                services.AddSingleton(Moq.Mock.Of<ILogger>());
                services.TryAddSingleton(sp => sp.GetServiceByName<IGrainStorage>(ProviderConstants.DEFAULT_STORAGE_PROVIDER_NAME));
                services.TryAddTransient(s => ConnectionMultiplexer.Connect(LocalRedisConnectionString).GetDatabase());
                services.AddSingleton(new OrleansClusterDetails(Guid.NewGuid().ToString(), Guid.NewGuid()));
            }
        }
    }

    public class OrleansClusterDetails
    {
        public Guid ServiceId { get; set; }
        public string ClusterId { get; set; }

        public OrleansClusterDetails(string deploymentId, Guid serviceId)
        {
            ClusterId = deploymentId;
            ServiceId = serviceId;
        }
    }
}
