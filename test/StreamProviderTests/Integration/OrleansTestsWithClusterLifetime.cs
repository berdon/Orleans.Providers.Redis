using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Options;
using Nito.AsyncEx;
using Orleans.Configuration;
using Orleans.Hosting;
using Orleans.Redis.Common;
using Orleans.Testing.Utils;
using Serilog;
using Shared;
using Shared.Orleans;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using Xunit;
using Xunit.Categories;

namespace CoreTests.Integration
{
    [Category("BVT")]
    [Category("Integration")]
    [Feature("Streaming")]
    [Collection("OrleansIntegrationTests")]
    public class OrleansTestsWithClusterLifetime
    {
        private const string StreamProviderName = nameof(StreamProviderName);
        private const string StreamStorageName = "PubSubStore";
        private static readonly TimeSpan DefaultBlockingTimeout = TimeSpan.FromMilliseconds(10000);

        private static CancellationTokenSource GetTokenSource(TimeSpan? time = null) => new CancellationTokenSource(time ?? DefaultBlockingTimeout);

        [Fact]
        public async Task RedisStreamCanSendAndReceiveItem()
        {
            using (var clusterFixture = new StreamingClusterFixture())
            {
                await clusterFixture.Start();
                await clusterFixture.Dispatch(async () =>
                {
                    var streamId = Guid.NewGuid();
                    var streamNamespace = Guid.NewGuid().ToString();

                    var streamSubscriptionAwaiter = await clusterFixture.SubscribeAndGetTaskAwaiter<string>(StreamProviderName, streamId, streamNamespace, 1);
                    await clusterFixture.PublishToStream(StreamProviderName, streamId, streamNamespace, "test");
                    using (var cts = GetTokenSource())
                    {
                        await streamSubscriptionAwaiter.WaitAsync(cts.Token);
                    }
                });
            }
        }

        [Theory]
        [InlineData(1, 1)]
        [InlineData(10, 10)]
        public async Task TwoRedisStreamsWithDifferentStreamIdsOnlyReceiveTheirOwnMessages(int messageCount1, int messageCount2)
        {
            using (var clusterFixture = new StreamingClusterFixture())
            {
                await clusterFixture.Start();
                await clusterFixture.Dispatch(async () =>
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

                    List<dynamic> items1 = null, items2 = null;
                    using (var cts = GetTokenSource())
                    {
                        var results = await Task.WhenAll(streamSubscriptionAwaiter1, streamSubscriptionAwaiter2).WaitAsync(cts.Token);
                        items1 = results[0];
                        items2 = results[1];
                    }

                    // Wait a little longer just in case something else is published (which would be bad)
                    await Task.Delay(100);

                    Assert.Equal(messageCount1, items1.Count);
                    Assert.Equal(messageCount2, items2.Count);

                    AssertEx.Equal(new object[messageCount1].Select((_, i) => $"test:{streamId1}-{streamNamespace} message:{i}").OrderBy(x => x), items1.Cast<string>().OrderBy(x => x));
                    AssertEx.Equal(new object[messageCount2].Select((_, i) => $"test:{streamId2}-{streamNamespace} message:{i}").OrderBy(x => x), items2.Cast<string>().OrderBy(x => x));
                });
            }
        }

        [Theory]
        [InlineData(1, 1)]
        [InlineData(10, 10)]
        public async Task TwoRedisStreamsWithSameStreamIdsAndDifferentStreamNamespacesOnlyReceiveTheirOwnMessages(int messageCount1, int messageCount2)
        {
            using (var clusterFixture = new StreamingClusterFixture())
            {
                await clusterFixture.Start();
                await clusterFixture.Dispatch(async () =>
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

                    List<dynamic> items1 = null, items2 = null;
                    using (var cts = GetTokenSource())
                    {
                        var results = await Task.WhenAll(streamSubscriptionAwaiter1, streamSubscriptionAwaiter2).WaitAsync(cts.Token);
                        items1 = results[0];
                        items2 = results[1];
                    }

                    // Wait a little longer just in case something else is published (which would be bad)
                    await Task.Delay(100);

                    Assert.Equal(messageCount1, items1.Count);
                    Assert.Equal(messageCount2, items2.Count);

                    AssertEx.Equal(new object[messageCount1].Select((_, i) => $"test:{streamId}-{streamNamespace1} message:{i}").OrderBy(x => x), items1.Cast<string>().OrderBy(x => x));
                    AssertEx.Equal(new object[messageCount2].Select((_, i) => $"test:{streamId}-{streamNamespace2} message:{i}").OrderBy(x => x), items2.Cast<string>().OrderBy(x => x));
                });
            }
        }

        [Theory]
        [InlineData(1, 1)]
        [InlineData(100, 100)]
        public async Task TwoRedisStreamsWithDifferentStreamIdsAndDifferentStreamNamespacesOnlyReceiveTheirOwnMessages(int messageCount1, int messageCount2)
        {
            using (var clusterFixture = new StreamingClusterFixture())
            {
                await clusterFixture.Start();
                await clusterFixture.Dispatch(async () =>
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

                    List<dynamic> items1 = null, items2 = null;
                    using (var cts = GetTokenSource())
                    {
                        var results = await Task.WhenAll(streamSubscriptionAwaiter1, streamSubscriptionAwaiter2).WaitAsync(cts.Token);
                        items1 = results[0];
                        items2 = results[1];
                    }

                    // Wait a little longer just in case something else is published (which would be bad)
                    await Task.Delay(100);

                    Assert.Equal(messageCount1, items1.Count);
                    Assert.Equal(messageCount2, items2.Count);

                    AssertEx.Equal(new object[messageCount1].Select((_, i) => $"test:{streamId1}-{streamNamespace1} message:{i}").OrderBy(x => x), items1.Cast<string>().OrderBy(x => x));
                    AssertEx.Equal(new object[messageCount2].Select((_, i) => $"test:{streamId2}-{streamNamespace2} message:{i}").OrderBy(x => x), items2.Cast<string>().OrderBy(x => x));
                });
            }
        }

        [Theory]
        [InlineData(10, 10)]
        [InlineData(100, 10)]
        public async Task NRedisStreamsWithDifferentStreamIdsAndDifferentStreamNamespacesOnlyReceiveTheirOwnMessages(int n, int messageCount)
        {
            using (var clusterFixture = new StreamingClusterFixture())
            {
                await clusterFixture.Start();
                await clusterFixture.Dispatch(async () =>
                {
                    var dataSets = await CreateProducerConsumerStreamAwaiter(clusterFixture, n, messageCount);

                    await Task.WhenAll(dataSets.Select(d => d.Awaiter));

                    foreach (var set in dataSets)
                    {
                        var items = await set.Awaiter;
                        Assert.Equal(messageCount, items.Count);
                        AssertEx.Equal(new object[messageCount].Select((_, i) => $"test:{set.StreamId}-{set.StreamNamespace} message:{i}").OrderBy(x => x), items.Cast<string>().OrderBy(x => x));
                    }
                });
            }
        }

        private async Task<List<(Guid StreamId, string StreamNamespace, Task<List<dynamic>> Awaiter)>> CreateProducerConsumerStreamAwaiter(ClusterFixture clusterFixture, int n, int messageCount)
        {
            var dataSets = new List<(Guid StreamId, string StreamNamespace, Task<List<dynamic>> Awaiter)>();
            for (var i = 0; i < n; i++)
            {
                dataSets.Add(await CreateProducerConsumerStreamAwaiter(clusterFixture, messageCount));
            }
            return dataSets;
        }

        private async Task<(Guid StreamId, string StreamNamespace, Task<List<dynamic>> Awaiter)> CreateProducerConsumerStreamAwaiter(ClusterFixture clusterFixture, int messageCount)
        {
            var streamId = Guid.NewGuid();
            var streamNamespace = Guid.NewGuid().ToString();
            var streamSubscriptionAwaiter = await clusterFixture.SubscribeAndGetTaskAwaiter<string>(StreamProviderName, streamId, streamNamespace, messageCount);
            var publishTask = Task.Factory.StartNew(async () =>
            {
                for (var i = 0; i < messageCount; i++)
                {
                    await clusterFixture.PublishToStream(StreamProviderName, streamId, streamNamespace, $"test:{streamId}-{streamNamespace} message:{i}");
                }
            });
            return (streamId, streamNamespace, streamSubscriptionAwaiter);
        }

        [Fact]
        public async Task OnlyOneConnectionMultiplexerIsCreated()
        {
            using (var clusterFixture = new StreamingClusterFixture())
            {
                await clusterFixture.Start();
                await clusterFixture.Dispatch(async () =>
                {
                    // Make sure some producer/consumers are set up
                    var dataSets = await CreateProducerConsumerStreamAwaiter(clusterFixture, 100, 10);
                    await Task.WhenAll(dataSets.Select(d => d.Awaiter));

                    var connectionMultiplexerFactory = (CachedConnectionMultiplexerFactory) clusterFixture.ClusterServices.GetRequiredService<IConnectionMultiplexerFactory>();
                    Assert.Single(connectionMultiplexerFactory.TestHook_ConnectionMultiplexers);
                });
            }
        }

        [MockStreamStorage(StreamStorageName)]
        private class StreamingClusterFixture : ClusterFixture
        {
            public const string LocalRedisConnectionString = "127.0.0.1:6379";
            
            public async Task Start()
            {
                await Start(11111 + Testing.TestIndex % 100, 30000 + Testing.TestIndex % 100);
            }
            protected override void OnConfigure(ISiloHostBuilder siloHostBuilder)
            {
                base.OnConfigure(siloHostBuilder);

                siloHostBuilder
                    .AddRedisStreams(StreamProviderName, c =>
                    {
                        c.Configure((OptionsBuilder<RedisStreamOptions> options) => options.Configure(o =>
                        {
                            o.PersistenceLifetime = PersistenceLifetime.ClusterLifetime;
                            o.ConnectionString = LocalRedisConnectionString;
                        }));
                    });
            }

            protected override void OnConfigureServices(IServiceCollection services)
            {
                base.OnConfigureServices(services);

                services.AddSingleton(new Moq.Mock<ILogger>() { DefaultValue = Moq.DefaultValue.Mock }.Object);
            }
        }
    }
}
