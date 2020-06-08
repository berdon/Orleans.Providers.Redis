using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.DependencyInjection.Extensions;
using Moq;
using Orleans;
using Orleans.Hosting;
using Orleans.Providers;
using Orleans.Redis.Common;
using Orleans.Streams;
using Orleans.Testing.Utils;
using Serilog;
using Shared.Orleans;
using StackExchange.Redis;
using StorageTests.GrainInterfaces;
using System;
using System.Collections.Generic;
using System.Reflection;
using System.Threading;
using System.Threading.Tasks;
using Xunit;
using Xunit.Categories;

namespace StorageTests
{
    [Category("BVT")]
    [Feature("Persistence")]
    public class RedisGrainStorageTests
    {
        private const string StreamProviderName = nameof(StreamProviderName);
        private const string StreamStorageName = "PubSubStore";
        private static readonly string DefaultStreamNamespace = nameof(DefaultStreamNamespace);
        private const int DefaultBlockingTimeoutInMs = 5000;

        private static CancellationToken GetDefaultBlockingToken() => new CancellationTokenSource(DefaultBlockingTimeoutInMs).Token;

        [Fact]
        public async Task SubscribedGrainStillSubscribedAfterDeactivation()
        {
            using (var clusterFixture = new ClusterFixture())
            {
                await clusterFixture.Start();
                await clusterFixture.Dispatch(async () =>
                {
                    var grain = clusterFixture.GrainFactory.GetGrain<ITestGrain>(Guid.Empty);
                    await grain.Subscribe();
                    await grain.Deactivate();

                    await clusterFixture.PublishToStream(StreamProviderName, Guid.Empty, DefaultStreamNamespace, "some item");

                    await TestGrain.Semaphore.WaitAsync(GetDefaultBlockingToken());
                    Assert.True(TestGrain.DidResumeSubscription);
                });
            }
        }

        [Fact(Skip = "Incomplete")]
        public async Task SubscribeCallToGrainCallsRedisStringSet()
        {
            using (var clusterFixture = new ClusterFixture())
            {
                await clusterFixture.Start();
                await clusterFixture.Dispatch(async () =>
                {
                    var writeSemaphore = new SemaphoreSlim(0);
                    clusterFixture.Redis
                        .Setup(x => x.StringSet(It.IsAny<RedisKey>(), It.IsAny<RedisValue>(), It.IsAny<TimeSpan?>(), It.IsAny<When>(), It.IsAny<CommandFlags>()))
                        .Callback((RedisKey key, RedisValue value, TimeSpan? timeSpan, When when, CommandFlags flags) => writeSemaphore.Release());

                    var grain = clusterFixture.GrainFactory.GetGrain<ITestGrain>(Guid.Empty);
                    await grain.Subscribe();

                    await writeSemaphore.WaitAsync(GetDefaultBlockingToken());
                });
            }
        }

        public class TestGrainState
        {
            public bool IsSubscribed { get; set; } = false;
        }

        [StorageProvider]
        public class TestGrain : Grain<TestGrainState>, ITestGrain, IAsyncObserver<string>
        {
            public static bool DidResumeSubscription { get; private set; } = false;
            public static SemaphoreSlim Semaphore { get; } = new SemaphoreSlim(0);
            public static List<string> Items { get; } = new List<string>();

            public override async Task OnActivateAsync()
            {
                if (State.IsSubscribed)
                {
                    DidResumeSubscription = true;

                    var streamProvider = GetStreamProvider(StreamProviderName);
                    var stream = streamProvider.GetStream<string>(Guid.Empty, DefaultStreamNamespace);
                    foreach (var handle in await stream.GetAllSubscriptionHandles())
                    {
                        await handle.ResumeAsync(this);
                    }
                }
            }

            public async Task Deactivate()
            {
                DidResumeSubscription = false;
                DeactivateOnIdle();
            }

            public async Task Subscribe()
            {
                var streamProvider = GetStreamProvider(StreamProviderName);
                var stream = streamProvider.GetStream<string>(Guid.Empty, DefaultStreamNamespace);
                await stream.SubscribeAsync(this);

                State.IsSubscribed = true;
                await WriteStateAsync();
            }

            public async Task Unsubscribe()
            {
                var streamProvider = GetStreamProvider(StreamProviderName);
                var stream = streamProvider.GetStream<string>(Guid.Empty, DefaultStreamNamespace);
                foreach (var subscription in await stream.GetAllSubscriptionHandles())
                {
                    await subscription.UnsubscribeAsync();
                }

                State.IsSubscribed = false;
                await WriteStateAsync();
            }

            public async Task OnCompletedAsync()
            {
                throw new NotImplementedException();
            }

            public async Task OnErrorAsync(Exception ex)
            {
                throw new NotImplementedException();
            }

            public async Task OnNextAsync(string item, StreamSequenceToken token = null)
            {
                Semaphore.Release();
            }
        }

        [MockStorageProvider(ProviderConstants.DEFAULT_STORAGE_PROVIDER_NAME)]
        [MockStreamProvider(StreamProviderName)]
        public class ClusterFixture : Orleans.Testing.Utils.ClusterFixture
        {
            public Mock<IDatabase> Redis { get; } = new Mock<IDatabase>();
            public Mock<ILogger> Serilog { get; } = new Mock<ILogger>();

            public async Task Start()
            {
                await Start(11111 + Testing.TestIndex % 100, 30000 + Testing.TestIndex % 100);
            }

            protected override void OnConfigure(LocalClusterBuilder clusterBuilder)
            {
                base.OnConfigure(clusterBuilder);

                clusterBuilder.ConfigureApplicationParts(o =>
                {
                    o.AddFromAppDomain();
                    o.AddApplicationPart(Assembly.GetAssembly(typeof(TestGrain)));
                });
            }            

            protected override void OnConfigure(ISiloHostBuilder siloHostBuilder)
            {
                base.OnConfigure(siloHostBuilder);
                siloHostBuilder.AddRedisGrainStorage(StreamStorageName, o => { o.ConnectionString = "127.0.0.1:6379"; });
            }

            protected override void OnConfigureServices(IServiceCollection services)
            {
                base.OnConfigureServices(services);

                services.AddSingleton(Redis.Object);
                services.AddSingleton(Serilog.Object);

                services.TryAddSingleton(Moq.Mock.Of<ISerializationManager>());
            }
        }
    }
}
