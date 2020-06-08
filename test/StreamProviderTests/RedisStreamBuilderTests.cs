using Orleans.Redis.Common;
using CoreTests.Extensions;
using Microsoft.Extensions.DependencyInjection;
using Moq;
using Orleans;
using Orleans.Hosting;
using Orleans.Runtime;
using Orleans.Streaming.Redis;
using Serilog;
using StackExchange.Redis;
using System;
using System.Linq;
using Xunit;
using Orleans.Streaming;
using Orleans.Configuration;
using Orleans.Providers.Streams.Redis;
using Xunit.Categories;

namespace CoreTests
{
    [Category("BVT")]
    [Feature("Streaming")]
    public class RedisStreamBuilderTests
    {
        private const string MockStreamProviderName = nameof(MockStreamProviderName);
        private const string MockRedisConnectionString = nameof(MockRedisConnectionString);
        private const string ValidRedisConnectionString = "127.0.0.1:6379";

        [Fact]
        public void SiloStreamConfiguratorThrowsOnNullRedisOptionsConnectionString()
        {
            // Create the service collection
            var serviceCollection = new ServiceCollection();
            serviceCollection.AddSingleton(Mock.Of<ILogger>());

            // Mock the silo host builder
            var mockSiloHostBuilder = new Mock<ISiloHostBuilder>() { DefaultValue = DefaultValue.Mock };
            mockSiloHostBuilder
                .Setup(x => x.ConfigureServices(It.IsAny<Action<HostBuilderContext, IServiceCollection>>()))
                .Callback((Action<HostBuilderContext, IServiceCollection> action) => action?.Invoke(null, serviceCollection))
                .Returns(mockSiloHostBuilder.Object);

            // Create the stream configurator
            var siloStreamConfigurator = new SiloRedisStreamConfigurator(MockStreamProviderName, x => mockSiloHostBuilder.Object.ConfigureServices(x), x => mockSiloHostBuilder.Object.ConfigureApplicationParts(x));

            // Override the normal multiplexer dependency factory
            serviceCollection.AddSingleton(new Mock<IConnectionMultiplexer>() { DefaultValue = DefaultValue.Mock }.Object);

            // Setup redis
            siloStreamConfigurator.ConfigureRedis(options =>
            {
                options.ConnectionString = null;
            });

            // Build the service provider
            var serviceProvider = (IServiceProvider)serviceCollection.BuildServiceProvider();

            // Mimic the validation call ISiloHostBuilder makes
            var validator = serviceProvider.GetServices<IConfigurationValidator>().First(v => v.GetType() == typeof(RedisStreamOptionsValidator));

            var exception = Assert.Throws<OrleansConfigurationException>(() => validator.ValidateConfiguration());
            Assert.Equal($"RedisStreamOptions on stream provider {MockStreamProviderName} is invalid. ConnectionString is invalid", exception.Message);
        }

        [Fact]
        public void SiloStreamConfiguratorAddsConnectionMultiplexer()
        {
            // Create the service collection
            var serviceCollection = new ServiceCollection();
            serviceCollection.AddSingleton(Mock.Of<ILogger>());

            // Mock the silo host builder
            var mockSiloHostBuilder = new Mock<ISiloHostBuilder>() { DefaultValue = DefaultValue.Mock };
            mockSiloHostBuilder
                .Setup(x => x.ConfigureServices(It.IsAny<Action<HostBuilderContext, IServiceCollection>>()))
                .Callback((Action<HostBuilderContext, IServiceCollection> action) => action?.Invoke(null, serviceCollection))
                .Returns(mockSiloHostBuilder.Object);

            // Create the stream configurator
            var siloStreamConfigurator = new SiloRedisStreamConfigurator(MockStreamProviderName, x => mockSiloHostBuilder.Object.ConfigureServices(x), x => mockSiloHostBuilder.Object.ConfigureApplicationParts(x));

            // Override the normal multiplexer dependency factory
            serviceCollection.AddSingleton(new Mock<IConnectionMultiplexer>() { DefaultValue = DefaultValue.Mock }.Object);

            // Setup redis
            siloStreamConfigurator.ConfigureRedis(options =>
            {
                options.ConnectionString = ValidRedisConnectionString;
            });

            // Build the service provider
            var serviceProvider = (IServiceProvider)serviceCollection.BuildServiceProvider();
            serviceProvider.AssertRegistered<IConnectionMultiplexer>();
        }

        [Fact]
        public void SiloStreamConfiguratorSetsUpCreatableRedisAdapterFactory()
        {
            // Create the service collection
            var serviceCollection = new ServiceCollection();
            serviceCollection.AddSingleton(Mock.Of<ILogger>());

            // Mock out the silo builder
            var mockSiloHostBuilder = new Mock<ISiloHostBuilder>() { DefaultValue = DefaultValue.Mock };
            mockSiloHostBuilder
                .Setup(x => x.ConfigureServices(It.IsAny<Action<HostBuilderContext, IServiceCollection>>()))
                .Callback((Action<HostBuilderContext, IServiceCollection> action) => action?.Invoke(null, serviceCollection))
                .Returns(mockSiloHostBuilder.Object);

            // Create the actual stream configurator
            var siloStreamConfigurator = new SiloRedisStreamConfigurator(MockStreamProviderName, x => mockSiloHostBuilder.Object.ConfigureServices(x), x => mockSiloHostBuilder.Object.ConfigureApplicationParts(x));

            // Override some required dependencies so mocks work
            serviceCollection.AddSingleton(Mock.Of<ISerializationManager>());

            siloStreamConfigurator.ConfigureRedis(options =>
            {
                options.ConnectionString = ValidRedisConnectionString;
            });

            var serviceProvider = (IServiceProvider)serviceCollection.BuildServiceProvider();
            var redisAdapterFactory = RedisQueueAdapterFactory.Create(serviceProvider, MockStreamProviderName);
        }
    }
}
