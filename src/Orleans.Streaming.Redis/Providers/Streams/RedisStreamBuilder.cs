using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.DependencyInjection.Extensions;
using Orleans.ApplicationParts;
using Orleans.Configuration;
using Orleans.Hosting;
using Orleans.Providers.Streams.Redis;
using Orleans.Redis.Common;
using Orleans.Streams;
using System;

namespace Orleans.Streaming
{
    public class SiloRedisStreamConfigurator : SiloPersistentStreamConfigurator
    {
        public SiloRedisStreamConfigurator(string name, Action<Action<IServiceCollection>> configureServicesDelegate, Action<Action<IApplicationPartManager>> configureAppPartsDelegate)
            : base(name, configureServicesDelegate, RedisQueueAdapterFactory.Create)
        {
            configureAppPartsDelegate(parts =>
            {
                parts.AddFrameworkPart(typeof(RedisQueueAdapterFactory).Assembly);
            });
            
            configureServicesDelegate(services => 
            {
                services.TryAddSingleton(SilentLogger.Logger);
                services.ConfigureNamedOptionForLogging<RedisStreamOptions>(name);
                services.TryAddSingleton(CachedConnectionMultiplexerFactory.Default);
                services.TryAddSingleton<ISerializationManager, OrleansSerializationManager>();
                services.AddSingleton<IRedisDataAdapter, RedisDataAdapter>();
                services.AddTransient<IConfigurationValidator>(sp => new RedisStreamOptionsValidator(sp.GetOptionsByName<RedisStreamOptions>(name), name));
                services.ConfigureNamedOptionForLogging<SimpleQueueCacheOptions>(name);
                services.AddTransient<IConfigurationValidator>(sp => SimpleQueueCacheOptionsValidator.Create(sp, name));
                services.ConfigureNamedOptionForLogging<HashRingStreamQueueMapperOptions>(name);
            });

            this.ConfigureStreamPubSub(StreamPubSubType.ExplicitGrainBasedAndImplicit);
        }

        public SiloRedisStreamConfigurator ConfigureRedis(Action<RedisStreamOptions> configureOptions)
        {
            this.Configure<RedisStreamOptions>(ob => ob.Configure(configureOptions));
            return this;
        }

        public SiloRedisStreamConfigurator ConfigureCache(int cacheSize = SimpleQueueCacheOptions.DEFAULT_CACHE_SIZE)
        {
            this.Configure<SimpleQueueCacheOptions>(ob => ob.Configure(options => options.CacheSize = cacheSize));
            return this;
        }

        public SiloRedisStreamConfigurator ConfigurePartitioning(int numOfPartition = HashRingStreamQueueMapperOptions.DEFAULT_NUM_QUEUES)
        {
            this.Configure<HashRingStreamQueueMapperOptions>(ob => ob.Configure(options => options.TotalQueueCount = numOfPartition));
            return this;
        }
    }

    public class ClusterClientRedisStreamConfigurator : ClusterClientPersistentStreamConfigurator
    {
        public ClusterClientRedisStreamConfigurator(string name, IClientBuilder builder)
            : base(name, builder, RedisQueueAdapterFactory.Create)
        {
            builder
                .ConfigureApplicationParts(parts => parts.AddFrameworkPart(typeof(RedisQueueAdapterFactory).Assembly))
                .ConfigureServices(services =>
                {
                    services.TryAddSingleton(SilentLogger.Logger);
                    services.ConfigureNamedOptionForLogging<RedisStreamOptions>(name);
                    services.TryAddSingleton(CachedConnectionMultiplexerFactory.Default);
                    services.TryAddSingleton<ISerializationManager, OrleansSerializationManager>();
                    services.AddSingleton<IRedisDataAdapter, RedisDataAdapter>();
                    services.AddTransient<IConfigurationValidator>(sp => new RedisStreamOptionsValidator(sp.GetOptionsByName<RedisStreamOptions>(name), name));
                    services.ConfigureNamedOptionForLogging<HashRingStreamQueueMapperOptions>(name);
                });
        }

        public ClusterClientRedisStreamConfigurator ConfigureRedis(Action<RedisStreamOptions> configureOptions)
        {
            this.Configure<RedisStreamOptions>(ob => ob.Configure(configureOptions));
            return this;
        }
    }
}
