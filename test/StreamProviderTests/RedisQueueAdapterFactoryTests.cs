using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Options;
using Moq;
using Orleans.Configuration;
using Orleans.Providers.Streams.Redis;
using Orleans.Redis.Common;
using Shared;
using System;
using System.Collections.Generic;
using System.Text;
using Xunit;
using Xunit.Categories;

namespace StreamingTests
{
    [Category("BVT")]
    [Feature("Streaming")]
    public class RedisQueueAdapterFactoryTests
    {
        [Fact]
        public void NullNameParamThrowsInConstructor() => AssertEx.ThrowsAny<ArgumentNullException>(
            () => new RedisQueueAdapterFactory(null, null, null, null, null, null, null, null, null, null), e => e.ParamName == "name");

        [Fact]
        public void NullRedisStreamOptionsParamThrowsInConstructor() => AssertEx.ThrowsAny<ArgumentNullException>(
            () => new RedisQueueAdapterFactory(TestConstants.ValidProviderName,
                null, null, null, null, null, null, null, null, null),
            e => e.ParamName == "options");

        [Fact]
        public void NullConnectionMultiplexerFactoryParamThrowsInConstructor() => AssertEx.ThrowsAny<ArgumentNullException>(
            () => new RedisQueueAdapterFactory(TestConstants.ValidProviderName, TestConstants.ValidRedisStreamOptions,
                null, null, null, null, null, null, null, null),
            e => e.ParamName == "connectionMultiplexerFactory");

        [Fact]
        public void NullHashRingStreamQueueMapperOptionsParamThrowsInConstructor() => AssertEx.ThrowsAny<ArgumentNullException>(
            () => new RedisQueueAdapterFactory(TestConstants.ValidProviderName, TestConstants.ValidRedisStreamOptions,
                CachedConnectionMultiplexerFactory.Default, null, null, null, null, null, null, null),
            e => e.ParamName == "queueMapperOptions");

        [Fact]
        public void NullSimpleQueueCacheOptionsParamThrowsInConstructor() => AssertEx.ThrowsAny<ArgumentNullException>(
            () => new RedisQueueAdapterFactory(
                TestConstants.ValidProviderName, TestConstants.ValidRedisStreamOptions, CachedConnectionMultiplexerFactory.Default,
                TestConstants.ValidHashRingStreamQueueMapperOptions, null, null, null, null, null, null),
            e => e.ParamName == "cacheOptions");

        [Fact]
        public void NullServiceProviderParamThrowsInConstructor() => AssertEx.ThrowsAny<ArgumentNullException>(
            () => new RedisQueueAdapterFactory(
                TestConstants.ValidProviderName, TestConstants.ValidRedisStreamOptions, CachedConnectionMultiplexerFactory.Default,
                TestConstants.ValidHashRingStreamQueueMapperOptions, TestConstants.ValidSimpleQueueCacheOptions,
                null, null, null, null, null),
            e => e.ParamName == "serviceProvider");

        [Fact]
        public void NullClusterOptionsParamThrowsInConstructor() => AssertEx.ThrowsAny<ArgumentNullException>(
            () => new RedisQueueAdapterFactory(
                TestConstants.ValidProviderName, TestConstants.ValidRedisStreamOptions, CachedConnectionMultiplexerFactory.Default,
                TestConstants.ValidHashRingStreamQueueMapperOptions, TestConstants.ValidSimpleQueueCacheOptions,
                new ServiceCollection().BuildServiceProvider(), null, null, null, null),
            e => e.ParamName == "clusterOptions");

        [Fact]
        public void NullDataAdapterParamThrowsInConstructor() => AssertEx.ThrowsAny<ArgumentNullException>(
            () => new RedisQueueAdapterFactory(
                TestConstants.ValidProviderName, TestConstants.ValidRedisStreamOptions, CachedConnectionMultiplexerFactory.Default,
                TestConstants.ValidHashRingStreamQueueMapperOptions, TestConstants.ValidSimpleQueueCacheOptions,
                new ServiceCollection().BuildServiceProvider(), Mock.Of<IOptions<ClusterOptions>>(),
                null, null, null),
            e => e.ParamName == "dataAdapter");

        [Fact]
        public void NullISerializationManagerParamThrowsInConstructor() => AssertEx.ThrowsAny<ArgumentNullException>(
            () => new RedisQueueAdapterFactory(
                TestConstants.ValidProviderName, TestConstants.ValidRedisStreamOptions, CachedConnectionMultiplexerFactory.Default,
                TestConstants.ValidHashRingStreamQueueMapperOptions, TestConstants.ValidSimpleQueueCacheOptions,
                new ServiceCollection().BuildServiceProvider(), Mock.Of<IOptions<ClusterOptions>>(),
                Mock.Of<IRedisDataAdapter>(), null, null),
            e => e.ParamName == "serializationManager");
    }
}
