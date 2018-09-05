using Orleans.Redis.Common;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Options;
using Orleans.Configuration;
using Orleans.Configuration.Overrides;
using Orleans.Providers.Streams.Common;
using Orleans.Streams;
using Serilog;
using StackExchange.Redis;
using System;
using System.Threading.Tasks;

namespace Orleans.Providers.Streams.Redis
{
    public class RedisQueueAdapterFactory : IQueueAdapterFactory
    {
        private readonly string _providerName;
        private readonly RedisStreamOptions _options;
        private readonly ClusterOptions _clusterOptions;
        private readonly ILogger _logger;
        private readonly IRedisDataAdapter _dataAdapter;
        private readonly HashRingBasedStreamQueueMapper _streamQueueMapper;
        private readonly IQueueAdapterCache _adapterCache;

        /// <summary>
        /// Application level failure handler override.
        /// </summary>
        protected Func<QueueId, Task<IStreamFailureHandler>> StreamFailureHandlerFactory { private get; set; }

        public RedisQueueAdapterFactory(
            string name,
            RedisStreamOptions options,
            HashRingStreamQueueMapperOptions queueMapperOptions,
            SimpleQueueCacheOptions cacheOptions,
            IServiceProvider serviceProvider,
            IOptions<ClusterOptions> clusterOptions,
            IRedisDataAdapter dataAdapter,
            ILogger logger,
            ISerializationManager serializationManager,
            IServiceProvider provider)
        {
            _providerName = name;
            _options = options ?? throw new ArgumentNullException(nameof(options));
            _clusterOptions = clusterOptions.Value;
            _logger = logger ?? throw new ArgumentNullException(nameof(logger));
            _dataAdapter = dataAdapter;

            _streamQueueMapper = new HashRingBasedStreamQueueMapper(queueMapperOptions, _providerName);

            var microsoftLoggerFactory = provider.GetService<Microsoft.Extensions.Logging.ILoggerFactory>();
            _adapterCache = new SimpleQueueAdapterCache(cacheOptions, _providerName, microsoftLoggerFactory);
        }

        public virtual void Init()
        {
            StreamFailureHandlerFactory = StreamFailureHandlerFactory ??
                ((qid) => Task.FromResult<IStreamFailureHandler>(new NoOpStreamDeliveryFailureHandler()));
        }

        public Task<IQueueAdapter> CreateAdapter()
        {
            var adapter = new RedisQueueAdapter(
                _options,
                _dataAdapter, 
                _streamQueueMapper,
                _logger,
                _clusterOptions.ServiceId,
                _providerName);

            return Task.FromResult<IQueueAdapter>(adapter);
        }

        public Task<IStreamFailureHandler> GetDeliveryFailureHandler(QueueId queueId) => StreamFailureHandlerFactory(queueId);
        public IQueueAdapterCache GetQueueAdapterCache() => _adapterCache;
        public IStreamQueueMapper GetStreamQueueMapper() => _streamQueueMapper;

        public static RedisQueueAdapterFactory Create(IServiceProvider services, string name)
        {
            var RedisOptions = services.GetOptionsByName<RedisStreamOptions>(name);
            var hashRingOptions = services.GetOptionsByName<HashRingStreamQueueMapperOptions>(name);
            var cacheOptions = services.GetOptionsByName<SimpleQueueCacheOptions>(name);
            IOptions<ClusterOptions> clusterOptions = services.GetProviderClusterOptions(name);

            var factory = ActivatorUtilities.CreateInstance<RedisQueueAdapterFactory>(
                services,
                name,
                RedisOptions,
                hashRingOptions,
                cacheOptions,
                clusterOptions);
            factory.Init();

            return factory;
        }
    }
}
