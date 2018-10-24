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
using Orleans.Runtime;

namespace Orleans.Providers.Streams.Redis
{
    public class RedisQueueAdapterFactory : IQueueAdapterFactory
    {
        private readonly string _providerName;
        private readonly RedisStreamOptions _options;
        private readonly IConnectionMultiplexerFactory _connectionMultiplexerFactory;
        private readonly ClusterOptions _clusterOptions;
        private readonly ILogger _logger;
        private readonly IRedisDataAdapter _dataAdapter;
        private readonly IStreamQueueMapper _streamQueueMapper;
        private readonly IQueueAdapterCache _adapterCache;

        /// <summary>
        /// Application level failure handler override.
        /// </summary>
        protected Func<QueueId, Task<IStreamFailureHandler>> StreamFailureHandlerFactory { private get; set; }

        public RedisQueueAdapterFactory(
            string name,
            RedisStreamOptions options,
            IConnectionMultiplexerFactory connectionMultiplexerFactory,
            HashRingStreamQueueMapperOptions queueMapperOptions,
            SimpleQueueCacheOptions cacheOptions,
            IServiceProvider serviceProvider,
            IOptions<ClusterOptions> clusterOptions,
            IRedisDataAdapter dataAdapter,
            ILogger logger,
            ISerializationManager serializationManager)
        {
            if (string.IsNullOrEmpty(name)) throw new ArgumentNullException(nameof(name));
            if (options == null) throw new ArgumentNullException(nameof(options));
            if (connectionMultiplexerFactory == null) throw new ArgumentNullException(nameof(connectionMultiplexerFactory));
            if (queueMapperOptions == null) throw new ArgumentNullException(nameof(queueMapperOptions));
            if (cacheOptions == null) throw new ArgumentNullException(nameof(cacheOptions));
            if (serviceProvider == null) throw new ArgumentNullException(nameof(serviceProvider));
            if (clusterOptions == null) throw new ArgumentNullException(nameof(clusterOptions));
            if (dataAdapter == null) throw new ArgumentNullException(nameof(dataAdapter));
            if (serializationManager == null) throw new ArgumentNullException(nameof(serializationManager));

            _providerName = name;
            _options = options;
            _connectionMultiplexerFactory = connectionMultiplexerFactory;
            _clusterOptions = clusterOptions.Value;
            _logger = logger.ForContext<RedisQueueAdapterFactory>();
            _dataAdapter = dataAdapter;

            _streamQueueMapper = new HashRingBasedStreamQueueMapper(queueMapperOptions, _providerName);

            var microsoftLoggerFactory = serviceProvider.GetService<Microsoft.Extensions.Logging.ILoggerFactory>();
            _adapterCache = new SimpleQueueAdapterCache(cacheOptions, _providerName, microsoftLoggerFactory);
        }

        public virtual void Init()
        {
            StreamFailureHandlerFactory = StreamFailureHandlerFactory ??
                ((qid) => Task.FromResult<IStreamFailureHandler>(new LoggingStreamDeliveryFailureHandler(_logger)));
        }

        public Task<IQueueAdapter> CreateAdapter()
        {
            var adapter = new RedisQueueAdapter(
                _options,
                _connectionMultiplexerFactory,
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

    internal class LoggingStreamDeliveryFailureHandler : IStreamFailureHandler
    {
        public bool ShouldFaultSubsriptionOnError => false;

        private readonly ILogger _logger;

        public LoggingStreamDeliveryFailureHandler(ILogger logger)
        {
            _logger = logger;
        }

        public Task OnDeliveryFailure(GuidId subscriptionId, string streamProviderName, IStreamIdentity streamIdentity, StreamSequenceToken sequenceToken)
        {
            _logger.Error("Failed to deliver message to {ProviderName}://{Identity}/{SubscriptionId}#{SequenceId}", streamProviderName, streamIdentity, subscriptionId, sequenceToken);
            return Task.CompletedTask;
        }

        public Task OnSubscriptionFailure(GuidId subscriptionId, string streamProviderName, IStreamIdentity streamIdentity, StreamSequenceToken sequenceToken)
        {
            _logger.Error("Failed to subscribe to {ProviderName}://{Identity}/{SubscriptionId}#{SequenceId}", streamProviderName, streamIdentity, subscriptionId, sequenceToken);
            return Task.CompletedTask;
        }
    }
}
