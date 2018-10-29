using Orleans.Redis.Common;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Options;
using Newtonsoft.Json;
using Orleans.Configuration;
using Orleans.Runtime;
using Orleans.Persistence.Redis.Extensions;
using Orleans.Persistence.Redis.Helpers;
using Serilog;
using StackExchange.Redis;
using System;
using System.Collections.Generic;
using System.Security.Cryptography;
using System.Text;
using System.Threading.Tasks;
using System.Threading;

namespace Orleans.Storage
{
    public class RedisGrainStorage : IGrainStorage, ILifecycleParticipant<ISiloLifecycle>
    {
        private readonly string _name;
        private readonly RedisGrainStorageOptions _options;
        private readonly ClusterOptions _clusterOptions;
        private readonly ILogger _logger;
        private readonly ISerializationManager _serializationManager;
        private readonly JsonSerializerSettings _jsonSerializerSettings = new JsonSerializerSettings
        {
            // Since order is not guaranteed during serialization force dictionary to be sorted then write json.
            // When serializing dates and de-serializing dates your offsets can change so use the custom formatter.
            Converters = new List<JsonConverter> { new CustomDateFormatConverter(), new UnorderedCollectionConverter() },
            //Don't add null or default properties to the json.
            DefaultValueHandling = DefaultValueHandling.IgnoreAndPopulate,
            //Don't add null properties to the json.
            NullValueHandling = NullValueHandling.Ignore
        };

        private IConnectionMultiplexerFactory _connectionMultiplexerFactory;
        private IConnectionMultiplexer _connectionMultiplexer;
        private IDatabase _redisClient => _connectionMultiplexer.GetDatabase();

        public RedisGrainStorage(string name, RedisGrainStorageOptions options, ILogger logger, IOptions<ClusterOptions> clusterOptions, ISerializationManager serializationManager, IConnectionMultiplexerFactory connectionMultiplexerFactory)
        {
            _name = name;
            _options = options;
            _logger = logger;
            _serializationManager = serializationManager;
            _clusterOptions = clusterOptions.Value;
            _connectionMultiplexerFactory = connectionMultiplexerFactory;
        }

        public async Task Init(CancellationToken ct)
        {
            _connectionMultiplexer = await _connectionMultiplexerFactory.CreateAsync(_options.ConnectionString);
        }

        public Task Close(CancellationToken ct)
        {
            return Task.CompletedTask;
        }

        public async Task ReadStateAsync(string grainType, GrainReference grainReference, IGrainState grainState)
        {
            try
            {
                string key = DetermineRedisKey(grainType, grainReference);
                var stateType = grainState.State.GetType();
                var state = await ReadStateFromRedisAsync(key, stateType);

                if (state != null)
                {
                    grainState.ETag = GenerateETag(state, stateType);
                    grainState.State = state;
                }
            }
            catch (Exception e)
            {
                _logger.Error(e, e.Message);
            }
        }

        private async Task<object> ReadStateFromRedisAsync(string key, Type type)
        {
            return await Task.Run(() => _redisClient.GetObject(_serializationManager, key, type));

        }

        public async Task WriteStateAsync(string grainType, GrainReference grainReference, IGrainState grainState)
        {
            try
            {
                string key = DetermineRedisKey(grainType, grainReference);
                var stateType = grainState.State.GetType();
                var storedState = await ReadStateFromRedisAsync(key, stateType);

                if (storedState != null)
                {
                    await ValidateETag(grainState.ETag, storedState, stateType);
                }

                await Task.Run(() => _redisClient.StoreObject(_serializationManager, grainState.State, stateType, key));
                grainState.ETag = GenerateETag(grainState.State, stateType);
            }
            catch (Exception e)
            {
                _logger.Error(e, e.Message);
            }
        }

        public async Task ClearStateAsync(string grainType, GrainReference grainReference, IGrainState grainState)
        {
            try
            {
                string key = DetermineRedisKey(grainType, grainReference);
                var stateType = grainState.State.GetType();
                await Task.Run(() => _redisClient.DeleteObject(stateType, key));
                grainState.ETag = null;
            }
            catch (Exception e)
            {
                _logger.Error(e, e.Message);
            }
        }

        private string DetermineRedisKey(string grainType, GrainReference grainReference)
        {
            var persistencePrefix = _options.PersistenceLifetime == PersistenceLifetime.ServiceLifetime
                ? _clusterOptions.ServiceId
                : _clusterOptions.ClusterId;

            return string.Concat(_name, ":", persistencePrefix, ":", grainReference.ToKeyString());
        }

        /// <summary>
        /// Throws InconsistentStateException if ETags don't match up.
        /// </summary>
        private Task ValidateETag<T>(string currentETag, T storedDocument, Type stateType)
        {
            var storedETag = GenerateETag(storedDocument, typeof(T));
            if (storedETag != currentETag)
            {
                if (_options.ThrowExceptionOnInconsistentETag) {
                    // Etags don't match! Inconsistent state
                    throw new InconsistentStateException(
                        $"Inconsistent state detected while performing write operations for type:{stateType.Name}.", storedETag, currentETag);
                }

                _logger.Warning("Inconsistent state detected while performing write operations for type:{typeof(T).Name}.", stateType.Name);
            }

            return Task.CompletedTask;
        }

        private string GenerateETag<T>(object obj)
        {
            return GenerateETag(obj, typeof(T));
        }

        private string GenerateETag(object obj, Type type)
        {
            //See comments on the json serializer settings for full details on why this happens.
            var json = JsonConvert.SerializeObject(obj, type, _jsonSerializerSettings);
            return GenerateMD5(json);
        }

        public static string GenerateMD5(string input)
        {
            var hash = MD5.Create();
            var bytes = hash.ComputeHash(Encoding.UTF8.GetBytes(input));
            return BitConverter.ToString(bytes).Replace("-", "");
        }

        public void Participate(ISiloLifecycle lifecycle)
        {
            lifecycle.Subscribe(OptionFormattingUtilities.Name<RedisGrainStorage>(_name), _options.InitStage, Init, Close);
        }
    }

    /// <summary>
    /// Factory for creating MemoryGrainStorage
    /// </summary>
    public class RedisGrainStorageFactory
    {
        public static IGrainStorage Create(IServiceProvider services, string name)
        {
            IOptionsSnapshot<RedisGrainStorageOptions> optionsSnapshot = services.GetRequiredService<IOptionsSnapshot<RedisGrainStorageOptions>>();
            return ActivatorUtilities.CreateInstance<RedisGrainStorage>(services, name, optionsSnapshot.Get(name));
        }
    }
}
