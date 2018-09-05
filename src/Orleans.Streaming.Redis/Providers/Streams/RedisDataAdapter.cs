using Orleans.Redis.Common;
using Orleans.Providers.Streams.Common;
using Orleans.Serialization;
using Orleans.Streams;
using StackExchange.Redis;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;

namespace Orleans.Providers.Streams.Redis
{
    /// <summary>
    /// Converts event data to and from RedisValue message
    /// </summary>
    public interface IRedisDataAdapter
    {
        /// <summary>
        /// Creates a RedisValue message from stream event data.
        /// </summary>
        RedisValue ToRedisValue<T>(Guid streamGuid, string streamNamespace, IEnumerable<T> events, Dictionary<string, object> requestContext);

        /// <summary>
        /// Creates a batch container from a RedisValue message
        /// </summary>
        IBatchContainer FromRedisValue(RedisValue value, long sequenceId);
    }

    public class RedisDataAdapter : IRedisDataAdapter, IOnDeserialized
    {
        private ISerializationManager _serializationManager;

        public RedisDataAdapter(ISerializationManager serializationManager)
        {
            _serializationManager = serializationManager;
        }

        public RedisValue ToRedisValue<T>(Guid streamGuid, string streamNamespace, IEnumerable<T> events, Dictionary<string, object> requestContext)
        {
            var container = new RedisBatchContainer(streamGuid, streamNamespace, events.Cast<object>().ToList(), requestContext);
            var rawBytes = _serializationManager.SerializeToByteArray(container);
            return rawBytes;
        }

        public IBatchContainer FromRedisValue(RedisValue value, long sequenceId)
        {
            var container = _serializationManager.DeserializeFromByteArray<RedisBatchContainer>(value);
            container.RealSequenceToken = new EventSequenceTokenV2(sequenceId);
            return container;
        }

        public void OnDeserialized(ISerializerContext context)
        {
            _serializationManager = new OrleansSerializationManager(context.GetSerializationManager());
        }
    }
}
