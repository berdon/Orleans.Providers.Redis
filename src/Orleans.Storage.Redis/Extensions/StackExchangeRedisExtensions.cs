using Orleans.Redis.Common;
using StackExchange.Redis;
using System;
using System.Threading.Tasks;

namespace Orleans.Persistence.Redis.Extensions
{
    internal static class StackExchangeRedisExtension
    {
        #region String helpers
        public static async Task<T> GetObjectAsync<T>(this IDatabase redisClient, ISerializationManager serializationManager, string objectId)
        {
            var value = await redisClient.StringGetAsync(CreateUrn<T>(objectId));
            if (value.HasValue)
            {
                //Do special things here to handle versions in JSON.
                return serializationManager.DeserializeFromByteArray<T>(value);
            }

            return default;
        }

        public static async Task<object> GetObjectAsync(this IDatabase redisClient, ISerializationManager serializationManager, string objectId, Type type)
        {
            var value = await redisClient.StringGetAsync(CreateUrn(objectId, type));
            if (value.HasValue)
            {
                //Do special things here to handle versions in JSON.
                return serializationManager.DeserializeFromByteArray(type, value);
            }

            return default;
        }

        public static async Task StoreObjectAsync<T>(this IDatabase redisClient, ISerializationManager serializationManager, T value, string objectId, DateTime? expireTime = null)
        {
            string key = CreateUrn<T>(objectId);
            var data = serializationManager.SerializeToByteArray(value);
            await redisClient.StringSetAsync(key, data);
            if (expireTime.HasValue)
            {
                await redisClient.KeyExpireAsync(key, expireTime);
            }
        }

        public static async Task StoreObjectAsync(this IDatabase redisClient, ISerializationManager serializationManager, object value, Type type, string objectId, DateTime? expireTime = null)
        {
            string key = CreateUrn(objectId, type);
            var data = serializationManager.SerializeToByteArray(value);
            await redisClient.StringSetAsync(key, data);
            if (expireTime.HasValue)
            {
                await redisClient.KeyExpireAsync(key, expireTime);
            }
        }

        public static async Task DeleteObjectAsync<T>(this IDatabase redisClient, string objectId)
        {
            await redisClient.KeyDeleteAsync(CreateUrn<T>(objectId));
        }

        public static async Task DeleteObjectAsync(this IDatabase redisClient, Type type, string objectId)
        {
            await redisClient.KeyDeleteAsync(CreateUrn(objectId, type));
        }

        #endregion

        public static string CreateUrn<T>(string id)
        {
            return $"urn:{typeof(T).Name.ToLowerInvariant()}:{id}";
        }

        public static string CreateUrn(string id, Type type)
        {
            return $"urn:{type.Name.ToLowerInvariant()}:{id}";
        }
    }
}
