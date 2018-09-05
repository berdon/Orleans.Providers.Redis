using Orleans.Redis.Common;
using StackExchange.Redis;
using System;

namespace Orleans.Persistence.Redis.Extensions
{
    internal static class StackExchangeRedisExtension
    {
        #region String helpers
        public static T GetObject<T>(this IDatabase redisClient, ISerializationManager serializationManager, string objectId)
        {
            var value = redisClient.StringGet(CreateUrn<T>(objectId));
            if (value.HasValue)
            {
                //Do special things here to handle versions in JSON.
                return serializationManager.DeserializeFromByteArray<T>(value);
            }

            return default;
        }

        public static object GetObject(this IDatabase redisClient, ISerializationManager serializationManager, string objectId, Type type)
        {
            var value = redisClient.StringGet(CreateUrn(objectId, type));
            if (value.HasValue)
            {
                //Do special things here to handle versions in JSON.
                return serializationManager.DeserializeFromByteArray(type, value);
            }

            return default;
        }

        public static void StoreObject<T>(this IDatabase redisClient, ISerializationManager serializationManager, T value, string objectId, DateTime? expireTime = null)
        {
            string key = CreateUrn<T>(objectId);
            var data = serializationManager.SerializeToByteArray(value);
            redisClient.StringSet(key, data);
            if (expireTime.HasValue)
            {
                redisClient.KeyExpire(key, expireTime);
            }
        }

        public static void StoreObject(this IDatabase redisClient, ISerializationManager serializationManager, object value, Type type, string objectId, DateTime? expireTime = null)
        {
            string key = CreateUrn(objectId, type);
            var data = serializationManager.SerializeToByteArray(value);
            redisClient.StringSet(key, data);
            if (expireTime.HasValue)
            {
                redisClient.KeyExpire(key, expireTime);
            }
        }

        public static void DeleteObject<T>(this IDatabase redisClient, string objectId)
        {
            redisClient.KeyDelete(CreateUrn<T>(objectId));
        }

        public static void DeleteObject(this IDatabase redisClient, Type type, string objectId)
        {
            redisClient.KeyDelete(CreateUrn(objectId, type));
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
