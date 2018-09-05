using Orleans.Serialization;
using System;
using System.Reflection;

namespace Orleans.Redis.Common
{
    public interface ISerializationManager
    {
        byte[] SerializeToByteArray(object raw);
        T DeserializeFromByteArray<T>(byte[] data);
        object DeserializeFromByteArray(Type type, byte[] data);
    }

    public class OrleansSerializationManager : ISerializationManager
    {
        private readonly SerializationManager _serializationManager;
        private readonly MethodInfo _deserializeByteArrayMethod;

        public OrleansSerializationManager(SerializationManager serializationManager)
        {
            _serializationManager = serializationManager;
            _deserializeByteArrayMethod = _serializationManager.GetType().GetMethod("DeserializeFromByteArray");
        }

        public T DeserializeFromByteArray<T>(byte[] data)
        {
            return _serializationManager.DeserializeFromByteArray<T>(data);
        }

        public object DeserializeFromByteArray(Type type, byte[] data)
        {
            var method = _deserializeByteArrayMethod.MakeGenericMethod(type);
            return method.Invoke(_serializationManager, new object[] { data });
        }

        public byte[] SerializeToByteArray(object raw)
        {
            return _serializationManager.SerializeToByteArray(raw);
        }
    }
}
