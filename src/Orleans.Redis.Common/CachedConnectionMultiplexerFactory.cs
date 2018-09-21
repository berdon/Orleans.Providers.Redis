using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;
using StackExchange.Redis;

namespace Orleans.Redis.Common
{
    /// <summary>
    /// Caches connections based on the configuration string and reuses them
    /// if able.
    /// </summary>
    public class CachedConnectionMultiplexerFactory : IConnectionMultiplexerFactory
    {
        public static IConnectionMultiplexerFactory Default = new CachedConnectionMultiplexerFactory();

        private readonly SemaphoreSlim _lock = new SemaphoreSlim(1, 1);
        private readonly Dictionary<string, IConnectionMultiplexer> _connectionMultiplexers = new Dictionary<string, IConnectionMultiplexer>();
        internal Dictionary<string, IConnectionMultiplexer> TestHook_ConnectionMultiplexers => _connectionMultiplexers;

        private CachedConnectionMultiplexerFactory() { }

        public async Task<IConnectionMultiplexer> CreateAsync(string configuration)
        {
            if (!_connectionMultiplexers.TryGetValue(configuration, out var connectionMultiplexer))
            {
                try
                {
                    await _lock.WaitAsync();

                    if (!_connectionMultiplexers.TryGetValue(configuration, out connectionMultiplexer))
                    {
                        connectionMultiplexer = await ConnectionMultiplexer.ConnectAsync(configuration);
                        _connectionMultiplexers.Add(configuration, connectionMultiplexer);
                    }
                }
                finally
                {
                    _lock.Release();
                }
            }

            return connectionMultiplexer;
        }
    }
}
