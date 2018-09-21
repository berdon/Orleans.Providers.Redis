using StackExchange.Redis;
using System;
using System.Collections.Generic;
using System.Text;
using System.Threading.Tasks;

namespace Orleans.Redis.Common
{
    /// <summary>
    /// You probably don't want this factory implementation. This does not pool
    /// or attempt to reuse connection multiplexers.
    /// </summary>
    internal class ConnectionMultiplexerFactory : IConnectionMultiplexerFactory
    {
        public static ConnectionMultiplexerFactory Default = new ConnectionMultiplexerFactory();

        private ConnectionMultiplexerFactory() { }

        public async Task<IConnectionMultiplexer> CreateAsync(string configuration)
        {
            return await ConnectionMultiplexer.ConnectAsync(configuration);
        }
    }
}
