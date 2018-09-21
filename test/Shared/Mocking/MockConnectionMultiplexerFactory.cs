using Orleans.Redis.Common;
using StackExchange.Redis;
using System;
using System.Collections.Generic;
using System.Text;
using System.Threading.Tasks;

namespace Shared.Mocking
{
    public class MockConnectionMultiplexerFactory : IConnectionMultiplexerFactory
    {
        private readonly Func<string, Task<IConnectionMultiplexer>> _factoryDelegate;

        private MockConnectionMultiplexerFactory(Func<string, Task<IConnectionMultiplexer>> factoryDelegate)
        {
            _factoryDelegate = factoryDelegate;
        }

        public static MockConnectionMultiplexerFactory Returns(IConnectionMultiplexer connectionMultiplexer)
        {
            return new MockConnectionMultiplexerFactory((configuration) => Task.FromResult(connectionMultiplexer));
        }

        public static MockConnectionMultiplexerFactory Returns(Func<string, Task<IConnectionMultiplexer>> factoryDelegate)
        {
            return new MockConnectionMultiplexerFactory(factoryDelegate);
        }

        public async Task<IConnectionMultiplexer> CreateAsync(string configuration)
        {
            return await _factoryDelegate.Invoke(configuration);
        }
    }
}
