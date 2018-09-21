using StackExchange.Redis;
using System.Threading.Tasks;

namespace Orleans.Redis.Common
{
    public interface IConnectionMultiplexerFactory
    {
        Task<IConnectionMultiplexer> CreateAsync(string configuration);
    }
}
