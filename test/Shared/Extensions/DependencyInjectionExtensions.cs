using Microsoft.Extensions.DependencyInjection;
using System;
using Xunit;

namespace CoreTests.Extensions
{
    public static class DependencyInjectionExtensions
    {
        public static void AssertRegistered<TService>(this IServiceProvider provider)
        {
            provider.GetRequiredService<TService>();
        }

        public static void AssertRegistered<TService>(this IServiceProvider provider, TService instance)
            where TService : class
        {
            var service = provider.GetRequiredService<TService>();
            Assert.Same(instance, service);
        }
    }
}
