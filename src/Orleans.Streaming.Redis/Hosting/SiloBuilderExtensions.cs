using Orleans.Configuration;
using Orleans.Hosting;
using Orleans.Streaming;
using System;
using System.Collections.Generic;
using System.Text;

namespace Orleans.Hosting
{
    public static class SiloBuilderExtensions
    {
        /// <summary>
        /// Configure silo to use redis queue persistent streams. 
        /// </summary>
        public static ISiloHostBuilder AddRedisStreams(this ISiloHostBuilder builder, string name, Action<SiloRedisStreamConfigurator> configure)
        {
            var configurator = new SiloRedisStreamConfigurator(name, 
                configureServicesDelegate => builder.ConfigureServices(configureServicesDelegate),
                configureAppPartsDelegate => builder.ConfigureApplicationParts(configureAppPartsDelegate));
            configure?.Invoke(configurator);
            return builder;
        }
    }
}
