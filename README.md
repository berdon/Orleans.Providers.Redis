# Orleans.Providers.Redis
Redis stream and storage providers for Microsoft Orleans.

# Installation
TODO *Add automated build/deployment to nuget*

For now, you'll need to build locally.

# Usage
```C#
// Silo
siloBuilder
    .AddRedisStreams("RedisProvider", // Add the Redis stream provider
        c => c.ConfigureRedis(options => options.ConnectionString = Startup.Configuration.Redis.ConnectionString))
    .AddRedisGrainStorage("PubSubStore", // Add the redis grain storage
        c => c.Configure(options => options.ConnectionString = Startup.Configuration.Redis.ConnectionString));

// Client
clientBuilder
    .AddRedisStreams("RedisProvider", // Add the Redis stream provider,
        c => c.ConfigureRedis(options => options.ConnectionString = redisConfiguration.ConnectionString));
```