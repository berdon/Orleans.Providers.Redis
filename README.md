# Orleans.Providers.Redis
[![Build Status](https://img.shields.io/vso/build/berdon/Zuercher.Orleans.Providers.Redis/1.svg)](https://dev.azure.com/berdon/Zuercher.Orleans.Providers.Redis/_build?definitionId=1)

Redis stream and storage providers for Microsoft Orleans.

| Library  | Version |
| ------------- | ------------- |
| [Zuercher.Orleans.Persistence.Redis](https://www.nuget.org/packages/Zuercher.Orleans.Persistence.Redis/)  | [![NugGetVersion](https://img.shields.io/nuget/v/Zuercher.Orleans.Persistence.Redis.svg)](https://www.nuget.org/packages/Zuercher.Orleans.Persistence.Redis/) |
| [Zuercher.Orleans.Streaming.Redis](https://www.nuget.org/packages/Zuercher.Orleans.Streaming.Redis/) | [![NugGetVersion](https://img.shields.io/nuget/v/Zuercher.Orleans.Streaming.Redis.svg)](https://www.nuget.org/packages/Zuercher.Orleans.Streaming.Redis/) |


## Installation

```bash
# For redis storage
dotnet add package Zuercher.Orleans.Persistence.Redis

# For redis streams
dotnet add package Zuercher.Orleans.Streaming.Redis
```

## Usage
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

## Development

### Building

```bash
dotnet build
```

### Running Tests

```bash
./scripts/start-test-deps.sh
dotnet test
./scripts/stop-test-deps.sh
```