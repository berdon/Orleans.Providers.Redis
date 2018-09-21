using Serilog;
using System;
using System.Collections.Generic;
using System.Text;

namespace Orleans.Redis.Common
{
    public class SilentLogger
    {
        public static readonly ILogger Logger = new LoggerConfiguration().CreateLogger();
    }
}
