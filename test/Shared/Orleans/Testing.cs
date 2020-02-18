using System.Threading;

namespace Shared.Orleans
{
    public static class Testing
    {
        private static int _testIndex = 0;
        public static int TestIndex
        {
            get => Interlocked.Increment(ref _testIndex);
        }
    }
}