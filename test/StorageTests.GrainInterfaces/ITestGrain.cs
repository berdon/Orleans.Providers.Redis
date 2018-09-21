using Orleans;
using System;
using System.Threading.Tasks;

namespace StorageTests.GrainInterfaces
{
    public interface ITestGrain : IGrainWithGuidKey
    {
        Task Subscribe();
        Task Unsubscribe();
        Task Deactivate();
    }
}
