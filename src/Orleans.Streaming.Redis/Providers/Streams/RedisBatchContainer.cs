using Newtonsoft.Json;
using Orleans.Providers.Streams.Common;
using Orleans.Runtime;
using Orleans.Streams;
using System;
using System.Collections.Generic;
using System.Linq;

namespace Orleans.Providers.Streams.Redis
{
    [Serializable]
    public class RedisBatchContainer : IBatchContainer
    {
        [JsonProperty]
        private EventSequenceTokenV2 _sequenceToken;

        [JsonProperty]
        private readonly List<object> _events;

        [JsonProperty]
        private readonly Dictionary<string, object> _requestContext;

        public Guid StreamGuid { get; }

        public string StreamNamespace { get; }

        public StreamSequenceToken SequenceToken => _sequenceToken;

        internal EventSequenceTokenV2 RealSequenceToken
        {
            set { _sequenceToken = value; }
        }

        [JsonConstructor]
        public RedisBatchContainer(
            Guid streamGuid,
            string streamNamespace,
            List<object> events,
            Dictionary<string, object> requestContext,
            EventSequenceTokenV2 sequenceToken)
            : this(streamGuid, streamNamespace, events, requestContext)
        {
            _sequenceToken = sequenceToken;
        }

        public RedisBatchContainer(
            Guid streamGuid,
            string streamNamespace,
            List<object> events,
            Dictionary<string, object> requestContext)
        {
            if (events == null) throw new ArgumentNullException(nameof(events), "Message contains no events");

            StreamGuid = streamGuid;
            StreamNamespace = streamNamespace;
            _events = events;
            _requestContext = requestContext;
        }

        public IEnumerable<Tuple<T, StreamSequenceToken>> GetEvents<T>()
        {
            return _events.OfType<T>().Select((e, i) => Tuple.Create<T, StreamSequenceToken>(e, _sequenceToken.CreateSequenceTokenForEvent(i)));
        }

        public bool ShouldDeliver(IStreamIdentity stream, object filterData, StreamFilterPredicate shouldReceiveFunc)
        {
            foreach (object item in _events)
            {
                if (shouldReceiveFunc(stream, filterData, item))
                {
                    // There is something in this batch that the consumer is interested in, so we should send it.
                    return true;
                }
            }

            // Consumer is not interested in any of these events, so don't send.
            return false;
        }

        public bool ImportRequestContext()
        {
            if (_requestContext != null)
            {
                RequestContextExtensions.Import(_requestContext);
                return true;
            }

            return false;
        }

        public override string ToString()
        {
            return $"[RedisBatchContainer:Stream={StreamGuid},#Items={_events.Count}]";
        }
    }
}
