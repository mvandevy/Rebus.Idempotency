using System;
using System.Collections.Concurrent;
using System.Threading.Tasks;
using Newtonsoft.Json;

namespace Rebus.Idempotency.Persistence
{
    public class InMemoryMessageStorage : IMessageStorage
    {
        readonly ConcurrentDictionary<string, MessageData> _data = new ConcurrentDictionary<string, MessageData>();
        readonly object _lock = new object();

        readonly JsonSerializerSettings _serializerSettings = new JsonSerializerSettings
        {
            TypeNameHandling = TypeNameHandling.All
        };

#pragma warning disable CS1998 // Async method lacks 'await' operators and will run synchronously
        public async Task<MessageData> Find(string messageId)
#pragma warning restore CS1998 // Async method lacks 'await' operators and will run synchronously
        {
            lock (_lock)
            {
                MessageData result = null;
                if (!string.IsNullOrWhiteSpace(messageId) && _data.ContainsKey(messageId))
                {
                    result = _data[messageId];
                }
                if (result == null)
                {
                    result = new MessageData() {MessageId = messageId};
                }
                return Clone(result);
            }
        }

#pragma warning disable CS1998 // Async method lacks 'await' operators and will run synchronously
        public async Task<bool> IsProcessing(string messageId)
#pragma warning restore CS1998 // Async method lacks 'await' operators and will run synchronously
        {
            lock (_lock)
            {
                if (!_data.ContainsKey(messageId))
                {
                    return false;
                }
                return (_data[messageId].ProcessingThreadId != null);
            }
        }

        private MessageData Clone(MessageData messageData)
        {
            var serializedObject = JsonConvert.SerializeObject(messageData, _serializerSettings);
            return JsonConvert.DeserializeObject<MessageData>(serializedObject, _serializerSettings);
        }

#pragma warning disable CS1998 // Async method lacks 'await' operators and will run synchronously
        public async Task InsertOrUpdate(MessageData messageData)
#pragma warning restore CS1998 // Async method lacks 'await' operators and will run synchronously
        {
            if (messageData == null)
            {
                return;
            }

            lock (_lock)
            {
                _data.AddOrUpdate(
                    messageData.MessageId,
                    messageData,
                    ((key, existingVal) => messageData));
            }
        }
    }
}
