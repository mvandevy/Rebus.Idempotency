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

        public async Task<MessageData> Find(string messageId)
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

        public async Task<bool> IsProcessing(string messageId)
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

        public async Task InsertOrUpdate(MessageData messageData)
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
