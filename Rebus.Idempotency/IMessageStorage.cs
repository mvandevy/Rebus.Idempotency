using System;
using System.Threading.Tasks;

namespace Rebus.Idempotency
{
    /// <summary>
    /// Abstraction for a mechanism that is capable of storing message processing state, retrieving it again by querying by the message identifier
    /// </summary>
    public interface IMessageStorage
    {
        /// <summary>
        /// Inserts a messageData object into the underlying storage, or just updates the existing item.
        /// </summary>
        /// <param name="messageData">The item to insert or update.</param>
        Task InsertOrUpdate(MessageData messageData);

        /// <summary>
        /// Finds an already-existing instance of the given idempotency data based on the <paramref name="messageId"/>.
        /// Returns null if no such instance could be found.
        /// </summary>
        Task<MessageData> Find(MessageId messageId);

        /// <summary>
        /// Verifies whether no one else is currently processing the message
        /// </summary>
        Task<bool> IsProcessing(MessageId messageId);

        /// <summary>
        /// Cleans the message store of item older than the specified parameter
        /// </summary>
        /// <returns></returns>
        Task Cleanup(TimeSpan olderThan);
    }
}
