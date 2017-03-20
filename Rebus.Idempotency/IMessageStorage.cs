using System.Threading.Tasks;

namespace Rebus.Idempotency
{
    /// <summary>
    /// Abstraction for a mechanism that is capable of storing message processing state, retrieving it again by querying by the message identifier
    /// </summary>
    public interface IMessageStorage
    {
        /// <summary>
        /// Finds an already-existing instance of the given idempotency data based on the <paramref name="messageId"/>.
        /// Returns null if no such instance could be found.
        /// </summary>
        Task<MessageData> Find(string messageId);

        /// <summary>
        /// Verifies whether no one else is currenlty processing the message
        /// </summary>
        Task<bool> IsProcessing(string messageId);

        Task InsertOrUpdate(MessageData messageData);
    }
}
