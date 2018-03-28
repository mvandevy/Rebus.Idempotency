using System;
using System.Threading.Tasks;
using Rebus.Bus;
using Rebus.Logging;
using Rebus.Messages;
using Rebus.Pipeline;
using Rebus.Transport;

namespace Rebus.Idempotency
{
    [StepDocumentation("Initializes the transactioncontext with the message data available.")]
    public class LoadMessageDataStep : IIncomingStep
    {
        private readonly IMessageStorage _msgStorage;
        private readonly ILog _log;

        /// <summary>
        /// Constructs the step with the given saga storage
        /// </summary>
        public LoadMessageDataStep(IMessageStorage messageStorage, ITransport transport, IRebusLoggerFactory rebusLoggerFactory)
        {
            if (messageStorage == null) throw new ArgumentNullException(nameof(messageStorage));
            if (rebusLoggerFactory == null) throw new ArgumentNullException(nameof(rebusLoggerFactory));
            _msgStorage = messageStorage;
            _log = rebusLoggerFactory.GetLogger<LoadMessageDataStep>();
        }

        public async Task Process(IncomingStepContext context, Func<Task> next)
        {
            var message = context.Load<Message>();
            var messageId = message.GetMessageId();

            var transactionContext = context.Load<ITransactionContext>();
            var messageData = await _msgStorage.Find(messageId) ?? new MessageData() { MessageId = messageId };

            TryMountMessageDataOnTransactionContext(messageData, transactionContext);
            try
            {
                // invoke the rest of the pipeline (most likely also dispatching the incoming message to the now-ready saga handlers)
                await next();
            }
            catch (DuplicateProcessingException)
            {
                // here we do nothing since we want the duplicate to be thrown.
                throw;
            }
            catch (Exception)
            {
                // here we will clear out the processing thread id so we can give it another try to process without being treated as a duplicate.
                messageData.ProcessingThreadId = null;
                throw;
            }
            finally
            {
                // everything went well - let's save message data
                await SaveMessageData(messageData);
            }
        }

        private void TryMountMessageDataOnTransactionContext(MessageData messageData, ITransactionContext transactionContext)
        {
            if (messageData != null)
            {
                _log.Info($"Mounting message data for message with ID {messageData.MessageId} onto the transactioncontext.");
            }

            transactionContext?.Items.AddOrUpdate(
                Keys.MessageData,
                messageData,
                (key, existingVal) => messageData);
        }

        private async Task SaveMessageData(MessageData messageData)
        {
            if (messageData != null)
            {
                if (messageData.CanBeSaved())
                {
                    _log.Info($"Saving the message data for message with ID {messageData.MessageId} into the message store.");
                    await _msgStorage.InsertOrUpdate(messageData);
                }
                else
                {
                    _log.Warn($"Not saving the message data for message with ID {messageData.MessageId} since this is marked as a possible duplicate(canbesaved==false)");

                    // We must throw an exception here. If we don't throw an exception, the message will be marked as success and it will be removed from the queue, 
                    // so it will not longer be possible to add to the error queue. 
                    // When mark it as an error, the message will be reprocessed again. 
                    // When the message is completed as success, it will be in the idempotent messages which will make sure it is not handled twice. 
                    throw new DuplicateProcessingException("Throwing an exception to make sure, we don't mark this message as success");
                }
            }
        }
    }
}