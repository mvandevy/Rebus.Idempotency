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

            var messageData = await _msgStorage.Find(messageId);
            var transactionContext = context.Load<ITransactionContext>();
            TryMountMessageDataOnTransactionContext(messageData, transactionContext);

            // invoke the rest of the pipeline (most likely also dispatching the incoming message to the now-ready saga handlers)
            await next();

            // everything went well - let's save message data
            await SaveMessageData(messageData);
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
                _log.Info($"Saving the message data for message with ID {messageData.MessageId} into the message store.");
            }
            await _msgStorage.InsertOrUpdate(messageData);
        }
    }
}
