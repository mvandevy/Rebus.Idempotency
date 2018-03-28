using System;
using System.Linq;
using System.Threading.Tasks;
using Rebus.Bus;
using Rebus.Logging;
using Rebus.Messages;
using Rebus.Pipeline;
using Rebus.Transport;
using System.Threading;

namespace Rebus.Idempotency
{
    [StepDocumentation("Controls whether a message has already been processed before processing it again.")]
    public class IdempotentMessageIncomingStep : IIncomingStep
    {
        private readonly ITransport _transport;
        private readonly IMessageStorage _msgStorage;
        private readonly ILog _log;

        /// <summary>
        /// Constructs the step
        /// </summary>
        public IdempotentMessageIncomingStep(ITransport transport, IMessageStorage messageStorage, IRebusLoggerFactory rebusLoggerFactory)
        {
            if (transport == null) throw new ArgumentNullException(nameof(transport));
            if (messageStorage == null) throw new ArgumentNullException(nameof(messageStorage));
            if (rebusLoggerFactory == null) throw new ArgumentNullException(nameof(rebusLoggerFactory));
            _transport = transport;
            _msgStorage = messageStorage;
            _log = rebusLoggerFactory.GetLogger<IdempotentMessageIncomingStep>();
        }

        /// <summary>
        /// Checks the loaded saga data to see if the message currently being handled is a duplicate of a message that has previously been handled. 
        /// If that is the case, message dispatch is skipped, but any messages stored as outgoing messages from previously handling the incoming message will be sent.
        /// </summary>
        public async Task Process(IncomingStepContext context, Func<Task> next)
        {
            var message = context.Load<Message>();
            var messageId = message.GetMessageId();

            var transactionContext = context.Load<ITransactionContext>();

            object temp;
            if (transactionContext.Items.TryGetValue(Keys.MessageData, out temp))
            {
                _log.Info($"Checking if message with ID {messageId} has already been processed before.");

                var messageData = (MessageData) temp;
                if (messageData.HasAlreadyHandled(messageId))
                {
                    _log.Warn($"Message with ID {messageId} has already been handled");

                    var outgoingMessages = messageData
                        .GetOutgoingMessages()
                        .ToList();

                    if (outgoingMessages.Any())
                    {
                        _log.Info("Found {0} outgoing messages to be (re-)sent... will do that now",
                            outgoingMessages.Count);

                        foreach (var messageToResend in outgoingMessages)
                        {
                            foreach (var destinationAddress in messageToResend.DestinationAddresses)
                            {
                                await
                                    _transport.Send(destinationAddress, messageToResend.TransportMessage,
                                        transactionContext);
                            }
                        }
                    }
                    else
                    {
                        _log.Info("Found no outgoing messages to be (re-)sent...");
                    }
                }
                else
                {
                    _log.Info($"Message with ID {messageId} has not been handled yet.");
                    if (await _msgStorage.IsProcessing(messageId)) // does the message have an assigned thread id?
                    {
                        _log.Warn($"Message with ID {messageId} is ignored as it already is being processed.");
                        messageData.MarkMessageAsDuplicate();
                        var waitTask = Task.Delay(5000);
                        Task.WaitAll(waitTask);
                        // ignore the message or maybe return as in 'Assigned' state
                        // todo: should we retry once a message has been assigned for too long? What is too long? By default 5m?
                        return;
                    }

                    var threadId = Thread.CurrentThread.ManagedThreadId;
                    _log.Info($"Updating message storage for the message with ID {messageId} as being processed by thread {threadId}");

                    // insert the message or update it with the current input queue address, the current thread id and the current date
                    messageData.InputQueueAddress = _transport.Address;
                    messageData.ProcessingThreadId = threadId;
                    messageData.TimeThreadIdAssigned = DateTime.UtcNow;
                    await _msgStorage.InsertOrUpdate(messageData);

                    // hand the message of to the next pipeline step
                    await next();

                    _log.Info($"Marking the message with ID {messageId} as been handled.");
                    messageData.MarkMessageAsHandled();
                }
            }
            else
            {
                // The LoadMessageDataStep was not triggered 
                await next();
            }
        }
    }
}
