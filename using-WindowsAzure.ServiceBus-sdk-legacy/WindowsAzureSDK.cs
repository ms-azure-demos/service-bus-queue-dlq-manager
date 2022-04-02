using System;
using System.Threading.Tasks;
using System.Transactions;
using Microsoft.ServiceBus.Messaging;

namespace DeadLetterProcessor
{
    class WindowsAzureSDK
    {
        internal async static Task ReSubmitDeadLetterMessages(string connectionString, string queueName, int count)
        {
            var dlqReceiverQueueClient = QueueClient.CreateFromConnectionString(connectionString,
               QueueClient.FormatDeadLetterPath(queueName), 
               ReceiveMode.PeekLock);
            var resubmitterQueueClient = QueueClient.CreateFromConnectionString(connectionString, queueName);
           
            BrokeredMessage originalMessage;
            int processed = 0;
            do
            {
                originalMessage = await dlqReceiverQueueClient.ReceiveAsync();
                Console.WriteLine($"Message received from deadletter queue. SequenceNo:{originalMessage.SequenceNumber}");
                if (originalMessage != null)
                {
                    // Transaction scope is not supported in basic tier.
                    //using (var scope = new TransactionScope(TransactionScopeAsyncFlowOption.Enabled))
                    {
                        // Clone received message
                        var resubmittableMessage = originalMessage.Clone();

                        resubmittableMessage.Properties.Remove("DeadLetterReason");
                        resubmittableMessage.Properties.Remove("DeadLetterErrorDescription");

                        // Resend cloned DLQ message and complete original DLQ message
                        resubmitterQueueClient.Send(resubmittableMessage);
                        Console.WriteLine($"OriginalMessage {originalMessage.SequenceNumber} resumitted");
                        // Remove the message from DLQ
                        originalMessage.Complete();

                        // Complete transaction 
                        //scope.Complete();
                    }
                    processed++;
                }
                else
                {
                    Console.WriteLine("Not dequeued");
                }
            } while (originalMessage != null && processed < count);
        }
        internal async static Task DequeueMessage(string connectionString, string queueName, int count)
        {
            BrokeredMessage originalMessage;
            var queueClient = QueueClient.CreateFromConnectionString(connectionString, queueName, ReceiveMode.PeekLock);

            int processed = 0;
            do
            {
                originalMessage = await queueClient.ReceiveAsync();
                Console.WriteLine($"Message received from queue. SequenceNo:{originalMessage.SequenceNumber}");
                await originalMessage.CompleteAsync();
                processed++;
            } while (originalMessage != null && processed < count);
        }

        internal async static Task SimulateFailure(string connectionString, string queueName)
        {
            BrokeredMessage originalMessage;
            var queue = QueueClient.CreateFromConnectionString(connectionString, queueName, ReceiveMode.PeekLock);

            originalMessage = await queue.ReceiveAsync();
            throw new Exception("Forceful failure");
        }
    }
}
