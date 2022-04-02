using System;
using System.Transactions;
using Microsoft.ServiceBus.Messaging;

namespace DeadLetterProcessor
{
    class WindowsAzureSDK
    {
        internal static void DequeueMessage(string connectionString, string queueName, int count)
        {
            var client = QueueClient.CreateFromConnectionString(connectionString, queueName);
            BrokeredMessage originalMessage;
            var queue = QueueClient.CreateFromConnectionString(connectionString, queueName, ReceiveMode.PeekLock);

            int processed = 0;
            do
            {
                originalMessage = queue.Receive();
                Console.WriteLine($"Message received from queue. SequenceNo:{originalMessage.SequenceNumber}");
                originalMessage.Complete();
                processed++;
            } while (originalMessage != null && processed < count);
        }
        internal static void ReSubmitDeadLetterMessages(string connectionString, string queueName, int count)
        {
            var client = QueueClient.CreateFromConnectionString(connectionString, queueName);
            BrokeredMessage originalMessage;
            var queue = QueueClient.CreateFromConnectionString(connectionString,
                QueueClient.FormatDeadLetterPath(queueName), ReceiveMode.PeekLock);

            int processed = 0;
            do
            {
                originalMessage = queue.Receive();
                Console.WriteLine($"Message received from deadletter queue. SequenceNo:{originalMessage.SequenceNumber}");
                if (originalMessage != null)
                {
                    using (var scope = new TransactionScope(TransactionScopeAsyncFlowOption.Enabled))
                    {
                        // Clone received message
                        var resubmittableMessage = originalMessage.Clone();

                        resubmittableMessage.Properties.Remove("DeadLetterReason");
                        resubmittableMessage.Properties.Remove("DeadLetterErrorDescription");

                        // Resend cloned DLQ message and complete original DLQ message
                        client.Send(resubmittableMessage);
                        Console.WriteLine($"OriginalMessage {originalMessage.SequenceNumber} resumitted");
                        // Remove the message from DLQ
                        originalMessage.Complete();

                        // Complete transaction 
                        scope.Complete();
                    }
                }
                processed++;
            } while (originalMessage != null && processed < count);
            Console.WriteLine($"Completed processing {processed}");
        }

        internal static QueueClient SimulateFailure(string connectionString, string queueName)
        {
            BrokeredMessage originalMessage;
            var queue = QueueClient.CreateFromConnectionString(connectionString, queueName, ReceiveMode.PeekLock);

            originalMessage = queue.Receive();
            throw new Exception("Forceful failure");
            return queue;
        }
    }
}
