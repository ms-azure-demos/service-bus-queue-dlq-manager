using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using Microsoft.ServiceBus.Messaging;
using System.Transactions;

namespace DeadLetterProcessor
{
    internal class Program
    {

        static void Main(string[] args)
        {
            var connectionString = args[3];
            var queueName = args[2];
            var count = int.Parse(args[1]);
            var option = int.Parse(args[0]);
            Console.WriteLine($"Option:{option}, Count: {count}, queueName:{queueName}, connString:{connectionString}");

            switch (option)
            {
                case 0:
                    SimulateFailure(connectionString, queueName);
                    return;
                case 1:
                    OldDK.DequeueMessage(connectionString, queueName, count);
                    break;

                case 2:

                    //ReSubmitDeadLetterMessageUsingAzureSDK(connectionString, queueName, count);
                    MicrosoftAzureSDK.ReSubmitDeadLetterMessageUsingMicrosoftAzureSDK(connectionString, queueName, count);
                    //ReSubmitDeadLetterMessage(connectionString, queueName, count);
                    break;
            }
            Console.ReadLine();
        }
        
        private static void ReSubmitDeadLetterMessage(string connectionString, string queueName, int count)
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
                        // Create new message
                        var resubmittableMessage = originalMessage.Clone();

                        // Remove dead letter reason and description
                        resubmittableMessage.Properties.Remove("DeadLetterReason");
                        resubmittableMessage.Properties.Remove("DeadLetterErrorDescription");
                        
                        // Resend cloned DLQ message and complete original DLQ message
                        client.Send(resubmittableMessage);
                        Console.WriteLine($"OriginalMessage {originalMessage.SequenceNumber} resumitted");
                        originalMessage.Complete();

                        // Complete transaction
                        scope.Complete();
                    }
                }
                processed++;
            } while (originalMessage != null && processed < count);
            Console.WriteLine($"Completed processing {processed}");
        }

        private static QueueClient SimulateFailure(string connectionString, string queueName)
        {
            BrokeredMessage originalMessage;
            var queue = QueueClient.CreateFromConnectionString(connectionString, queueName, ReceiveMode.PeekLock);

            originalMessage = queue.Receive();
            throw new Exception("Forceful failure");
            return queue;
        }
    }
}
