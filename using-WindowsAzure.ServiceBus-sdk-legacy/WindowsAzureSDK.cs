using System;
using System.Linq;
using System.Threading.Tasks;
using System.Transactions;
using EasyConsole;
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
            foreach (var item in Enumerable.Range(0, count))
            {
                originalMessage = await dlqReceiverQueueClient.ReceiveAsync();
                if (originalMessage == null)
                {
                    Output.WriteLine(ConsoleColor.Yellow, "No message dequeued from DLQ"); break;
                }
                else
                {
                    Console.WriteLine($"{nameof(WindowsAzureSDK)} - Message received from DLQ. SequenceNo:{originalMessage.SequenceNumber}");
                    // Transaction scope is not supported in basic tier.
                    //using (var scope = new TransactionScope(TransactionScopeAsyncFlowOption.Enabled))
                    {
                        // Clone received message
                        await ResubmitMessage(resubmitterQueueClient, originalMessage);
                        await originalMessage.CompleteAsync();
                        Output.WriteLine("Removed message from DLQ");
                        //scope.Complete();
                    }
                }
            }
        }
        private static async Task ResubmitMessage(QueueClient resubmitterQueueClient, BrokeredMessage originalMessage)
        {
            var resubmittableMessage = originalMessage.Clone();

            resubmittableMessage.Properties.Remove("DeadLetterReason");
            resubmittableMessage.Properties.Remove("DeadLetterErrorDescription");

            // Resend cloned DLQ message and complete original DLQ message
            await resubmitterQueueClient.SendAsync(resubmittableMessage);
            Output.WriteLine(ConsoleColor.Green, $"Resumitted message. Sequence: {originalMessage.SequenceNumber} ");
        }

        internal async static Task DequeueMessage(string connectionString, string queueName, int count)
        {
            var queueClient = QueueClient.CreateFromConnectionString(connectionString, queueName, ReceiveMode.PeekLock);

            foreach (var item in Enumerable.Range(0, count))
            {
                BrokeredMessage originalMessage = await queueClient.ReceiveAsync();
                if (originalMessage == null)
                {
                    Output.WriteLine(ConsoleColor.Yellow, "No message dequeued."); break;
                }
                else
                {
                    await originalMessage.CompleteAsync();
                    Console.WriteLine($"Message received from queue. SequenceNo:{originalMessage.SequenceNumber}");
                }
            }
        }

        internal async static Task SimulateFailure(string connectionString, string queueName)
        {
            BrokeredMessage originalMessage;
            var queue = QueueClient.CreateFromConnectionString(connectionString, queueName, ReceiveMode.PeekLock);

            originalMessage = await queue.ReceiveAsync();
            if (originalMessage == null)
            {
                Output.WriteLine(ConsoleColor.Yellow, "No message dequeued.");
            }
            else
                throw new Exception("Forceful failure");
        }
    }
}
