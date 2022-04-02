using System;
using System.Linq;
using System.Threading.Tasks;
using EasyConsole;
using Microsoft.Azure.ServiceBus;

using Microsoft.Azure.ServiceBus.Core;
namespace DeadLetterProcessor
{
    class MicrosoftAzureSDK
    {
        internal async static Task ReSubmitDeadLetterMessages(string connectionString, string queueName, int count)
        {
            var dlqReceiver = new MessageReceiver(connectionString,
               EntityNameHelper.FormatDeadLetterPath(queueName),
               Microsoft.Azure.ServiceBus.ReceiveMode.PeekLock);
            dlqReceiver.ServiceBusConnection.TransportType = TransportType.AmqpWebSockets;
            var resubmitSender = new MessageSender(connectionString, queueName);
            resubmitSender.ServiceBusConnection.TransportType = TransportType.AmqpWebSockets;

            Message message;
            foreach (var item in Enumerable.Range(0, count))
            {
                message = await dlqReceiver.ReceiveAsync();
                if (message == null)
                {
                    Output.WriteLine(ConsoleColor.Yellow, "No message dequeued from DLQ"); break;
                }
                else
                {
                    Console.WriteLine($"{nameof(MicrosoftAzureSDK)} - Message Received from DLQ. SequenceNo :{message.MessageId}");
                    await ResubmitMessage(resubmitSender, message);

                    await dlqReceiver.CompleteAsync(message.SystemProperties.LockToken);
                    Output.WriteLine("Removed message from DLQ");
                }
            }
        }

        private static async Task ResubmitMessage(MessageSender resubmitSender, Message message)
        {
            // first, we create a clone of the picked up message
            // that we can resubmit. 
            var resubmittableMessage = message.Clone();
            // if the cloned message has an "error" we know the main loop
            // can't handle, let's fix the message
            await resubmitSender.SendAsync(resubmittableMessage);
            Output.WriteLine(ConsoleColor.Green, $"{nameof(MicrosoftAzureSDK)} - REsubmitted message. MessageId: {message.MessageId}");
        }

        internal async static Task DequeueMessage(string connectionString, string queueName, int count)
        {
            var receiver = new MessageReceiver(connectionString, queueName, ReceiveMode.PeekLock);
            foreach (var item in Enumerable.Range(0, count))
            {
                Message originalMessage = await receiver.ReceiveAsync();
                if (originalMessage == null)
                {
                    Output.WriteLine(ConsoleColor.Yellow, "No message dequeued."); break;
                }
                else
                {
                    await receiver.CompleteAsync(originalMessage.SystemProperties.LockToken);
                    Console.WriteLine($"Message received and completed from queue. SequenceNo:{originalMessage.MessageId}");
                }
            }
        }

        internal async static Task SimulateFailure(string connectionString, string queueName)
        {
            var dlqReceiver = new MessageReceiver(connectionString, 
                queueName, 
                ReceiveMode.PeekLock);

            var originalMessage = await dlqReceiver.ReceiveAsync();
            if (originalMessage == null)
            {
                Output.WriteLine(ConsoleColor.Yellow, "No message dequeued.");
            }
            else
                throw new Exception("Forceful failure");
        }
    }
}