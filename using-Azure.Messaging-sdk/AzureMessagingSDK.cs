using System;
using System.Linq;
using System.Threading.Tasks;
using Azure.Messaging.ServiceBus;
using EasyConsole;

namespace DeadLetterProcessor
{
    class AzureMessagingSDK
    {
        internal async static Task ReSubmitDeadLetterMessages(string connectionString, string queueName, int count)
        {
            ServiceBusClient client = GetServiceBusClient(connectionString);

            ServiceBusReceiver dlqReceiver = client.CreateReceiver(queueName,
                new ServiceBusReceiverOptions()
                {
                    SubQueue = SubQueue.DeadLetter,
                    ReceiveMode = ServiceBusReceiveMode.PeekLock
                });

            //message.clone
            // the sender used to publish messages to the topic
            ServiceBusSender resubmitSender = client.CreateSender(queueName);
            foreach (var item in Enumerable.Range(0, count))
            {
                ServiceBusReceivedMessage message = await dlqReceiver.ReceiveMessageAsync();
                if (message == null)
                {
                    Output.WriteLine(ConsoleColor.Yellow, "No message dequeued from DLQ"); break;
                }
                else
                {
                    Console.WriteLine($"{nameof(AzureMessagingSDK)} - Message Received from DLQ. SequenceNo :{message.SequenceNumber}");
                    await ResubmitMessage(resubmitSender, message);
                    await dlqReceiver.CompleteMessageAsync(message);
                    Output.WriteLine("Removed message from DLQ");
                }
            }
        }

        private static async Task ResubmitMessage(ServiceBusSender resubmitSender, ServiceBusReceivedMessage message)
        {
            var resubmittableMessage = new ServiceBusMessage(message);
            await resubmitSender.SendMessageAsync(resubmittableMessage);
            Output.WriteLine(ConsoleColor.Green, $"{nameof(AzureMessagingSDK)} - REsubmitted message. MessageId: {message.MessageId}");
        }

        internal async static Task DequeueMessage(string connectionString, string queueName, int count)
        {
            ServiceBusClient client = GetServiceBusClient(connectionString);

            ServiceBusReceiver receiver = client.CreateReceiver(queueName,
                new ServiceBusReceiverOptions() { ReceiveMode = ServiceBusReceiveMode.PeekLock });
            foreach (var item in Enumerable.Range(0, count))
            {
                var message = await receiver.ReceiveMessageAsync();
                if (message == null)
                {
                    Output.WriteLine(ConsoleColor.Yellow, "No message dequeued."); break;
                }
                else
                {
                    await receiver.CompleteMessageAsync(message);
                    Console.WriteLine($"Message received and completed from queue. SequenceNo:{message.SequenceNumber}");
                }
            }

        }
        internal async static Task SimulateFailure(string connectionString, string queueName)
        {
            ServiceBusClient client = GetServiceBusClient(connectionString);

            ServiceBusReceiver receiver = client.CreateReceiver(queueName,
                new ServiceBusReceiverOptions() { ReceiveMode = ServiceBusReceiveMode.PeekLock });
            var originalMessage = await receiver.ReceiveMessageAsync();
            if (originalMessage == null)
            {
                Output.WriteLine(ConsoleColor.Yellow, "No message dequeued.");
            }
            else
                throw new Exception("Forceful failure");
        }
        private static ServiceBusClient GetServiceBusClient(string connectionString)
        {
            ServiceBusClientOptions clientOption = new ServiceBusClientOptions();
            clientOption.TransportType = ServiceBusTransportType.AmqpWebSockets;
            // the client that owns the connection and can be used to create senders and receivers
            ServiceBusClient client = new ServiceBusClient(connectionString, clientOption);
            Console.WriteLine("ServiceBusClient created ...");
            return client;
        }
    }
}
