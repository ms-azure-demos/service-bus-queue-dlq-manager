using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using Microsoft.ServiceBus.Messaging;
using System.Transactions;
using System.Threading.Tasks;

namespace DeadLetterProcessor
{
    internal class Program
    {
        async static Task Main(string[] args)
        {
            var connectionString = args[3];
            var queueName = args[2];
            var count = int.Parse(args[1]);
            var option = int.Parse(args[0]);
            Console.WriteLine($"Option:{option}, Count: {count}, queueName:{queueName}, connString:{connectionString}");

            switch (option)
            {
                case 0:
                    WindowsAzureSDK.SimulateFailure(connectionString, queueName);
                    return;
                case 1:
                    WindowsAzureSDK.DequeueMessage(connectionString, queueName, count);
                    break;
                case 2:
                    WindowsAzureSDK.ReSubmitDeadLetterMessages(connectionString, queueName, count);
                    break;
            }
            Console.WriteLine("Press any key to exit...");
            Console.ReadLine();
        }
    }
}
