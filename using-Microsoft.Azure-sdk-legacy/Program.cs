using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using System.Transactions;

namespace DeadLetterProcessor
{
    internal class Program
    {
        async static Task Main(string[] args)
        {
            var option = int.Parse(args[0]);
            var count = int.Parse(args[1]);
            var queueName = args[2];

            var connectionString = args[3];

            Console.WriteLine($"Option:{option}, Count: {count}, queueName:{queueName}, connString:{connectionString}");

            switch (option)
            {
                case 0:
                    await MicrosoftAzureSDK.SimulateFailure(connectionString, queueName);
                    return;
                case 1:
                    await MicrosoftAzureSDK.DequeueMessage(connectionString, queueName, count);
                    break;

                case 2:
                    await MicrosoftAzureSDK.ReSubmitDeadLetterMessages(connectionString, queueName, count);
                    break;
            }
            Console.WriteLine("Press any key to exit...");
            Console.ReadLine();
        }
        
    }
}
