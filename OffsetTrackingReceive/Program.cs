using RabbitMQ.Stream.Client.Reliable;
using RabbitMQ.Stream.Client;
using System.IO;
using System.Text;
using System.Collections.Generic;
using System.Drawing;

var streamSystem = await StreamSystem.Create(new StreamSystemConfig());
var stream = "stream-offset-tracking-dotnet";

//IOffsetType offsetSpecification = new OffsetTypeNext();
//ulong initialValue = UInt64.MaxValue;
//ulong firstOffset = initialValue;
//ulong lastOffset = initialValue;
//var consumedCde = new CountdownEvent(1);
//var consumer = await Consumer.Create(new ConsumerConfig(streamSystem, stream)
//{
//    OffsetSpec = offsetSpecification,
//    MessageHandler = async (_, consumer, context, message) => {
//        if (Interlocked.CompareExchange(ref firstOffset, context.Offset, initialValue) == initialValue)
//        {
//            Console.WriteLine("First message received.");
//        }
//        if ("marker".Equals(Encoding.UTF8.GetString(message.Data.Contents)))
//        {
//            Interlocked.Exchange(ref lastOffset, context.Offset);
//            await consumer.Close();
//            consumedCde.Signal();
//        }
//        await Task.CompletedTask;
//    }
//});
//Console.WriteLine("Started consuming...");

//consumedCde.Wait();
//Console.WriteLine("Done consuming, first offset {0}, last offset {1}.", firstOffset, lastOffset);
//await streamSystem.Close();

var consumerName = "offset-tracking-tutorial1"; // name of the consumer
IOffsetType offsetSpecification;
try
{
    // get last stored offset
    ulong storedOffset = await streamSystem.QueryOffset(consumerName, stream).ConfigureAwait(false);
    // start just after the last stored offset
    offsetSpecification = new OffsetTypeOffset(storedOffset + 1);
}
catch (OffsetNotFoundException)
{
    // start consuming at the beginning of the stream if no stored offset
    offsetSpecification = new OffsetTypeFirst();
    ulong initialValue = UInt64.MaxValue;
    ulong firstOffset = initialValue;
    int messageCount = 0; // number of received messages
    ulong lastOffset = initialValue;
    var consumedCde = new CountdownEvent(1);
    var consumer = await Consumer.Create(new ConsumerConfig(streamSystem, stream)
    {
        OffsetSpec = offsetSpecification,
        Reference = consumerName,  // the consumer must a have name
        MessageHandler = async (_, consumer, context, message) =>
        {
            // The method compares firstOffset with initialValue.
            // If firstOffset is equal to initialValue, it sets firstOffset to context.Offset and returns the original value of firstOffset.
            // If firstOffset is not equal to initialValue, it does nothing and returns the current value of firstOffset.
            if (Interlocked.CompareExchange(ref firstOffset, context.Offset, initialValue) == initialValue)
            {
                Console.WriteLine("First message received.");
            }
            // The if statement checks if the incremented messageCount is divisible by 10 (messageCount % 10 == 0).
            // If the condition is true, it means that 10 messages have been processed since the last time this condition was true.
            if (Interlocked.Increment(ref messageCount) % 10 == 0)
            {
                Console.WriteLine("message: ", Encoding.UTF8.GetString(message.Data.Contents));
                // store offset every 10 messages
                // This stores the current offset (context.Offset) in a persistent storage,
                // ensuring that the consumer can resume from this point in case of a restart or failure.
                await consumer.StoreOffset(context.Offset).ConfigureAwait(false);
            }
            if ("marker".Equals(Encoding.UTF8.GetString(message.Data.Contents)))
            {


                // This method atomically sets the value of lastOffset to context.Offset and returns the original value of lastOffset.
                // ref lastOffset: The variable to be updated.	context.Offset: The new value to set.
                Interlocked.Exchange(ref lastOffset, context.Offset);
                // store the offset on consumer closing
                // await consumer.StoreOffset(context.Offset).ConfigureAwait(false); stores the current offset
                // (context.Offset) in persistent storage.
                // This ensures that the consumer can resume from this point in case of a restart or failure.
                await consumer.StoreOffset(context.Offset).ConfigureAwait(false);
                await consumer.Close();
                // consumedCde.Signal(); signals the CountdownEvent to indicate that the consumer has finished processing.
                // This is used to synchronize the completion of message processing.
                consumedCde.Signal();
            }
            await Task.CompletedTask;
        }
    });
    Console.WriteLine("Started consuming...");

    consumedCde.Wait();
    Console.WriteLine("Done consuming, first offset {0}, last offset {1}.", firstOffset, lastOffset);
    await streamSystem.Close();
    }