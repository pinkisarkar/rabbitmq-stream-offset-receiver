using RabbitMQ.Stream.Client;
using RabbitMQ.Stream.Client.Reliable;
using System;
using System.Collections.Concurrent;
using System.Text;
using System.Threading;
using System.Threading.Tasks;

class RetryLogic
{
    static async Task Retry(string[] args)
    {
        var streamSystem = await StreamSystem.Create(new StreamSystemConfig());
        var stream = "stream-offset-tracking-dotnet";
        var consumerName = "offset-tracking-tutorial"; // Name of the consumer

        IOffsetType offsetSpecification;
        try
        {
            // Get last stored offset
            ulong storedOffset = await streamSystem.QueryOffset(consumerName, stream).ConfigureAwait(false);/// QueryOffset retrieves the last consumer offset stored
                                                                                                            /// given a consumer name and stream name 
            // Start just after the last stored offset
            offsetSpecification = new OffsetTypeOffset(storedOffset + 1);
        }
        catch (OffsetNotFoundException)
        {
            // Start consuming at the beginning of the stream if no stored offset
            offsetSpecification = new OffsetTypeFirst();
        }

        ulong initialValue = UInt64.MaxValue;
        ulong firstOffset = initialValue;
        int messageCount = 0; // Number of received messages
        ulong lastOffset = initialValue;

        // Initializes a new instance of System.Threading.CountdownEvent class with the specified count.
        // InitialCount: The number of signals initially required to set the System.Threading.CountdownEvent.
        var consumedCde = new CountdownEvent(1);

        // Retry-related data structures
        //A ConcurrentQueue is a data structure that represents a first-in-first-out (FIFO) queue of elements.
        //It allows multiple threads to add and remove elements from the queue concurrently without requiring synchronization.
        var retryQueue = new ConcurrentQueue<(ulong Offset, Message Message)>(); // Queue for failed messages

        //ConcurrentDictionary : remove a key-value pair in case the Key exists in the dictionary,
        //and in case the key does not exist, it will do nothing.
        var retryAttempts = new ConcurrentDictionary<ulong, int>(); // Retry count per offset

        var retryDelay = TimeSpan.FromSeconds(5); // Delay between retries
        var maxRetries = 3; // Max retries per message

        // Method to handle retries
        async Task RetryFailedMessages()
        {
            while (!consumedCde.IsSet) //Indicates whether the CountdownEvent object's current count has reached zero.
            {
                if (retryQueue.TryDequeue(out var retryItem))
                {
                    var retryOffset = retryItem.Offset;
                    var retryMessage = retryItem.Message;

                    if (!retryAttempts.ContainsKey(retryOffset))
                        retryAttempts[retryOffset] = 0;

                    retryAttempts[retryOffset]++;
                    try
                    {
                        Console.WriteLine($"Retrying message at offset {retryOffset}, attempt {retryAttempts[retryOffset]}");

                        // Simulate message processing (You can implement your own logic)
                        if (ProcessMessage(retryMessage))
                        {
                            Console.WriteLine($"Successfully processed message at offset {retryOffset} on retry attempt {retryAttempts[retryOffset]}.");
                            retryAttempts.TryRemove(retryOffset, out _); // Remove retry tracking after success
                        }
                        else
                        {
                            throw new Exception("Message processing failed");
                        }
                    }
                    catch (Exception ex)
                    {
                        Console.WriteLine($"Retry failed for message at offset {retryOffset}: {ex.Message}");
                        if (retryAttempts[retryOffset] < maxRetries)
                        {
                            // Add the offset back to the retry queue for further attempts
                            retryQueue.Enqueue(retryItem);
                        }
                        else
                        {
                            Console.WriteLine($"Max retries reached for offset {retryOffset}. Giving up.");
                        }
                    }

                    // Delay before next retry
                    await Task.Delay(retryDelay);
                }
            }
        }

        // Simulated message processing logic
        bool ProcessMessage(Message message)
        {
            string messageContent = Encoding.UTF8.GetString(message.Data.Contents);
            return !messageContent.Contains("fail"); // Simulate processing failure if message contains "fail"
        }

        // Consumer logic
        var consumer = await Consumer.Create(new ConsumerConfig(streamSystem, stream)
        {
            OffsetSpec = offsetSpecification,
            Reference = consumerName,  // The consumer must have a name
            MessageHandler = async (_, consumer, context, message) =>
            {
                if (Interlocked.CompareExchange(ref firstOffset, context.Offset, initialValue) == initialValue)
                {
                    Console.WriteLine("First message received.");
                }

                try
                {
                    // Simulate message processing
                    if (ProcessMessage(message))
                    {
                        Console.WriteLine($"Successfully processed message at offset {context.Offset}");
                    }
                    else
                    {
                        throw new Exception("Message processing failed");
                    }

                    // Store offset every 10 messages
                    if (Interlocked.Increment(ref messageCount) % 10 == 0)
                    {
                        await consumer.StoreOffset(context.Offset).ConfigureAwait(false);
                    }

                    if ("marker".Equals(Encoding.UTF8.GetString(message.Data.Contents)))
                    {
                        Interlocked.Exchange(ref lastOffset, context.Offset);
                        // Store the offset on consumer closing
                        await consumer.StoreOffset(context.Offset).ConfigureAwait(false);
                        await consumer.Close();
                        consumedCde.Signal();
                    }
                }
                catch (Exception ex)
                {
                    Console.WriteLine($"Message processing failed at offset {context.Offset}: {ex.Message}");
                    // Add the failed message to the retry queue
                    retryQueue.Enqueue((context.Offset, message));
                }

                await Task.CompletedTask;
            }
        });

        Console.WriteLine("Started consuming...");

        // Start retrying failed messages in a background task
        var retryTask = Task.Run(() => RetryFailedMessages());

        consumedCde.Wait(); // Wait for consumption to complete
        await retryTask; // Wait for retry task to complete
        Console.WriteLine("Done consuming, first offset {0}, last offset {1}.", firstOffset, lastOffset);
        await streamSystem.Close();
    }
}
