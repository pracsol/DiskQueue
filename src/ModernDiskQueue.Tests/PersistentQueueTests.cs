using ModernDiskQueue.Implementation;
using NUnit.Framework;
using System;
using System.Collections.Generic;
using System.IO;
// ReSharper disable AssignNullToNotNullAttribute

// ReSharper disable PossibleNullReferenceException

namespace ModernDiskQueue.Tests
{
    [TestFixture, SingleThreaded]
    public class PersistentQueueTests : PersistentQueueTestsBase
    {
        protected override string Path => "./PersistentQueueTests";

        [Test]
        public void Only_single_instance_of_queue_can_exists_at_any_one_time()
        {
            var invalidOperationException = Assert.Throws<InvalidOperationException>(() =>
            {
                using (new PersistentQueue(Path))
                {
                    // ReSharper disable once ObjectCreationAsStatement
                    new PersistentQueue(Path);
                }
            });

            Assert.That(invalidOperationException.Message, Is.EqualTo("Another instance of the queue is already in action, or directory does not exist"));
        }

        [Test]
        public void If_a_non_running_process_has_a_lock_then_can_start_an_instance()
        {
            Directory.CreateDirectory(Path);
            var lockFilePath = System.IO.Path.Combine(Path, "lock");
            File.WriteAllText(lockFilePath, "78924759045");

            using (new PersistentQueue(Path))
            {
                Assert.Pass();
            }
        }

        [Test]
        public void Can_create_new_queue()
        {
            new PersistentQueue(Path).Dispose();
        }

        [Test]
        public void Corrupt_index_file_should_throw()
        {
            PersistentQueue.DefaultSettings.AllowTruncatedEntries = false;
            var buffer = new List<byte>();
            buffer.AddRange(Guid.NewGuid().ToByteArray());
            buffer.AddRange(Guid.NewGuid().ToByteArray());
            buffer.AddRange(Guid.NewGuid().ToByteArray());

            Directory.CreateDirectory(Path);
            File.WriteAllBytes(System.IO.Path.Combine(Path, "transaction.log"), buffer.ToArray());

            var invalidOperationException = Assert.Throws<UnrecoverableException>(() =>
            {
                // ReSharper disable once ObjectCreationAsStatement
                new PersistentQueue(Path);
            });

            Assert.That(invalidOperationException.Message, Is.EqualTo("Unexpected data in transaction log. Expected to get transaction separator but got unknown data. Tx #1"));
        }

        [Test]
        public void Dequeueing_from_empty_queue_will_return_null()
        {
            using (var queue = new PersistentQueue(Path))
            using (var session = queue.OpenSession())
            {
                Assert.That(session.Dequeue(), Is.Null);
            }
        }

        [Test]
        public void Can_enqueue_data_in_queue()
        {
            using (var queue = new PersistentQueue(Path))
            using (var session = queue.OpenSession())
            {
                session.Enqueue(new byte[] { 1, 2, 3, 4 });
                session.Flush();
            }
        }

        [Test]
        public void Can_dequeue_data_from_queue()
        {
            using (var queue = new PersistentQueue(Path))
            using (var session = queue.OpenSession())
            {
                session.Enqueue(new byte[] { 1, 2, 3, 4 });
                session.Flush();
                Assert.That(session.Dequeue(), Is.EqualTo(new byte[] { 1, 2, 3, 4 }));
            }
        }

        [Test]
        public void Queueing_and_dequeueing_empty_data_is_handled()
        {
            using (var queue = new PersistentQueue(Path))
            using (var session = queue.OpenSession())
            {
                session.Enqueue(new byte[0]);
                session.Flush();
                Assert.That(session.Dequeue(), Is.EqualTo(Array.Empty<byte>()));
            }
        }

        [Test]
        public void Can_enqueue_and_dequeue_data_after_restarting_queue()
        {
            using (var queue = new PersistentQueue(Path))
            using (var session = queue.OpenSession())
            {
                session.Enqueue(new byte[] { 1, 2, 3, 4 });
                session.Flush();
            }

            using (var queue = new PersistentQueue(Path))
            using (var session = queue.OpenSession())
            {
                Assert.That(session.Dequeue(), Is.EqualTo(new byte[] { 1, 2, 3, 4 }));
                session.Flush();
            }
        }

        [Test]
        public void After_dequeue_from_queue_item_no_longer_on_queue()
        {
            using (var queue = new PersistentQueue(Path))
            using (var session = queue.OpenSession())
            {
                session.Enqueue(new byte[] { 1, 2, 3, 4 });
                session.Flush();
            }

            using (var queue = new PersistentQueue(Path))
            using (var session = queue.OpenSession())
            {
                Assert.That(session.Dequeue(), Is.EqualTo(new byte[] { 1, 2, 3, 4 }));
                Assert.That(session.Dequeue(), Is.Null);
                session.Flush();
            }
        }

        [Test]
        public void After_dequeue_from_queue_item_no_longer_on_queue_with_queues_restarts()
        {
            using (var queue = new PersistentQueue(Path))
            using (var session = queue.OpenSession())
            {
                session.Enqueue(new byte[] { 1, 2, 3, 4 });
                session.Flush();
            }

            using (var queue = new PersistentQueue(Path))
            using (var session = queue.OpenSession())
            {
                Assert.That(session.Dequeue(), Is.EqualTo(new byte[] { 1, 2, 3, 4 }));
                session.Flush();
            }

            using (var queue = new PersistentQueue(Path))
            using (var session = queue.OpenSession())
            {
                Assert.That(session.Dequeue(), Is.Null);
                session.Flush();
            }
        }

        [Test]
        public void Not_flushing_the_session_will_revert_dequeued_items()
        {
            using (var queue = new PersistentQueue(Path))
            using (var session = queue.OpenSession())
            {
                session.Enqueue(new byte[] { 1, 2, 3, 4 });
                session.Flush();
            }

            using (var queue = new PersistentQueue(Path))
            using (var session = queue.OpenSession())
            {
                Assert.That(session.Dequeue(), Is.EqualTo(new byte[] { 1, 2, 3, 4 }));
                //Explicitly omitted: session.Flush();
            }

            using (var queue = new PersistentQueue(Path))
            using (var session = queue.OpenSession())
            {
                Assert.That(session.Dequeue(), Is.EqualTo(new byte[] { 1, 2, 3, 4 }));
                session.Flush();
            }
        }

        [Test]
        public void Not_flushing_the_session_will_revert_queued_items()
        {
            using (var queue = new PersistentQueue(Path))
            using (var session = queue.OpenSession())
            {
                session.Enqueue(new byte[] { 1, 2, 3, 4 });
            }

            using (var queue = new PersistentQueue(Path))
            using (var session = queue.OpenSession())
            {
                Assert.That(session.Dequeue(), Is.Null);
                session.Flush();
            }
        }

        [Test]
        public void Not_flushing_the_session_will_revert_dequeued_items_two_sessions_same_queue()
        {
            using (var queue = new PersistentQueue(Path))
            using (var session = queue.OpenSession())
            {
                session.Enqueue(new byte[] { 1, 2, 3, 4 });
                session.Flush();
            }

            using (var queue = new PersistentQueue(Path))
            using (var session2 = queue.OpenSession())
            {
                using (var session1 = queue.OpenSession())
                {
                    Assert.That(session1.Dequeue(), Is.EqualTo(new byte[] { 1, 2, 3, 4 }));
                    //Explicitly omitted: session.Flush();
                }
                Assert.That(session2.Dequeue(), Is.EqualTo(new byte[] { 1, 2, 3, 4 }));
                session2.Flush();
            }
        }

        [Test]
        public void Two_sessions_off_the_same_queue_cannot_get_same_item()
        {
            using (var queue = new PersistentQueue(Path))
            using (var session = queue.OpenSession())
            {
                session.Enqueue(new byte[] { 1, 2, 3, 4 });
                session.Flush();
            }

            using (var queue = new PersistentQueue(Path))
            using (var session2 = queue.OpenSession())
            using (var session1 = queue.OpenSession())
            {
                Assert.That(session1.Dequeue(), Is.EqualTo(new byte[] { 1, 2, 3, 4 }));
                Assert.That(session2.Dequeue(), Is.Null);
            }
        }

        [Test]
        public void Items_are_reverted_in_their_original_order()
        {
            using (var queue = new PersistentQueue(Path))
            using (var session = queue.OpenSession())
            {
                session.Enqueue(new byte[] { 1 });
                session.Enqueue(new byte[] { 2 });
                session.Enqueue(new byte[] { 3 });
                session.Enqueue(new byte[] { 4 });
                session.Flush();
            }

            for (int i = 0; i < 4; i++)
            {
                using (var queue = new PersistentQueue(Path))
                using (var session = queue.OpenSession())
                {
                    Assert.That(session.Dequeue(), Is.EqualTo(new byte[] { 1 }), $"Incorrect order on turn {i + 1}");
                    Assert.That(session.Dequeue(), Is.EqualTo(new byte[] { 2 }), $"Incorrect order on turn {i + 1}");
                    Assert.That(session.Dequeue(), Is.EqualTo(new byte[] { 3 }), $"Incorrect order on turn {i + 1}");
                    // Dispose without `session.Flush();`
                }
            }
        }
    }
}