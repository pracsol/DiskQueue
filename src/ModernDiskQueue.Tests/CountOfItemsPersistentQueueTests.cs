using NUnit.Framework;
// ReSharper disable PossibleNullReferenceException

namespace ModernDiskQueue.Tests
{
    [TestFixture, SingleThreaded]
    public class CountOfItemsPersistentQueueTests : PersistentQueueTestsBase
    {
        protected override string Path => "./CountOfItemsTests";

        [Test]
        public void Can_get_count_from_queue()
        {
            using (var queue = new PersistentQueue(Path))
            {
                Assert.That(0, Is.EqualTo(queue.EstimatedCountOfItemsInQueue));
            }
        }

        [Test]
        public void Can_enter_items_and_get_count_of_items()
        {
            using (var queue = new PersistentQueue(Path))
            {
                for (byte i = 0; i < 5; i++)
                {
                    using (var session = queue.OpenSession())
                    {
                        session.Enqueue(new[] { i });
                        session.Flush();
                    }
                }
                Assert.That(5, Is.EqualTo(queue.EstimatedCountOfItemsInQueue));
            }
        }


        [Test]
        public void Can_get_count_of_items_after_queue_restart()
        {
            using (var queue = new PersistentQueue(Path))
            {
                for (byte i = 0; i < 5; i++)
                {
                    using (var session = queue.OpenSession())
                    {
                        session.Enqueue(new[] { i });
                        session.Flush();
                    }
                }
            }

            using (var queue = new PersistentQueue(Path))
            {
                Assert.That(5, Is.EqualTo(queue.EstimatedCountOfItemsInQueue));
            }
        }
    }
}