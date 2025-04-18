namespace ModernDiskQueue.Implementation
{
    using ModernDiskQueue.PublicInterfaces;
    using System;
    using System.Buffers;
    using System.Collections.Generic;
    using System.ComponentModel;
    using System.IO;
    using System.Linq;
    using System.Runtime.CompilerServices;
    using System.Threading;
    using System.Threading.Tasks;

    /// <summary>
    /// A persistent queue implementation.
    /// <para>For synchronous code: Use with 'using' statement to dispose resources properly.</para>
    /// <para>For asynchronous code: Use with 'await using' statement to dispose resources properly.</para>
    /// <para>WARNING: Mixing sync and async operations can cause deadlocks.</para>
    /// </summary>
    internal class PersistentQueueImpl : IPersistentQueueImpl
    {
        private readonly HashSet<Entry> _checkedOutEntries = [];

        private readonly Dictionary<int, int> _countOfItemsPerFile = [];

        private readonly LinkedList<Entry> _entries = new();

        private readonly string _path;

        private readonly object _transactionLogLock = new();
        private readonly object _writerLock = new();
        private readonly AsyncLock _transactionLogLockAsync = new();
        private readonly AsyncLock _writerLockAsync = new();
        private readonly AsyncLock _entriesLockAsync = new();
        private readonly AsyncLock _checkedOutEntriesLockAsync = new();
        /// <summary>
        /// This is only used during async initialization and disposal.
        /// </summary>
        protected readonly AsyncLock _configLockAsync = new();
        private readonly AsyncLocal<bool> _holdsWriterLock = new();
        private readonly AsyncLocal<bool> _holdsEntriesLock = new();
        private readonly AsyncLocal<bool> _holdsCheckedOutEntriesLock = new();
        /// <summary>
        /// This flag is set during the factory initialization and is used to determine if the queue is in async mode.
        /// It will be used for runtime checks to ensure that async methods are not called in sync mode and vice versa.
        /// Sync and Async operations use different locking strategies, and should not be mixed.
        /// </summary>
        private readonly bool _isAsyncMode = false;
        private volatile bool _isInitialized = false;
        private readonly bool _throwOnConflict;
        private static readonly object _configLock = new();
        private volatile bool _disposed;
        private ILockFile? _fileLock;
        private IFileDriver _file;

        /// <summary>
        /// Private constructor is only for use by the Async factory method.
        /// </summary>
        /// <param name="path">The path to the folder in which the queue will be created.</param>
        /// <param name="maxFileSize">The maximum file size of the queue.</param>
        /// <param name="throwOnConflict"></param>
        /// <param name="isAsyncMode">Typically set to true. This parameter differentiates the constructor.</param>
        protected PersistentQueueImpl(string path, int maxFileSize, bool throwOnConflict, bool isAsyncMode)
        {
            _isAsyncMode = isAsyncMode;
            _file = new StandardFileDriver();
            MaxFileSize = maxFileSize;
            _throwOnConflict = throwOnConflict;
            try
            {
                _path = _file.GetFullPath(path);
            }
            catch (UnauthorizedAccessException)
            {
                throw new UnauthorizedAccessException("Directory \"" + path + "\" does not exist or is missing write permissions");
            }
        }

        public PersistentQueueImpl(string path) : this(path, Constants._32Megabytes, true) { }

        public PersistentQueueImpl(string path, int maxFileSize, bool throwOnConflict)
        {
            lock (_configLock)
            {
                _disposed = true;
                _file = new StandardFileDriver();
                TrimTransactionLogOnDispose = PersistentQueue.DefaultSettings.TrimTransactionLogOnDispose;
                ParanoidFlushing = PersistentQueue.DefaultSettings.ParanoidFlushing;
                AllowTruncatedEntries = PersistentQueue.DefaultSettings.AllowTruncatedEntries;
                FileTimeoutMilliseconds = PersistentQueue.DefaultSettings.FileTimeoutMilliseconds;
                SuggestedMaxTransactionLogSize = Constants._32Megabytes;
                SuggestedReadBuffer = 1024 * 1024;
                SuggestedWriteBuffer = 1024 * 1024;
                _throwOnConflict = throwOnConflict;

                MaxFileSize = maxFileSize;
                try
                {
                    _path = _file.GetFullPath(path);
                }
                catch (UnauthorizedAccessException)
                {
                    throw new UnauthorizedAccessException("Directory \"" + path + "\" does not exist or is missing write permissions");
                }

                LockAndReadQueue();

                _disposed = false;
            }
        }

        public static async Task<IPersistentQueueImpl> CreateAsync(string path, CancellationToken cancellationToken = default)
        {
            var queue = new PersistentQueueImpl(path, Constants._32Megabytes, true, true);
            await queue.InitializeAsync(cancellationToken);
            queue._disposed = false;
            return queue;
        }

        public static async Task<PersistentQueueImpl> CreateAsync(string path, int maxFileSize, bool throwOnConflict = true, CancellationToken cancellationToken = default)
        {
            var queue = new PersistentQueueImpl(path, maxFileSize, throwOnConflict, true);
            await queue.InitializeAsync(cancellationToken);
            queue._disposed = false;
            return queue;
        }

        protected async Task InitializeAsync(CancellationToken cancellationToken)
        {
            using (await _configLockAsync.LockAsync(cancellationToken).ConfigureAwait(false))
            {
                _disposed = true;
                TrimTransactionLogOnDispose = PersistentQueue.DefaultSettings.TrimTransactionLogOnDispose;
                ParanoidFlushing = PersistentQueue.DefaultSettings.ParanoidFlushing;
                AllowTruncatedEntries = PersistentQueue.DefaultSettings.AllowTruncatedEntries;
                FileTimeoutMilliseconds = PersistentQueue.DefaultSettings.FileTimeoutMilliseconds;
                SuggestedMaxTransactionLogSize = Constants._32Megabytes;
                SuggestedReadBuffer = 1024 * 1024;
                SuggestedWriteBuffer = 1024 * 1024;

                await LockAndReadQueueAsync(cancellationToken);
                _isInitialized = true;
                _disposed = false;
            }
        }
#if DEBUG
        ~PersistentQueueImpl()
        {
            // The finalizer should not be relied upon for cleanup, instead the object should be explicitly disposed
            // by the user invoking Dispose() or DisposeAsync(), either manually or through using or await using statements.
            if (!_disposed)
            {
                System.Diagnostics.Debug.Fail("PersistentQueueImpl was not properly disposed!");
            }
        }
#else
        ~PersistentQueueImpl()
        {
            if (_disposed) return;
            Dispose();
        }
#endif

        public int SuggestedReadBuffer { get; set; }

        public int SuggestedWriteBuffer { get; set; }

        public long SuggestedMaxTransactionLogSize { get; set; }

        /// <summary>
        /// <para>Setting this to false may cause unexpected data loss in some failure conditions.</para>
        /// <para>Defaults to true.</para>
        /// <para>If true, each transaction commit will flush the transaction log.</para>
        /// <para>This is slow, but ensures the log is correct per transaction in the event of a hard termination (i.e. power failure)</para>
        /// </summary>
        public bool ParanoidFlushing { get; set; }

        public bool AllowTruncatedEntries { get; set; }

        public int FileTimeoutMilliseconds { get; set; }

        public void SetFileDriver(IFileDriver newFileDriver)
        {
            _file = newFileDriver;
        }

        public bool TrimTransactionLogOnDispose { get; set; }

        public int MaxFileSize { get; set; }

        public int EstimatedCountOfItemsInQueue
        {
            get
            {
                if (_isAsyncMode) throw new Exception("A synchronous method was called but this queue was implemented in async mode.\n Always use the async equivalent operations on queues created asynchronously.");
                lock (_entries)
                {
                    return _entries.Count + _checkedOutEntries.Count;
                }
            }
        }

        public async Task<int> GetEstimatedCountOfItemsInQueueAsync(CancellationToken cancellationToken)
        {
            try
            {
                using (await _entriesLockAsync.LockAsync(cancellationToken).ConfigureAwait(false))
                {
                    _holdsEntriesLock.Value = true;
                    try
                    {
                        using (await _checkedOutEntriesLockAsync.LockAsync(cancellationToken).ConfigureAwait(false))
                        {
                            _holdsCheckedOutEntriesLock.Value = true;
                            return _entries.Count + _checkedOutEntries.Count;
                        }
                    }
                    finally
                    {
                        _holdsCheckedOutEntriesLock.Value = false;
                    }
                }
            }
            finally
            {
                _holdsEntriesLock.Value = false;
            }
        }

        public long CurrentFilePosition { get; private set; }

        public int CurrentFileNumber { get; private set; }

        public void Dispose()
        {
            lock (_configLock)
            {
                if (_disposed) return;
                try
                {
                    _disposed = true;
                    lock (_transactionLogLock)
                    {
                        if (TrimTransactionLogOnDispose) FlushTrimmedTransactionLog();
                    }
                    GC.SuppressFinalize(this);
                }
                finally
                {
                    UnlockQueue();
                }
            }
        }

        /// <summary>
        /// Asynchronously disposes the queue implementation, ensuring proper cleanup of resources.
        /// <para>Locks <see cref="_transactionLogLockAsync"/> and <see cref="_configLockAsync"/>.</para>
        /// </summary>
        public async ValueTask DisposeAsync()
        {
            using (await _configLockAsync.LockAsync().ConfigureAwait(false))
            {
                // First check if already disposed to avoid unnecessary work
                if (_disposed)
                {
                    return;
                }

                try
                {
                    _disposed = true;
                    // Use SemaphoreSlim for transaction log synchronization during async operation
                    using (await _transactionLogLockAsync.LockAsync().ConfigureAwait(false))
                    {
                        // Determine if we need to flush the transaction log.
                        if (TrimTransactionLogOnDispose)
                        {
                            await FlushTrimmedTransactionLogAsync();
                        }
                    }
                    GC.SuppressFinalize(this);
                }
                finally
                {
                    if (_holdsWriterLock.Value)
                    {
                        await UnlockQueueAsync_UnderLock().ConfigureAwait(false);
                    }
                    else
                    {
                        // Perform async unlock outside any locks
                        await UnlockQueueAsync().ConfigureAwait(false);
                    }
                }
            }
        }

        public void AcquireWriter(IFileStream stream, Func<IFileStream, Task<long>> action, Action<IFileStream> onReplaceStream)
        {
            if (_isAsyncMode) throw new Exception("A synchronous method was called but this queue was implemented in async mode.\n Always use the async equivalent operations on queues created asynchronously.");
            lock (_writerLock)
            {
                stream.SetPosition(CurrentFilePosition);
                CurrentFilePosition = Synchronise.Run(() => action(stream));
                if (CurrentFilePosition < MaxFileSize) return;

                CurrentFileNumber++;
                var writer = CreateWriter();
                // we assume same size messages, or near size messages
                // that gives us a good heuristic for creating the size of 
                // the new file, so it wouldn't be fragmented
                writer.SetLength(CurrentFilePosition);
                CurrentFilePosition = 0;
                onReplaceStream(writer);
            }
        }

        /// <summary>
        /// Asynchronously acquires a writer for the file stream.
        /// </summary>
        public async Task AcquireWriterAsync(IFileStream stream, Func<IFileStream, Task<long>> action,
            Action<IFileStream> onReplaceStream, CancellationToken cancellationToken = default)
        {
            // We use a semaphore to allow async operations while maintaining the lock semantics
            using (await _writerLockAsync.LockAsync(cancellationToken).ConfigureAwait(false))
            {
                _holdsWriterLock.Value = true;
                stream.SetPosition(CurrentFilePosition); // Set position at current file position
                CurrentFilePosition = await action(stream).ConfigureAwait(false); // Execute the action and await the result
                // Check if we need to create a new file
                if (CurrentFilePosition < MaxFileSize)
                {
                    return;
                }

                CurrentFileNumber++; // Roll over to a new file
                IFileStream writer = await _file.OpenWriteStreamAsync(GetDataPath(CurrentFileNumber), cancellationToken).ConfigureAwait(false);

                // Set the length of the new file
                writer.SetLength(CurrentFilePosition);
                CurrentFilePosition = 0;
                onReplaceStream(writer);
            }
        }

        public void CommitTransaction(ICollection<Operation> operations)
        {
            if (_isAsyncMode) throw new Exception("A synchronous method was called but this queue was implemented in async mode.\n Always use the async equivalent operations on queues created asynchronously.");
            if (operations.Count == 0)
                return;

            byte[] transactionBuffer = GenerateTransactionBuffer(operations);

            lock (_transactionLogLock)
            {
                long txLogSize;
                using (var stream = WaitForTransactionLog(transactionBuffer))
                {
                    txLogSize = stream.Write(transactionBuffer);
                    stream.Flush();
                }

                ApplyTransactionOperations(operations);
                TrimTransactionLogIfNeeded(txLogSize);

                _file.AtomicWrite(Meta, stream =>
                {
                    var bytes = BitConverter.GetBytes(CurrentFileNumber);
                    stream.Write(bytes);
                    bytes = BitConverter.GetBytes(CurrentFilePosition);
                    stream.Write(bytes);
                });

                if (ParanoidFlushing) FlushTrimmedTransactionLog();
            }
        }

        /// <summary>
        /// <para>UNSAFE. Incorrect use will result in data loss.</para>
        /// Asynchronously commit a sequence of operations to storage
        /// </summary>
        public async Task CommitTransactionAsync(ICollection<Operation> operations, CancellationToken cancellationToken = default)
        {
            cancellationToken.ThrowIfCancellationRequested();

            if (operations.Count == 0)
            {
                return;
            }

            byte[] transactionBuffer = GenerateTransactionBuffer(operations);
            long txLogSize;
            // Use SemaphoreSlim instead of lock for async-compatible synchronization
            using (await _transactionLogLockAsync.LockAsync(cancellationToken).ConfigureAwait(false))
            {
                await using (var stream = await WaitForTransactionLogAsync(transactionBuffer, cancellationToken).ConfigureAwait(false))
                {
                    txLogSize = await stream.WriteAsync(transactionBuffer.AsMemory(), cancellationToken).ConfigureAwait(false);
                    await stream.FlushAsync(cancellationToken).ConfigureAwait(false);
                }
            }

            // Apply operations and clean up files
            await ApplyTransactionOperationsAsync(operations, cancellationToken).ConfigureAwait(false);
            await TrimTransactionLogIfNeededAsync(txLogSize, cancellationToken);

            // Write metadata asynchronously
            await _file.AtomicWriteAsync(Meta, async writer =>
            {
                var bytes = BitConverter.GetBytes(CurrentFileNumber);
                await writer.WriteAsync(bytes.AsMemory(), cancellationToken);
                bytes = BitConverter.GetBytes(CurrentFilePosition);
                await writer.WriteAsync(bytes.AsMemory(), cancellationToken);
                await Task.CompletedTask; // Satisfy the async signature requirement
            }, cancellationToken).ConfigureAwait(false);

            // Handle paranoid flushing
            if (ParanoidFlushing)
            {
                await FlushTrimmedTransactionLogAsync(cancellationToken).ConfigureAwait(false);
            }
        }

        /// <summary>
        /// <para>UNSAFE. Incorrect use will result in data loss.</para>
        /// </summary>
        public Entry? Dequeue()
        {
            if (_isAsyncMode) throw new Exception("Cannot use Dequeue for async queue. Use DequeueAsync instead.");
            lock (_entries)
            {
                var first = _entries.First;
                if (first == null) return null;
                var entry = first.Value ?? throw new Exception("Entry queue was in an invalid state: null entry");
                if (entry.Data == null)
                {
                    var ok = ReadAhead();
                    if (!ok) return null;
                }
                _entries.RemoveFirst();
                // we need to create a copy so we will not hold the data
                // in memory as well as the position
                lock (_checkedOutEntries)
                {
                    _checkedOutEntries.Add(new Entry(entry.FileNumber, entry.Start, entry.Length));
                }
                return entry;
            }
        }

        /// <summary>
        /// <para>UNSAFE. Incorrect use will result in data loss.</para>
        /// Asynchronously dequeue data, returning storage entry
        /// </summary>
        public async Task<Entry?> DequeueAsync(CancellationToken cancellationToken = default)
        {
            cancellationToken.ThrowIfCancellationRequested();

            Entry? entry;

            try
            {
                // We need to be really careful about nested locks here.
                using (await _entriesLockAsync.LockAsync(cancellationToken).ConfigureAwait(false))
                {
                    _holdsEntriesLock.Value = true;
                    var first = _entries.First;
                    if (first == null)
                    {
                        return null;
                    }

                    entry = first.Value ?? throw new Exception("Entry queue was in an invalid state: null entry");

                    // If data is there, just complete the operation and get out.
                    if (entry.Data != null)
                    {
                        _entries.RemoveFirst();

                        try
                        {
                            // lock the checked out entries for addition of entry.
                            using (await _checkedOutEntriesLockAsync.LockAsync(cancellationToken).ConfigureAwait(false))
                            {
                                _holdsCheckedOutEntriesLock.Value = true;
                                _checkedOutEntries.Add(new Entry(entry.FileNumber, entry.Start, entry.Length));
                                return entry;
                            }
                        }
                        finally
                        {
                            _holdsCheckedOutEntriesLock.Value = false;
                        }
                    }

                    // Data needs loading, so load it with method that can work with existing lock.
                    bool waspagingSuccessful = await ReadAheadAsync_UnderLock(cancellationToken).ConfigureAwait(false);
                    if (waspagingSuccessful)
                    {
                        _entries.RemoveFirst();
                    }
                    else
                    {
                        return null;
                    }
                }
            }
            finally
            {
                _holdsEntriesLock.Value = false;
            }

            try
            {
                using (await _checkedOutEntriesLockAsync.LockAsync(cancellationToken).ConfigureAwait(false))
                {
                    _holdsCheckedOutEntriesLock.Value = true;
                    _checkedOutEntries.Add(new Entry(entry.FileNumber, entry.Start, entry.Length));
                    return entry;
                }
            }
            finally
            {
                _holdsCheckedOutEntriesLock.Value = false;
            }
        }

        public IPersistentQueueSession OpenSession()
        {
            if (_isAsyncMode) throw new Exception("Cannot use OpenSession for async queue. Use OpenSessionAsync instead.");
            return new PersistentQueueSession(this, CreateWriter(), SuggestedWriteBuffer, FileTimeoutMilliseconds);
        }

        /// <summary>
        /// Asynchronously lock the queue for use, and give access to session methods.
        /// The session <b>MUST</b> be disposed as soon as possible.
        /// </summary>
        public async Task<IPersistentQueueSession> OpenSessionAsync(CancellationToken cancellationToken = default)
        {
            cancellationToken.ThrowIfCancellationRequested();

            var writer = await CreateWriterAsync(cancellationToken).ConfigureAwait(false);

            return new PersistentQueueSession(this, writer, SuggestedWriteBuffer, FileTimeoutMilliseconds);
        }

        public void Reinstate(IEnumerable<Operation> reinstatedOperations)
        {
            if (_isAsyncMode) throw new Exception("A synchronous method was called but this queue was implemented in async mode.\n Always use the async equivalent operations on queues created asynchronously.");
            lock (_entries)
            {
                ApplyTransactionOperations(
                    from entry in reinstatedOperations.Reverse()
                    where entry.Type == OperationType.Dequeue
                    select new Operation(
                        OperationType.Reinstate,
                        entry.FileNumber,
                        entry.Start,
                        entry.Length
                        )
                    );
            }
        }

        /// <summary>
        /// <para>UNSAFE. Incorrect use will result in data loss.</para>
        /// <para>Asynchronously undo Enqueue and Dequeue operations.</para>
        /// <para>These MUST have been real operations taken.</para>
        /// </summary>
        public async Task ReinstateAsync(IEnumerable<Operation> reinstatedOperations, CancellationToken cancellationToken = default)
        {
            cancellationToken.ThrowIfCancellationRequested();

            // Transform operations outside lock for better performance
            var operations = (from entry in reinstatedOperations.Reverse()
                              where entry.Type == OperationType.Dequeue
                              select new Operation(
                                    OperationType.Reinstate,
                                    entry.FileNumber,
                                    entry.Start,
                                    entry.Length
                                )).ToList(); // Materialize the query outside the lock

            await ApplyTransactionOperationsAsync(operations, cancellationToken).ConfigureAwait(false);
        }

        public int[] ApplyTransactionOperationsInMemory(IEnumerable<Operation>? operations)
        {
            if (_isAsyncMode) throw new Exception("A synchronous method was called but this queue was implemented in async mode.\n Always use the async equivalent operations on queues created asynchronously.");
            if (operations == null) return [];

            foreach (var operation in operations)
            {
                switch (operation?.Type)
                {
                    case OperationType.Enqueue:
                        var entryToAdd = new Entry(operation);
                        lock (_entries) { _entries.AddLast(entryToAdd); }
                        var itemCountAddition = _countOfItemsPerFile.GetValueOrDefault(entryToAdd.FileNumber);
                        _countOfItemsPerFile[entryToAdd.FileNumber] = itemCountAddition + 1;
                        break;

                    case OperationType.Dequeue:
                        var entryToRemove = new Entry(operation);
                        lock (_checkedOutEntries) { _checkedOutEntries.Remove(entryToRemove); }
                        var itemCountRemoval = _countOfItemsPerFile.GetValueOrDefault(entryToRemove.FileNumber);
                        _countOfItemsPerFile[entryToRemove.FileNumber] = itemCountRemoval - 1;
                        break;

                    case OperationType.Reinstate:
                        var entryToReinstate = new Entry(operation);
                        lock (_entries) { _entries.AddFirst(entryToReinstate); }
                        lock (_checkedOutEntries) { _checkedOutEntries.Remove(entryToReinstate); }
                        break;
                }
            }

            var filesToRemove = new HashSet<int>(
                from pair in _countOfItemsPerFile
                where pair.Value < 1
                select pair.Key
                );

            foreach (var i in filesToRemove)
            {
                _countOfItemsPerFile.Remove(i);
            }
            return filesToRemove.ToArray();
        }

        /// <summary>
        /// Asynchronously applies a sequence of queue operations to the in-memory state.
        /// <para>This method processes enqueue, dequeue, and reinstate operations, updating the internal
        /// collections (<see cref="_entries"/> and <see cref="_checkedOutEntries"/>), and tracking file usage.</para>
        /// <para>The method also identifies files that are no longer needed (have no entries) and returns them for cleanup.</para>
        /// </summary>
        /// <param name="operations">Collection of operations to apply (enqueue, dequeue, reinstate)</param>
        /// <param name="cancellationToken"><see cref="CancellationToken"/></param>
        /// <returns>Array of file numbers that can be safely deleted (contain no entries)</returns>
        public async Task<int[]> ApplyTransactionOperationsInMemoryAsync(IEnumerable<Operation>? operations, CancellationToken cancellationToken)
        {
            if (operations == null) return [];

            foreach (var operation in operations)
            {
                cancellationToken.ThrowIfCancellationRequested();
                switch (operation?.Type)
                {
                    case OperationType.Enqueue:
                        var entryToAdd = new Entry(operation);
                        if (_holdsEntriesLock.Value)
                        {
                            _entries.AddLast(entryToAdd);
                        }
                        else
                        {
                            try
                            {
                                using (await _entriesLockAsync.LockAsync(cancellationToken).ConfigureAwait(false))
                                {
                                    _holdsEntriesLock.Value = true;
                                    _entries.AddLast(entryToAdd);
                                }
                            }
                            finally
                            {
                                _holdsEntriesLock.Value = false;
                            }
                        }
                        var itemCountAddition = _countOfItemsPerFile.GetValueOrDefault(entryToAdd.FileNumber);
                        _countOfItemsPerFile[entryToAdd.FileNumber] = itemCountAddition + 1;
                        break;

                    case OperationType.Dequeue:
                        var entryToRemove = new Entry(operation);
                        if (_holdsCheckedOutEntriesLock.Value)
                        {
                            _checkedOutEntries.Remove(entryToRemove);
                        }
                        else
                        {
                            try
                            {
                                using (await _checkedOutEntriesLockAsync.LockAsync(cancellationToken).ConfigureAwait(false))
                                {
                                    _holdsCheckedOutEntriesLock.Value = true;
                                    _checkedOutEntries.Remove(entryToRemove);
                                }
                            }
                            finally
                            {
                                _holdsCheckedOutEntriesLock.Value = false;
                            }
                        }
                        var itemCountRemoval = _countOfItemsPerFile.GetValueOrDefault(entryToRemove.FileNumber);
                        _countOfItemsPerFile[entryToRemove.FileNumber] = itemCountRemoval - 1;
                        break;
                    case OperationType.Reinstate:
                        var entryToReinstate = new Entry(operation);
                        if (_holdsEntriesLock.Value)
                        {
                            _entries.AddFirst(entryToReinstate);
                        }
                        else
                        {
                            try
                            {
                                using (await _entriesLockAsync.LockAsync(cancellationToken).ConfigureAwait(false))
                                {
                                    _holdsEntriesLock.Value = true;
                                    _entries.AddFirst(entryToReinstate);
                                }
                            }
                            finally
                            {
                                _holdsEntriesLock.Value = false;
                            }
                        }
                        if (_holdsCheckedOutEntriesLock.Value)
                        {
                            _checkedOutEntries.Remove(entryToReinstate);
                        }
                        else
                        {
                            try
                            {
                                using (await _checkedOutEntriesLockAsync.LockAsync(cancellationToken).ConfigureAwait(false))
                                {
                                    _holdsCheckedOutEntriesLock.Value = true;
                                    _checkedOutEntries.Remove(entryToReinstate);
                                }
                            }
                            finally
                            {
                                _holdsCheckedOutEntriesLock.Value = false;
                            }
                        }
                        break;
                }
            }

            var filesToRemove = new HashSet<int>(
                from pair in _countOfItemsPerFile
                where pair.Value < 1
                select pair.Key
                );

            foreach (var i in filesToRemove)
            {
                _countOfItemsPerFile.Remove(i);
            }
            return filesToRemove.ToArray();
        }

        public void HardDelete(bool reset)
        {
            if (_isAsyncMode) throw new Exception("Cannot use HardDelete for async queue. Use HardDeleteAsync instead.");
            lock (_writerLock)
            {
                UnlockQueue();
                _file.DeleteRecursive(_path);
                if (reset) LockAndReadQueue();
                else Dispose();
            }
        }

        /// <summary>
        /// WARNING:
        /// Asynchronously attempt to delete the queue, all its data, and all support files.
        /// </summary>
        public async Task HardDeleteAsync(bool reset, CancellationToken cancellationToken = default)
        {
            cancellationToken.ThrowIfCancellationRequested();
            if (_holdsWriterLock.Value)
            {
                await HardDeleteAsync_UnderLock(reset, cancellationToken).ConfigureAwait(false);
            }
            else
            {
                try
                {
                    // Use a semaphore for async-compatible locking
                    using (await _writerLockAsync.LockAsync(cancellationToken).ConfigureAwait(false))
                    {
                        _holdsWriterLock.Value = true;
                        await HardDeleteAsync_UnderLock(reset, cancellationToken).ConfigureAwait(false);
                    }
                }
                finally
                {
                    _holdsWriterLock.Value = false;
                }
            }
        }

        /// <summary>
        /// WARNING:
        /// Asynchronously attempt to delete the queue, all its data, and all support files.
        /// <para>WARNING: Assumes the caller has already locked <see cref="_writerLockAsync"/>.</para>
        /// </summary>
        private async Task HardDeleteAsync_UnderLock(bool reset, CancellationToken cancellationToken = default)
        {
            // We can call the UnderLock directly since we already know we're operating under a lock
            await UnlockQueueAsync_UnderLock(cancellationToken).ConfigureAwait(false);

            // If IFileDriver gets an async version of DeleteRecursive in the future, use it here
            await _file.DeleteRecursiveAsync(_path, cancellationToken);

            if (reset)
            {
                await LockAndReadQueueAsync(cancellationToken).ConfigureAwait(false);
            }
            else
            {
                // Since we're performing a hard delete, and the whole point is to clean up the folders and files, 
                // we override the flag TrimTransactionLogOnDispose, setting it false to avoid the 
                // transaction log being recreated.
                bool archivedTransactionLogConfiguration = TrimTransactionLogOnDispose;
                TrimTransactionLogOnDispose = false;
                await DisposeAsync().ConfigureAwait(false);
                TrimTransactionLogOnDispose = archivedTransactionLogConfiguration;
            }
        }

        private IFileStream WaitForTransactionLog(byte[] transactionBuffer)
        {
            if (_isAsyncMode) throw new Exception("A synchronous method was called but this queue was implemented in async mode.\n Always use the async equivalent operations on queues created asynchronously.");
            for (int i = 0; i < 10; i++)
            {
                try
                {
                    return _file.OpenTransactionLog(TransactionLog, transactionBuffer.Length);
                }
                catch (Exception ex)
                {
                    PersistentQueue.Log(ex.ToString() ?? "");
                    Thread.Sleep(250);
                }
            }
            throw new TimeoutException("Could not acquire transaction log lock");
        }

        /// <summary>
        /// Wait for transaction log asynchronously
        /// </summary>
        private async Task<IFileStream> WaitForTransactionLogAsync(byte[] transactionBuffer, CancellationToken cancellationToken = default)
        {
            const int maxRetries = 10;
            int retryCount = 0;
            TimeSpan delay = TimeSpan.FromMilliseconds(50); // implemented an exponential backoff, starting with small delay.

            while (true)
            {
                cancellationToken.ThrowIfCancellationRequested();

                try
                {
                    return await _file.OpenTransactionLogAsync(
                        TransactionLog,
                        transactionBuffer.Length,
                        cancellationToken).ConfigureAwait(false);
                }
                catch (Exception ex)
                {
                    PersistentQueue.Log(ex.ToString() ?? "");

                    if (++retryCount >= maxRetries)
                        throw new TimeoutException("Could not acquire transaction log lock");

                    await Task.Delay(delay, cancellationToken).ConfigureAwait(false);
                    delay = TimeSpan.FromMilliseconds(Math.Min(delay.TotalMilliseconds * 2, 1000)); // cap delay to 1 second
                }
            }
        }

        private int CurrentCountOfItemsInQueue
        {
            get
            {
                if (_isAsyncMode) throw new Exception("A synchronous method was called but this queue was implemented in async mode.\n Always use the async equivalent operations on queues created asynchronously.");
                lock (_entries)
                {
                    return _entries.Count + _checkedOutEntries.Count;
                }
            }
        }

        private async Task<int> GetCurrentCountOfItemsInQueueAsync(CancellationToken cancellationToken)
        {
            try
            {
                using (await _entriesLockAsync.LockAsync(cancellationToken).ConfigureAwait(false))
                {
                    _holdsEntriesLock.Value = true;
                    try
                    {
                        using (await _checkedOutEntriesLockAsync.LockAsync(cancellationToken).ConfigureAwait(false))
                        {
                            _holdsCheckedOutEntriesLock.Value = true;
                            return _entries.Count + _checkedOutEntries.Count;
                        }
                    }
                    finally
                    {
                        _holdsCheckedOutEntriesLock.Value = false;
                    }
                }
            }
            finally
            {
                _holdsEntriesLock.Value = false;
            }
        }

        private void LockAndReadQueue()
        {
            if (_isAsyncMode) throw new Exception("A synchronous method was called but this queue was implemented in async mode.\n Always use the async equivalent operations on queues created asynchronously.");
            try
            {
                if (!_file.DirectoryExists(_path))
                    CreateDirectory(_path);

                var result = LockQueue();
                if (result.IsFailure)
                {
#pragma warning disable IDE0079 // Suppress warning about suppressing warnings
#pragma warning disable CA1816 // Use concrete types when possible for improved performance
                    GC.SuppressFinalize(this); //avoid finalizing invalid instance
#pragma warning restore CA1816, IDE0079
                    throw new InvalidOperationException("Another instance of the queue is already in action, or directory does not exist", result.Error ?? new Exception());
                }
            }
            catch (UnauthorizedAccessException)
            {
                throw new UnauthorizedAccessException("Directory \"" + _path + "\" does not exist or is missing write permissions");
            }

            CurrentFileNumber = 0;
            CurrentFilePosition = 0;
            if (_file.FileExists(Meta) || _file.FileExists(TransactionLog))
            {
                ReadExistingQueue();
            }
        }

        /// <summary>
        /// Lock and read queue asynchronously
        /// </summary>
        protected async Task LockAndReadQueueAsync(CancellationToken cancellationToken = default)
        {
            Maybe<bool> isFilePathLocked;
            try
            {
                bool directoryExists = await _file.DirectoryExistsAsync(_path, cancellationToken)
                    .ConfigureAwait(false);

                if (!directoryExists)
                {
                    await _file.CreateDirectoryAsync(_path, cancellationToken)
                        .ConfigureAwait(false);
                }

                if (_holdsWriterLock.Value)
                {
                    isFilePathLocked = await LockQueueAsync_UnderLock(cancellationToken).ConfigureAwait(false);
                }
                else
                {
                    isFilePathLocked = await LockQueueAsync(cancellationToken).ConfigureAwait(false);
                }
                if (isFilePathLocked.IsFailure)
                {
#pragma warning disable IDE0079 // Suppress warning about suppressing warnings
#pragma warning disable CA1816 // Use concrete types when possible for improved performance
                    GC.SuppressFinalize(this); // avoid finalizing invalid instance
#pragma warning restore CA1859, IDE0079
                    throw new InvalidOperationException(
                        "Another instance of the queue is already in action, or directory does not exist",
                        isFilePathLocked.Error ?? new Exception());
                }

                CurrentFileNumber = 0;
                CurrentFilePosition = 0;

                bool metaExists, transactionLogExists;

                metaExists = await _file.FileExistsAsync(Meta, cancellationToken)
                    .ConfigureAwait(false);
                transactionLogExists = await _file.FileExistsAsync(TransactionLog, cancellationToken)
                    .ConfigureAwait(false);

                if (metaExists || transactionLogExists)
                {
                    await ReadExistingQueueAsync(cancellationToken).ConfigureAwait(false);
                }
            }
            catch (UnauthorizedAccessException)
            {
                throw new UnauthorizedAccessException(
                    $"Directory \"{_path}\" does not exist or is missing write permissions");
            }
        }

        private void ReadExistingQueue()
        {
            if (_isAsyncMode) throw new Exception("A synchronous method was called but this queue was implemented in async mode.\n Always use the async equivalent operations on queues created asynchronously.");
            try
            {
                ReadMetaState();
                ReadTransactionLog();
            }
            catch (Exception)
            {
                GC.SuppressFinalize(this); //avoid finalizing invalid instance
                UnlockQueue();
                throw;
            }
        }

        /// <summary>
        /// Asynchronously read the existing queue
        /// </summary>
        private async Task ReadExistingQueueAsync(CancellationToken cancellationToken = default)
        {
            try
            {
                await ReadMetaStateAsync(cancellationToken).ConfigureAwait(false);
                await ReadTransactionLogAsync(cancellationToken).ConfigureAwait(false);
            }
            catch (Exception)
            {
                GC.SuppressFinalize(this); //avoid finalizing invalid instance
                await UnlockQueueAsync(cancellationToken).ConfigureAwait(false);
                throw;
            }
        }

        private void UnlockQueue()
        {
            if (_isAsyncMode) throw new Exception("A synchronous method was called but this queue was implemented in async mode.\n Always use the async equivalent operations on queues created asynchronously.\n Make sure async queues are being disposed properly with 'await using' statements.");
            lock (_writerLock)
            {
                if (string.IsNullOrWhiteSpace(_path)) return;
                var target = _file.PathCombine(_path, "lock");
                if (_fileLock != null)
                {
                    _file.ReleaseLock(_fileLock);
                    _file.PrepareDelete(target);
                }

                _fileLock = null;
            }
            _file.Finalise();
        }

        /// <summary>
        /// Asynchronously unlock and clear the queue's lock file.
        /// <para>Locks <see cref="_writerLockAsync"/>.</para>
        /// </summary>
        private async Task UnlockQueueAsync(CancellationToken cancellationToken = default)
        {
            try
            {
                using (await _writerLockAsync.LockAsync(cancellationToken).ConfigureAwait(false))
                {
                    _holdsWriterLock.Value = true;
                    await UnlockQueueAsync_UnderLock(cancellationToken).ConfigureAwait(false);
                }
            }
            finally
            {
                _holdsWriterLock.Value = false;
            }
        }

        /// <summary>
        /// Asynchronously unlock and clear the queue's lock file.
        /// <para>WARNING: Assumes the caller has already locked <see cref="_writerLockAsync"/>.</para>
        /// </summary>
        private async Task UnlockQueueAsync_UnderLock(CancellationToken cancellationToken = default)
        {
            if (string.IsNullOrWhiteSpace(_path)) return;
            var target = _file.PathCombine(_path, "lock");

            if (_fileLock != null)
            {
                // Delete the lock file.
                await _file.ReleaseLockAsync(_fileLock, cancellationToken);
                // Enqueue any files in the queue folder for deletion.
                await _file.PrepareDeleteAsync(target, cancellationToken).ConfigureAwait(false);
                // Move through queue and delete any files queued for deletion.
                await _file.FinaliseAsync(cancellationToken).ConfigureAwait(false);
            }
            _fileLock = null;
        }

        private Maybe<bool> LockQueue()
        {
            if (_isAsyncMode) throw new Exception("A synchronous method was called but this queue was implemented in async mode.\n Always use the async equivalent operations on queues created asynchronously.");
            lock (_writerLock)
            {
                try
                {
                    var target = _file.PathCombine(_path, "lock");
                    var result = _file.CreateLockFile(target);
                    if (result.IsFailure) return result.Chain<bool>();
                    _fileLock = result.Value!;
                    return Maybe<bool>.Success(true);
                }
                catch (Exception ex)
                {
                    return Maybe<bool>.Fail(ex);
                }
            }
        }

        /// <summary>
        /// Try to get a lock on a file path asynchronously
        /// <para>Locks <see cref="_writerLockAsync"/>.</para>
        /// </summary>
        private async Task<Maybe<bool>> LockQueueAsync(CancellationToken cancellationToken = default)
        {
            if (_holdsWriterLock.Value)
            {
                return await LockQueueAsync_UnderLock(cancellationToken).ConfigureAwait(false);
            }
            else
            {
                try
                {
                    using (await _writerLockAsync.LockAsync(cancellationToken).ConfigureAwait(false))
                    {
                        _holdsWriterLock.Value = true;
                        return await LockQueueAsync_UnderLock(cancellationToken).ConfigureAwait(false);
                    }
                }
                finally
                {
                    _holdsWriterLock.Value = false;
                }
            }
        }

        /// <summary>
        /// Try to get a lock on a file path asynchronously
        /// <para>WARNING: Caller must have a lock on <see cref="_writerLockAsync"/>.</para>
        /// </summary>
        private async Task<Maybe<bool>> LockQueueAsync_UnderLock(CancellationToken cancellationToken = default)
        {
            try
            {
                var target = _file.PathCombine(_path, "lock");

                Maybe<ILockFile> result = await _file.CreateLockFileAsync(target, cancellationToken)
                        .ConfigureAwait(false);

                if (result.IsFailure)
                    return result.Chain<bool>();

                _fileLock = result.Value!;
                return Maybe<bool>.Success(true);
            }
            catch (Exception ex)
            {
                return Maybe<bool>.Fail(ex);
            }
        }

        private void CreateDirectory(string s)
        {
            _file.CreateDirectory(s);
            SetPermissions.TryAllowReadWriteForAll(s);
        }

        private string TransactionLog => _file.PathCombine(_path, "transaction.log");

        private string Meta => _file.PathCombine(_path, "meta.state");

        /// <summary>
        /// Assumes that entries has at least one entry. Should be called inside a lock.
        /// </summary>
        private bool ReadAhead()
        {
            if (_isAsyncMode) throw new Exception("A synchronous method was called but this queue was implemented in async mode.\n Always use the async equivalent operations on queues created asynchronously.");
            long currentBufferSize = 0;

            var firstEntry = _entries.First?.Value ?? throw new Exception("Invalid queue state: first entry is null");
            Entry lastEntry = firstEntry;
            foreach (var entry in _entries)
            {
                // we can't read ahead to another file or
                // if we have unordered queue, or sparse items
                if (entry != lastEntry &&
                    (entry.FileNumber != lastEntry.FileNumber ||
                    entry.Start != (lastEntry.Start + lastEntry.Length)))
                {
                    break;
                }
                if (currentBufferSize + entry.Length > SuggestedReadBuffer)
                {
                    break;
                }
                lastEntry = entry;
                currentBufferSize += entry.Length;
            }
            if (lastEntry == firstEntry)
            {
                currentBufferSize = lastEntry.Length;
            }

            var buffer = ReadEntriesFromFile(firstEntry, currentBufferSize);
            if (buffer.IsFailure)
            {
                if (AllowTruncatedEntries) return false;
                throw buffer.Error!;
            }

            var index = 0;
            foreach (var entry in _entries)
            {
                entry.Data = new byte[entry.Length];
                Buffer.BlockCopy(buffer.Value!, index, entry.Data, 0, entry.Length);
                index += entry.Length;
                if (entry == lastEntry)
                    break;
            }
            return true;
        }

        /// <summary>
        /// Reads ahead to get entries from disk.
        /// </summary>
        /// <param name="cancellationToken"><see cref="CancellationToken"/>.</param>
        /// <returns>True if data read from successfully.</returns>
        private async Task<bool> ReadAheadAsync(CancellationToken cancellationToken)
        {
            try
            {
                using (await _entriesLockAsync.LockAsync(cancellationToken).ConfigureAwait(false))
                {
                    _holdsEntriesLock.Value = true;
                    return await ReadAheadAsync_UnderLock(cancellationToken).ConfigureAwait(false);
                }
            }
            finally
            {
                _holdsEntriesLock.Value = false;
            }
        }

        /// <summary>
        /// Reads ahead to get entries from disk without acquiring locks. The caller MUST hold _entriesLockAsync.
        /// </summary>
        /// <param name="cancellationToken"><see cref="CancellationToken"/>.</param>
        /// <returns>True if data has been read.</returns>
        private async Task<bool> ReadAheadAsync_UnderLock(CancellationToken cancellationToken)
        {
            // IMPORTANT: Caller must hold _entriesLockAsync lock!
            long currentBufferSize = 0;

            var firstEntry = _entries.First?.Value ?? throw new Exception("Invalid queue state: first entry is null");
            Entry lastEntry = firstEntry;
            foreach (var entry in _entries)
            {
                cancellationToken.ThrowIfCancellationRequested();
                // we can't read ahead to another file or
                // if we have unordered queue, or sparse items
                if (entry != lastEntry &&
                    (entry.FileNumber != lastEntry.FileNumber ||
                    entry.Start != (lastEntry.Start + lastEntry.Length)))
                {
                    break;
                }
                if (currentBufferSize + entry.Length > SuggestedReadBuffer)
                {
                    break;
                }
                lastEntry = entry;
                currentBufferSize += entry.Length;
            }
            if (lastEntry == firstEntry)
            {
                currentBufferSize = lastEntry.Length;
            }

            var buffer = await ReadEntriesFromFileAsync(firstEntry, currentBufferSize, cancellationToken).ConfigureAwait(false);
            if (buffer.IsFailure)
            {
                if (AllowTruncatedEntries) return false;
                throw buffer.Error!;
            }

            var index = 0;
            foreach (var entry in _entries)
            {
                cancellationToken.ThrowIfCancellationRequested();
                entry.Data = new byte[entry.Length];
                Buffer.BlockCopy(buffer.Value!, index, entry.Data, 0, entry.Length);
                index += entry.Length;
                if (entry == lastEntry)
                    break;
            }

            return true;
        }

        private Maybe<byte[]> ReadEntriesFromFile(Entry firstEntry, long currentBufferSize)
        {
            if (_isAsyncMode) throw new Exception("A synchronous method was called but this queue was implemented in async mode.\n Always use the async equivalent operations on queues created asynchronously.");
            try
            {
                var buffer = new byte[currentBufferSize];
                if (firstEntry.Length < 1) return buffer.Success();
                using var reader = _file.OpenReadStream(GetDataPath(firstEntry.FileNumber));
                reader.MoveTo(firstEntry.Start);
                var totalRead = 0;
                var failCount = 0;
                do
                {
                    var bytesRead = reader.Read(buffer, totalRead, buffer.Length - totalRead);
                    if (bytesRead < 1)
                    {
                        if (failCount > 10)
                        {
                            return Maybe<byte[]>.Fail(new InvalidOperationException("End of file reached while trying to read queue item. Exceeded retry count."));
                        }

                        failCount++;
                        Thread.Sleep(100);
                    }
                    else
                    {
                        failCount = 0;
                    }

                    totalRead += bytesRead;
                } while (totalRead < buffer.Length);

                return buffer.Success();
            }
            catch (Exception ex)
            {
                return Maybe<byte[]>.Fail(new InvalidOperationException("End of file reached while trying to read queue item", ex));
            }
        }

        private async Task<Maybe<byte[]>> ReadEntriesFromFileAsync(Entry firstEntry, long currentBufferSize, CancellationToken cancellationToken)
        {
            byte[]? rentedBuffer = null;
            try
            {
                rentedBuffer = ArrayPool<byte>.Shared.Rent((int)currentBufferSize);

                if (firstEntry.Length < 1)
                {
                    return Array.Empty<byte>().Success();
                }

                await using var reader = await _file.OpenReadStreamAsync(GetDataPath(firstEntry.FileNumber), cancellationToken).ConfigureAwait(false);

                reader.MoveTo(firstEntry.Start);

                // Read the data with variable retry logic.
                var totalRead = 0;
                var failCount = 0;
                const int MaxFailCount = 10;

                do
                {
                    cancellationToken.ThrowIfCancellationRequested();

                    int bytesRead = await reader.ReadAsync(rentedBuffer, totalRead, (int)currentBufferSize - totalRead, cancellationToken).ConfigureAwait(false);
                    if (bytesRead < 1)
                    {
                        if (++failCount > MaxFailCount)
                        {
                            return Maybe<byte[]>.Fail(new InvalidOperationException("End of file reached while trying to read queue item. Exceeded retry count."));
                        }

                        // figure out the varying backoff for retry delay.
                        int delayMs = Math.Min(100 * (1 << (failCount -1)), 1000) + new Random().Next(50);
                        await Task.Delay(delayMs, cancellationToken).ConfigureAwait(false);
                    }
                    else
                    {
                        failCount = 0;
                    }

                    totalRead += bytesRead;
                } while (totalRead < currentBufferSize);

                // Create a result array that won't be returned to the pool.
                byte[] result = new byte[totalRead];
                Buffer.BlockCopy(rentedBuffer, 0, result, 0, totalRead);

                return result.Success();
            }
            catch (OperationCanceledException) when (cancellationToken.IsCancellationRequested)
            {
                throw;
            }
            catch (Exception ex)
            {
                return Maybe<byte[]>.Fail(new InvalidOperationException("End of file reached while trying to read queue item", ex));
            }
            finally
            {
                // It's a rental, return it!
                if (rentedBuffer != null)
                {
                    ArrayPool<byte>.Shared.Return(rentedBuffer, clearArray: false);
                }
            }
        }

        private void ReadTransactionLog()
        {
            if (_isAsyncMode) throw new Exception("A synchronous method was called but this queue was implemented in async mode.\n Always use the async equivalent operations on queues created asynchronously.");
            var requireTxLogTrimming = false;

            var ok = _file.AtomicRead(TransactionLog, binaryReader =>
            {
                bool readingTransaction = false;
                try
                {
                    int txCount = 0;
                    while (true)
                    {
                        txCount++ ;
						// this code ensures that we read the full transaction
						// before we start to apply it. The last truncated transaction will be
						// ignored automatically.
						var state = AssertTransactionSeparator(binaryReader, txCount, Marker.StartTransaction, () => readingTransaction = true);
                        if (state == SeparatorState.Missing)
                        {
                            if (readingTransaction) requireTxLogTrimming = true;
                            return;
                        }

                        var opsCount = binaryReader.ReadInt32();
                        var txOps = new List<Operation>(opsCount);
                        for (var i = 0; i < opsCount; i++)
                        {
                            AssertOperationSeparator(binaryReader);
                            var operation = new Operation(
                                (OperationType)binaryReader.ReadByte(),
                                binaryReader.ReadInt32(),
                                binaryReader.ReadInt32(),
                                binaryReader.ReadInt32()
                            );
                            txOps.Add(operation);
							//if we have non enqueue entries, this means 
							// that we have not closed properly, so we need
							// to trim the log
							if (operation.Type != OperationType.Enqueue)
                                requireTxLogTrimming = true;
                        }

						// check that the end marker is in place
						state = AssertTransactionSeparator(binaryReader, txCount, Marker.EndTransaction, () => { });
                        if (state == SeparatorState.Missing)
                        {
                            if (readingTransaction) requireTxLogTrimming = true;
                            return;
                        }

                        readingTransaction = false;
                        ApplyTransactionOperations(txOps);
                    }
                }
                catch (EndOfStreamException)
                {
                    if (readingTransaction) requireTxLogTrimming = true;
                }
            });
            if (!ok || requireTxLogTrimming) FlushTrimmedTransactionLog();
        }

        /// <summary>
        /// Asynchronously reads the transaction log and applies the operations found
        /// </summary>
        private async Task ReadTransactionLogAsync(CancellationToken cancellationToken = default)
        {
            var requireTxLogTrimming = false;

            bool ok = await _file.AtomicReadAsync(TransactionLog, async (binaryReader) =>
            {
                bool readingTransaction = false;
                try
                {
                    int txCount = 0;
                    while (true)
                    {
                        cancellationToken.ThrowIfCancellationRequested();
                        txCount++;

                        // This code ensures that we read the full transaction
                        // before we start to apply it. The last truncated transaction will be
                        // ignored automatically.
                        var state = AssertTransactionSeparator(binaryReader, txCount, Marker.StartTransaction, () => readingTransaction = true);
                        if (state == SeparatorState.Missing)
                        {
                            if (readingTransaction) requireTxLogTrimming = true;
                            return;
                        }

                        var opsCount = binaryReader.ReadInt32();
                        var txOps = new List<Operation>(opsCount);
                        for (var i = 0; i < opsCount; i++)
                        {
                            AssertOperationSeparator(binaryReader);
                            var operation = new Operation(
                            (OperationType)binaryReader.ReadByte(),
                            binaryReader.ReadInt32(),
                            binaryReader.ReadInt32(),
                            binaryReader.ReadInt32()
                        );
                            txOps.Add(operation);

                            // If we have non-enqueue entries, this means 
                            // that we have not closed properly, so we need
                            // to trim the log
                            if (operation.Type != OperationType.Enqueue)
                                requireTxLogTrimming = true;
                        }

                        // Check that the end marker is in place
                        state = AssertTransactionSeparator(binaryReader, txCount, Marker.EndTransaction, () => { });
                        if (state == SeparatorState.Missing)
                        {
                            if (readingTransaction) requireTxLogTrimming = true;
                            return;
                        }

                        readingTransaction = false;

                        // Apply operations asynchronously if possible
                        await ApplyTransactionOperationsAsync(txOps, cancellationToken).ConfigureAwait(false);
                    }
                }
                catch (EndOfStreamException)
                {
                    if (readingTransaction) requireTxLogTrimming = true;
                }
                // Task.CompletedTask is needed to satisfy the async delegate signature
                await Task.CompletedTask;
            }, cancellationToken).ConfigureAwait(false);

            // If needed, flush the trimmed transaction log
            if (!ok || requireTxLogTrimming)
            {
                await FlushTrimmedTransactionLogAsync(cancellationToken).ConfigureAwait(false);
            }
        }

        private void FlushTrimmedTransactionLog()
        {
            if (_isAsyncMode) throw new Exception("A synchronous method was called but this queue was implemented in async mode.\n Always use the async equivalent operations on queues created asynchronously.");
            byte[] transactionBuffer;
            using (var ms = new MemoryStream())
            {
                ms.Write(Constants.StartTransactionSeparator, 0, Constants.StartTransactionSeparator.Length);

                var count = BitConverter.GetBytes(EstimatedCountOfItemsInQueue);
                ms.Write(count, 0, count.Length);

                Entry[] checkedOut;
                lock (_checkedOutEntries)
                {
                    checkedOut = _checkedOutEntries.ToArray();
                }
                foreach (var entry in checkedOut)
                {
                    WriteEntryToTransactionLog(ms, entry, OperationType.Enqueue);
                }

                Entry[] listedEntries;
                lock (_entries)
                {
                    listedEntries = ToArray(_entries);
                }

                foreach (var entry in listedEntries)
                {
                    WriteEntryToTransactionLog(ms, entry, OperationType.Enqueue);
                }
                ms.Write(Constants.EndTransactionSeparator, 0, Constants.EndTransactionSeparator.Length);
                ms.Flush();
                transactionBuffer = ms.ToArray();
            }
            _file.AtomicWrite(TransactionLog, stream =>
            {
                stream.Truncate();
                stream.Write(transactionBuffer);
            });
        }

        /// <summary>
        /// Asynchronously flush the trimmed transaction log.
        /// <para>Locks <see cref="_entriesLockAsync"/> and <see cref="_checkedOutEntriesLockAsync"/>.</para>
        /// </summary>
        private async Task FlushTrimmedTransactionLogAsync(CancellationToken cancellationToken = default)
        {
            byte[] transactionBuffer;
            await using (var ms = new MemoryStream())
            {
                ms.Write(Constants.StartTransactionSeparator, 0, Constants.StartTransactionSeparator.Length);

                var count = BitConverter.GetBytes(await GetEstimatedCountOfItemsInQueueAsync(cancellationToken));
                ms.Write(count, 0, count.Length);

                // acquire locks in the same sequence used elsewhere in this class, specifically, deal with _entriesLockAsync then _checkedOutEntriesLockAsync.
                // This helps avoids potential deadlock-like scenarios.
                Entry[] listedEntries;
                try
                {
                    using (await _entriesLockAsync.LockAsync(cancellationToken).ConfigureAwait(false))
                    {
                        _holdsEntriesLock.Value = true;
                        listedEntries = ToArray(_entries);
                    }
                }
                finally
                {
                    _holdsEntriesLock.Value = false;
                }

                foreach (var entry in listedEntries)
                {
                    cancellationToken.ThrowIfCancellationRequested();
                    WriteEntryToTransactionLog(ms, entry, OperationType.Enqueue);
                }

                Entry[] checkedOut;
                try
                {
                    using (await _checkedOutEntriesLockAsync.LockAsync(cancellationToken).ConfigureAwait(false))
                    {
                        _holdsCheckedOutEntriesLock.Value = false;
                        checkedOut = _checkedOutEntries.ToArray();
                    }
                }
                finally
                {
                    _holdsCheckedOutEntriesLock.Value = false;
                }

                foreach (var entry in checkedOut)
                {
                    cancellationToken.ThrowIfCancellationRequested();

                    // Write the entry to the transaction log
                    WriteEntryToTransactionLog(ms, entry, OperationType.Enqueue);
                }

                ms.Write(Constants.EndTransactionSeparator, 0, Constants.EndTransactionSeparator.Length);
                ms.Flush();
                transactionBuffer = ms.ToArray();
            }

            await _file.AtomicWriteAsync(TransactionLog, async writer =>
            {
                writer.Truncate();
                await writer.WriteAsync(transactionBuffer.AsMemory(), cancellationToken);
                await Task.CompletedTask;
            }, cancellationToken).ConfigureAwait(false);
        }

        /// <summary>
        /// This special purpose function is to work around potential issues with Mono
        /// </summary>
	    private static Entry[] ToArray(LinkedList<Entry>? list)
        {
            if (list == null) return [];
            var outp = new List<Entry>(25);
            var cur = list.First;
            while (cur != null)
            {
                outp.Add(cur.Value);
                cur = cur.Next;
            }
            return outp.ToArray();
        }

        private static void WriteEntryToTransactionLog(Stream ms, Entry entry, OperationType operationType)
        {
            ms.Write(Constants.OperationSeparatorBytes);
            ms.WriteByte((byte)operationType);

            Span<byte> buffer = stackalloc byte[12]; // for 3 int32 values
            BitConverter.TryWriteBytes(buffer.Slice(0, 4), entry.FileNumber);
            BitConverter.TryWriteBytes(buffer.Slice(4, 4), entry.Start);
            BitConverter.TryWriteBytes(buffer.Slice(8, 4), entry.Length);

            ms.Write(buffer);
        }

        private void AssertOperationSeparator(IBinaryReader reader)
        {
            var separator = reader.ReadInt32();
            if (separator == Constants.OperationSeparator) return; // OK

            ThrowIfStrict("Unexpected data in transaction log. Expected to get transaction separator but got unknown data");
        }

        /// <summary>
        /// If 'throwOnConflict' was set in the constructor, throw an InvalidOperationException. This will stop program flow.
        /// If not, return a value which should result in silent data truncation.
        /// </summary>
	    private SeparatorState ThrowIfStrict(string msg)
        {
            if (_throwOnConflict)
            {
                throw new UnrecoverableException(msg);
            }

            return SeparatorState.Missing;   // silently truncate transactions
        }

        private SeparatorState AssertTransactionSeparator(IBinaryReader binaryReader, int txCount, Marker whichSeparator, Action hasData)
        {
            var bytes = binaryReader.ReadBytes(16);
            if (bytes.Length == 0) return SeparatorState.Missing;

            hasData();
            if (bytes.Length != 16)
            {
                // looks like we have a truncated transaction in this case, we will 
                // say that we run into end of stream and let the log trimming to deal with this
                if (binaryReader.GetLength() == binaryReader.GetPosition())
                {
                    return SeparatorState.Missing;
                }
                ThrowIfStrict("Unexpected data in transaction log. Expected to get transaction separator but got truncated data. Tx #" + txCount);
            }

            Guid expectedValue, otherValue;
            Marker otherSeparator;
            if (whichSeparator == Marker.StartTransaction)
            {
                expectedValue = Constants.StartTransactionSeparatorGuid;
                otherValue = Constants.EndTransactionSeparatorGuid;
                otherSeparator = Marker.EndTransaction;
            }
            else if (whichSeparator == Marker.EndTransaction)
            {
                expectedValue = Constants.EndTransactionSeparatorGuid;
                otherValue = Constants.StartTransactionSeparatorGuid;
                otherSeparator = Marker.StartTransaction;
            }
            else
            {
                throw new InvalidProgramException("Wrong kind of separator in inner implementation");
            }

            var separator = new Guid(bytes);
            if (separator != expectedValue)
            {
                if (separator == otherValue) // found a marker, but of the wrong type
                {
                    return ThrowIfStrict("Unexpected data in transaction log. Expected " + whichSeparator + " but found " + otherSeparator);
                }
                return ThrowIfStrict("Unexpected data in transaction log. Expected to get transaction separator but got unknown data. Tx #" + txCount);
            }
            return SeparatorState.Present;
        }

        private enum SeparatorState
        {
            Present, Missing
        }

        private void ReadMetaState()
        {
            if (_isAsyncMode) throw new Exception("A synchronous method was called but this queue was implemented in async mode.\n Always use the async equivalent operations on queues created asynchronously.");
            var ok = _file.AtomicRead(Meta, binaryReader =>
            {
                try
                {
                    CurrentFileNumber = binaryReader.ReadInt32();
                    CurrentFilePosition = binaryReader.ReadInt64();
                }
                catch (EndOfStreamException ex)
                {
                    PersistentQueue.Log($"Truncation {ex}");
                }
            });
            if (!ok) PersistentQueue.Log("Could not access meta state");
        }

        /// <summary>
        /// Asynchronously read meta state
        /// </summary>
        private async Task ReadMetaStateAsync(CancellationToken cancellationToken = default)
        {
            bool ok = await _file.AtomicReadAsync(Meta, async reader =>
            {
                try
                {
                    CurrentFileNumber = reader.ReadInt32();
                    CurrentFilePosition = reader.ReadInt64();
                    await Task.CompletedTask;
                }
                catch (EndOfStreamException ex)
                {
                    PersistentQueue.Log($"Truncation {ex}");
                }
            }, cancellationToken).ConfigureAwait(false);

            if (!ok) PersistentQueue.Log("Could not access meta state");
        }

        private void TrimTransactionLogIfNeeded(long txLogSize)
        {
            if (_isAsyncMode) throw new Exception("A synchronous method was called but this queue was implemented in async mode.\n Always use the async equivalent operations on queues created asynchronously.");
            if (txLogSize < SuggestedMaxTransactionLogSize) return; // it is not big enough to care

            var optimalSize = GetOptimalTransactionLogSize();
            if (txLogSize < (optimalSize * 2)) return;  // not enough disparity to bother trimming

            FlushTrimmedTransactionLog();
        }

        private async Task TrimTransactionLogIfNeededAsync(long txLogSize, CancellationToken cancellationToken)
        {
            if (txLogSize < SuggestedMaxTransactionLogSize) return; // it is not big enough to care

            var optimalSize = await GetOptimalTransactionLogSizeAsync(cancellationToken);
            if (txLogSize < (optimalSize * 2)) return;  // not enough disparity to bother trimming

            await FlushTrimmedTransactionLogAsync(cancellationToken);
        }

        private void ApplyTransactionOperations(IEnumerable<Operation> operations)
        {
            if (_isAsyncMode) throw new Exception("A synchronous method was called but this queue was implemented in async mode.\n Always use the async equivalent operations on queues created asynchronously.");
            lock (_entries)
            {
                var filesToRemove = ApplyTransactionOperationsInMemory(operations);
                foreach (var fileNumber in filesToRemove)
                {
                    if (CurrentFileNumber == fileNumber)
                        continue;

                    _file.PrepareDelete(GetDataPath(fileNumber));
                }
            }
            _file.Finalise();
        }

        /// <summary>
        /// Apply transaction operations asynchronously
        /// <para>Locks <see cref="_entriesLockAsync"/></para>
        /// </summary>
        private async Task ApplyTransactionOperationsAsync(IEnumerable<Operation> operations, CancellationToken cancellationToken = default)
        {
            try
            {
                using (await _entriesLockAsync.LockAsync(cancellationToken).ConfigureAwait(false))
                {
                    _holdsEntriesLock.Value = true;
                    var filesToRemove = await ApplyTransactionOperationsInMemoryAsync(operations, cancellationToken);
                    foreach (var fileNumber in filesToRemove)
                    {
                        cancellationToken.ThrowIfCancellationRequested();
                        if (CurrentFileNumber == fileNumber)
                            continue;

                        await _file.PrepareDeleteAsync(GetDataPath(fileNumber), cancellationToken);
                    }
                }
            }
            finally
            {
                _holdsEntriesLock.Value = false;
            }

            await _file.FinaliseAsync(cancellationToken).ConfigureAwait(false);
        }

        private static byte[] GenerateTransactionBuffer(ICollection<Operation> operations)
        {
            // TODO: implement array pool and buffer rental here.
            using var ms = new MemoryStream();
            ms.Write(Constants.StartTransactionSeparator, 0, Constants.StartTransactionSeparator.Length);

            var count = BitConverter.GetBytes(operations.Count);
            ms.Write(count, 0, count.Length);

            foreach (var operation in operations)
            {
                WriteEntryToTransactionLog(ms, new Entry(operation), operation.Type);
            }
            ms.Write(Constants.EndTransactionSeparator, 0, Constants.EndTransactionSeparator.Length);

            ms.Flush();
            var transactionBuffer = ms.ToArray();
            return transactionBuffer;
        }

        protected IFileStream CreateWriter()
        {
            if (_isAsyncMode) throw new Exception("A synchronous method was called but this queue was implemented in async mode.\n Always use the async equivalent operations on queues created asynchronously.");
            var dataFilePath = GetDataPath(CurrentFileNumber);
            return _file.OpenWriteStream(dataFilePath);
        }

        protected async Task<IFileStream> CreateWriterAsync(CancellationToken cancellationToken)
        {
            var dataFilePath = GetDataPath(CurrentFileNumber);
            return await _file.OpenWriteStreamAsync(dataFilePath, cancellationToken);
        }

        private string GetDataPath(int index)
        {
            return _file.PathCombine(_path, "data." + index);
        }

        private long GetOptimalTransactionLogSize()
        {
            if (_isAsyncMode) throw new Exception("A synchronous method was called but this queue was implemented in async mode.\n Always use the async equivalent operations on queues created asynchronously.");
            long size = 0;
            size += 16 /*sizeof(guid)*/; //	initial tx separator
            size += sizeof(int); // 	operation count

            size +=
                ( // entry size == 16
                sizeof(int) + // 		operation separator
                sizeof(int) + // 		file number
                sizeof(int) + //		start
                sizeof(int)   //		length
                )
                *
                CurrentCountOfItemsInQueue;

            return size;
        }

        private async Task<long> GetOptimalTransactionLogSizeAsync(CancellationToken cancellationToken)
        {
            long size = 0;
            size += 16 /*sizeof(guid)*/; //	initial tx separator
            size += sizeof(int); // 	operation count

            size +=
                ( // entry size == 16
                sizeof(int) + // 		operation separator
                sizeof(int) + // 		file number
                sizeof(int) + //		start
                sizeof(int)   //		length
                )
                *
                await GetCurrentCountOfItemsInQueueAsync(cancellationToken);

            return size;
        }

        // Simple helper method that doesn't require a branch for each call
        // This is used internally and has minimal overhead
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        private void EnsureInitialized()
        {
            // This will get JIT-optimized and potentially eliminated in release builds
            // when _isInitialized is true.
            if (!_isInitialized)
            {
                ThrowNotInitialized();
            }
        }

        // We move the throw to a separate method to keep EnsureInitialized small
        // for better inlining potential
        [MethodImpl(MethodImplOptions.NoInlining)]
        private static void ThrowNotInitialized()
        {
            throw new InvalidOperationException("Queue was not properly initialized");
        }
    }
}