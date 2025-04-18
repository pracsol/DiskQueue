// Copyright (c) 2005 - 2008 Ayende Rahien (ayende@ayende.com)
// All rights reserved.
// 
// Redistribution and use in source and binary forms, with or without modification,
// are permitted provided that the following conditions are met:
// 
//     * Redistributions of source code must retain the above copyright notice,
//     this list of conditions and the following disclaimer.
//     * Redistributions in binary form must reproduce the above copyright notice,
//     this list of conditions and the following disclaimer in the documentation
//     and/or other materials provided with the distribution.
//     * Neither the name of Ayende Rahien nor the names of its
//     contributors may be used to endorse or promote products derived from this
//     software without specific prior written permission.
// 
// THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS" AND
// ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE IMPLIED
// WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE ARE
// DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT OWNER OR CONTRIBUTORS BE LIABLE
// FOR ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR CONSEQUENTIAL
// DAMAGES (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR
// SERVICES; LOSS OF USE, DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER
// CAUSED AND ON ANY THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY,
// OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE OF
// THIS SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.

using System;
using System.Text;

namespace ModernDiskQueue.Implementation
{
    /// <summary>
    /// Exception thrown when data can't be persisted
    /// </summary>
    public class PendingWriteException : Exception
    {
        private readonly Exception[] _pendingWritesExceptions;

        /// <summary>
        /// Initializes a new instance of the PendingWriteException class
        /// </summary>
        public PendingWriteException()
            : base("Error during pending writes")
        {
            _pendingWritesExceptions = [];
        }

        /// <summary>
        /// Initializes a new instance of the PendingWriteException class with a specified error message
        /// </summary>
        public PendingWriteException(string message)
            : base(message)
        {
            _pendingWritesExceptions = [];
        }

        /// <summary>
        /// Initializes a new instance of the PendingWriteException class with a specified error message
        /// and a reference to the inner exception that is the cause of this exception
        /// </summary>
        public PendingWriteException(string message, Exception innerException)
            : base(message, innerException)
        {
            _pendingWritesExceptions = [innerException];
        }

        /// <summary>
        /// Aggregate causing exceptions
        /// </summary>
        public PendingWriteException(Exception[] pendingWritesExceptions)
            : base("Error during pending writes")
        {
            _pendingWritesExceptions = pendingWritesExceptions ?? throw new ArgumentNullException(nameof(pendingWritesExceptions));
        }

        /// <summary>
        /// Set of causing exceptions
        /// </summary>
        public Exception[] PendingWritesExceptions => _pendingWritesExceptions;

        /// <summary>
        /// Gets a message that describes the current exception.
        /// </summary>
        public override string Message
        {
            get
            {
                var sb = new StringBuilder(base.Message ?? "Error").Append(':');
                foreach (var exception in _pendingWritesExceptions)
                {
                    sb.AppendLine().Append(" - ").Append(exception.Message ?? "<unknown>");
                }
                return sb.ToString();
            }
        }

        /// <summary>
        /// Creates and returns a string representation of the current exception.
        /// </summary>
        public override string ToString()
        {
            var sb = new StringBuilder(base.Message ?? "Error").Append(':');
            foreach (var exception in _pendingWritesExceptions)
            {
                sb.AppendLine().Append(" - ").Append(exception);
            }
            return sb.ToString();
        }

        // Helper class for JSON structure
        private class ExceptionData
        {
            public string? BaseMessage { get; set; }
            public ExceptionDetails[]? PendingWritesExceptions { get; set; }
        }

        private class ExceptionDetails
        {
            public string? Message { get; set; }
            public string? StackTrace { get; set; }
            public string? InnerException { get; set; }
        }
    }
}