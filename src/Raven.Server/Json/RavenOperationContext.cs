﻿using System;
using System.Collections.Generic;
using System.IO;
using System.Text;
using Bond;
using Microsoft.Extensions.DependencyInjection;
using Newtonsoft.Json;
using Sparrow;
using Sparrow.Binary;
//using Raven.Imports.Newtonsoft.Json;
using Voron.Impl;
using Voron.Util;
using Marshal = System.Runtime.InteropServices.Marshal;

namespace Raven.Server.Json
{
    /// <summary>
    /// Single threaded for contexts
    /// </summary>
    public unsafe class RavenOperationContext : IDisposable
    {
        private LinkedList<UnmanagedBuffersPool.AllocatedMemoryData>[] _allocatedMemory;

        public readonly UnmanagedBuffersPool Pool;
        private UnmanagedBuffersPool.AllocatedMemoryData _tempBuffer;
        private Dictionary<string, LazyStringValue> _fieldNames;
        private Dictionary<LazyStringValue, LazyStringValue> _internedFieldNames;
        private Dictionary<string, byte[]> _fieldNamesAsByteArrays;
        private bool _disposed;

        private byte[] _bytesBuffer;

        public LZ4 Lz4 = new LZ4();
        public UTF8Encoding Encoding;
        public Transaction Transaction;
        public CachedProperties CachedProperties;

        public RavenOperationContext(UnmanagedBuffersPool pool)
        {
            Pool = pool;
            Encoding = new UTF8Encoding();
            CachedProperties = new CachedProperties(this);
        }

        public byte[] GetManagedBuffer()
        {
            if (_bytesBuffer == null)
                _bytesBuffer = new byte[4096];
            return _bytesBuffer;
        }

        /// <summary>
        /// Returns memory buffer to work with, be aware, this buffer is not thread safe
        /// </summary>
        /// <param name="requestedSize"></param>
        /// <param name="actualSize"></param>
        /// <returns></returns>
        public byte* GetNativeTempBuffer(int requestedSize, out int actualSize)
        {
            if (requestedSize == 0)
                throw new ArgumentException(nameof(requestedSize));

            if (_tempBuffer == null)
            {
                _tempBuffer = GetMemory(requestedSize);
            }
            else if (requestedSize > _tempBuffer.SizeInBytes)
            {
                ReturnMemory(_tempBuffer);
                _tempBuffer = GetMemory(requestedSize);
            }

            actualSize = _tempBuffer.SizeInBytes;
            return (byte*)_tempBuffer.Address;
        }

        public UnmanagedBuffersPool.AllocatedMemoryData GetMemory(int requestedSize)
        {
            if (requestedSize == 0)
                return new UnmanagedBuffersPool.AllocatedMemoryData
                {
                    Address = IntPtr.Zero,
                    SizeInBytes = 0
                };

            var actualSize = Bits.NextPowerOf2(requestedSize);
            var index = UnmanagedBuffersPool.GetIndexFromSize(actualSize);
            if (index == -1)
            {
                return Pool.Allocate(requestedSize);
            }

            if (_allocatedMemory?[index] == null ||
                _allocatedMemory[index].Count == 0)
                return Pool.Allocate(actualSize);
            var last = _allocatedMemory[index].Last.Value;
            _allocatedMemory[index].RemoveLast();
            return last;
        }

        public void ReturnMemory(UnmanagedBuffersPool.AllocatedMemoryData buffer)
        {
            if (buffer.SizeInBytes == 0)
                return;

            if (_allocatedMemory == null)
                _allocatedMemory = new LinkedList<UnmanagedBuffersPool.AllocatedMemoryData>[16];
            var index = UnmanagedBuffersPool.GetIndexFromSize(buffer.SizeInBytes);
            if (index == -1)
            {
                Pool.Return(buffer);
                return;
            }
            if (_allocatedMemory[index] == null)
                _allocatedMemory[index] = new LinkedList<UnmanagedBuffersPool.AllocatedMemoryData>();
            _allocatedMemory[index].AddLast(buffer);
        }

        /// <summary>
        /// Generates new unmanaged stream. Should be disposed at the end of the usage.
        /// </summary>
        /// <param name="documentId"></param>
        public UnmanagedWriteBuffer GetStream(string documentId)
        {
            return new UnmanagedWriteBuffer(this);
        }

        public void Dispose()
        {
            if (_disposed)
                return;
            Lz4.Dispose();
            if (_tempBuffer != null)
                Pool.Return(_tempBuffer);
            if (_fieldNames != null)
            {
                foreach (var kvp in _fieldNames.Values)
                {
                    Pool.Return(kvp.AllocatedMemoryData);
                }
            }
            if (_internedFieldNames != null)
            {
                foreach (var key in _internedFieldNames.Keys)
                {
                    Pool.Return(key.AllocatedMemoryData);

                }
            }
            _disposed = true;
        }

        public LazyStringValue GetComparerFor(string field)
        {
            LazyStringValue value;
            if (_fieldNames == null)
                _fieldNames = new Dictionary<string, LazyStringValue>();

            if (_fieldNames.TryGetValue(field, out value))
                return value;

            var maxByteCount = Encoding.GetMaxByteCount(field.Length);
            var memory = GetMemory(maxByteCount);
            try
            {
                fixed (char* pField = field)
                {
                    var address = (byte*)memory.Address;
                    var actualSize = Encoding.GetBytes(pField, field.Length, address, memory.SizeInBytes);
                    _fieldNames[field] = value = new LazyStringValue(field, address, actualSize, this)
                    {
                        AllocatedMemoryData = memory
                    };
                }

            }
            catch (Exception)
            {
                ReturnMemory(memory);
                throw;
            }
            return value;
        }

        public LazyStringValue Intern(LazyStringValue val)
        {
            LazyStringValue value;
            if (_internedFieldNames == null)
                _internedFieldNames = new Dictionary<LazyStringValue, LazyStringValue>();

            if (_internedFieldNames.TryGetValue(val, out value))
                return value;

            var memory = GetMemory(val.Size);
            try
            {
                var address = (byte*)memory.Address;
                Memory.Copy(address, val.Buffer, val.Size);
                value = new LazyStringValue(null, address, val.Size, this)
                {
                    EscapePositions = val.EscapePositions,
                    AllocatedMemoryData = memory
                };
                _internedFieldNames[value] = value;
                return value;

            }
            catch (Exception)
            {
                ReturnMemory(memory);
                throw;
            }
        }

        public byte[] GetBytesForFieldName(string field)
        {
            if (_fieldNamesAsByteArrays == null)
                _fieldNamesAsByteArrays = new Dictionary<string, byte[]>();

            byte[] returnedByteArray;

            if (_fieldNamesAsByteArrays.TryGetValue(field, out returnedByteArray))
            {
                return returnedByteArray;
            }
            returnedByteArray = Encoding.GetBytes(field);
            _fieldNamesAsByteArrays.Add(field, returnedByteArray);
            return returnedByteArray;
        }

        public BlittableJsonWriter Read(Stream stream, string documentId)
        {
            using (var parser = new UnmanagedJsonParser(stream, this))
            {
                var writer = new BlittableJsonWriter(parser, this, documentId);
                try
                {
                    CachedProperties.NewDocument();
                    writer.Run();
                    return writer;
                }
                catch (Exception)
                {
                    writer.Dispose();
                    throw;
                }
            }
        }

    }
}