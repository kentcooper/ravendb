﻿using System.Runtime.InteropServices;

namespace Voron.Impl.FileHeaders
{
    [StructLayout(LayoutKind.Explicit, Pack = 1)]
    public struct TreeRootHeader
    {
        [FieldOffset(0)]
        public RootObjectType RootObjectType;
        [FieldOffset(1)]
        public TreeFlags Flags;
        [FieldOffset(2)]
        public long RootPageNumber;
        [FieldOffset(10)]
        public long BranchPages;
        [FieldOffset(18)]
        public long LeafPages;
        [FieldOffset(34)]
        public long OverflowPages;
        [FieldOffset(42)]
        public long PageCount;
        [FieldOffset(50)]
        public long NumberOfEntries;
        [FieldOffset(58)]
        public int Depth;
    }
}