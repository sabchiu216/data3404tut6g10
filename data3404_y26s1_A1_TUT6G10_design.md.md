# Design Document – LRU Buffer Replacement

## Goal
The goal is to implement an LRU buffer replacement policy to replace the existing RandomReplacer.

## Existing System
SimpleDB uses a BufferManager with pluggable replacement policies.
Frames support pin/unpin and dirty page flushing.

## Design Overview
We implemented a new class `LRUReplacer` following the existing `Replacer` interface.

The LRU policy:
- maintains an ordered list of frames
- moves accessed frames to the end
- selects the least recently used unpinned frame

## Methods

### notify(pool, frame)
Updates the usage order whenever a frame is accessed.

### choose(pool)
Selects the least recently used frame that is not pinned.

## Integration
LRUReplacer replaces RandomReplacer in DatabaseManager.
No major changes were required in BufferManager.

## Design Decisions
- Used a Python list for simplicity
- Avoided complex structures (e.g. linked list)
- Focused on correctness and clarity

## Limitations
- O(n) operations for lookup
- Not optimised for large buffer pools