# Design Document – LRU Buffer Replacement

## Goal
The goal of this assignment is to implement an LRU (Least Recently Used) buffer replacement policy to replace the existing RandomReplacer in SimpleDB.

## Existing System
SimpleDB uses a BufferManager that manages a pool of buffer frames.

Key components:
- `BufferManager`: manages page loading, eviction, and flushing
- `BufferFrame`: represents a page in memory
- `Replacer`: interface for replacement strategies
- `RandomReplacer`: default implementation

Important behaviours:
- Pages can be pinned/unpinned
- Dirty pages are flushed before eviction
- Replacement policy is pluggable

---

## Design Overview
We implemented a new class `LRUReplacer` that follows the existing `Replacer` interface.

The LRU policy:
- maintains an ordered list of frames
- moves recently accessed frames to the end
- selects the least recently used unpinned frame for eviction


## Relevant Files and Changes

### New file:
- `simpledb/buffer/replacement/lru_replacer.py`

### Modified files:
- `simpledb/main/database_manager.py`
  - replaced `RandomReplacer` with `LRUReplacer`


### Test files:
- `tests/test_buffer/test_lru_replacer.py`

---

## Implementation Details

### Data Structure
We used a Python list to maintain the access order of frames.

- Most recently used → end of list
- Least recently used → beginning of list

### notify()
- Called when a frame is accessed
- Moves the frame to the end of the list

### choose()
- Iterates through frames in order
- Selects the first frame that is not pinned
- Throws exception if all frames are pinned

---

## Design Decisions

- Chose LRU over Random because it considers access patterns
- Used a list instead of more complex structures for simplicity
- Reused existing `Replacer` interface to avoid modifying core system logic
- Relied on BufferManager to handle dirty pages and flushing



## Integration with Existing System

The new LRUReplacer integrates seamlessly because:
- It follows the same interface as RandomReplacer
- BufferManager already supports pluggable policies
- No major changes were required in core logic

This ensures minimal disruption to existing functionality.

---

## Testing and Verification Plan

We verified correctness using unit tests:

Test cases include:
- correct victim selection (LRU behaviour)
- recently accessed frame moves to end
- pinned frames are not selected
- all frames pinned → exception raised

Additionally:
- all existing system tests were executed
- all tests passed successfully

---

## Limitations

- List-based implementation results in O(n) operations
- Not optimal for large buffer sizes
- No concurrency handling

---

## Future Improvements

- Implement CLOCK or G-CLOCK policies
- Use more efficient data structures (e.g. linked list + hash map)
- Add performance benchmarking