# DATA3404 Assignment 1 – LRU Buffer Replacement

## Option Chosen
Option 1: Buffer Replacement Policy (LRU)

## Overview
This project extends SimpleDB by implementing an LRU (Least Recently Used) buffer replacement policy.

## Implementation
A new class `LRUReplacer` was added under:
simpledb/buffer/replacement/

It follows the existing `Replacer` interface and replaces the default RandomReplacer.

## Algorithm Description
- When a frame is accessed, it is moved to the end of the list (most recently used)
- The front of the list represents the least recently used frames
- The system selects the first unpinned frame from the front for replacement

## Features
- Tracks frame usage order
- Selects least recently used unpinned frame
- Works with BufferManager without major modifications
- Compatible with dirty page flushing

## How to Run

Run demo:
python3 -B -m simpledb.run.demo

Run tests:
python3 -B -m unittest discover -s tests -p "test_*.py" -v

## Testing

Unit tests verify:

- correct frame ordering
- correct victim selection
- pinned frames are skipped
- exception when all frames are pinned

All existing tests passed successfully.

## Files Modified
- simpledb/buffer/replacement/lru_replacer.py
- simpledb/main/database_manager.py
- tests/test_buffer/test_lru_replacer.py