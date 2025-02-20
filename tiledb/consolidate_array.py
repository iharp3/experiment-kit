import tiledb

array = "/data/iharp-customized-storage/storage/experiments_tdb"

# fragments = tiledb.array_fragments(array)
# print(f"Total fragments: {len(fragments)}")

config = tiledb.Config()
config["sm.consolidation.buffer_size"] = str(50 * 1024 * 1024)  # 50MB buffer

# Get list of fragments
fragments = tiledb.array_fragments(array)
num_fragments = len(fragments)

# Define batch size: Start with merging 5-10 fragments at a time
batch_size = min(20, num_fragments // 20)  # 10 fragments or ~10% of total

print(f"Total fragments: {num_fragments}")
print(f"Consolidating in batches of {batch_size}...")

for i in range(0, num_fragments, batch_size):
    selected_fragments = fragments[i : i + batch_size]  # Select a small batch
    print(f"Consolidating fragments {i+1} to {i+batch_size}...")

    tiledb.consolidate(array, config=config, fragments=selected_fragments)
    tiledb.vacuum(array, config=config)  # Clean up old fragments

    # # Check if storage is reduced significantly
    # num_remaining_fragments = len(tiledb.array_fragments(array))
    # print(f"Remaining fragments: {num_remaining_fragments}")
    
    # if num_remaining_fragments < 10:  # Stop if few fragments remain
    #     print("Fragmentation reduced enough. Stopping.")
    #     break