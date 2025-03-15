import multiprocessing
from collections import defaultdict

def mapper(text_chunk):
    word_counts = defaultdict(int)
    for word in text_chunk.split():
        word = word.lower().strip(".,!?;:()[]{}\"'")  # Normalize words
        word_counts[word] += 1
    return list(word_counts.items())  # Convert dict_items to list before returning

def shuffle(mapped_values):
    shuffled = defaultdict(list)
    for word, count in mapped_values:
        shuffled[word].append(count)
    return shuffled

def reducer(shuffled_values):
    reduced_counts = {word: sum(counts) for word, counts in shuffled_values.items()}
    return reduced_counts

def parallel_map_reduce(file_path, num_workers=4, chunk_size=1024*1024):  # 1MB chunks
    mapped_results = []
    with open(file_path, "r", encoding="utf-8", errors="ignore") as f:
        with multiprocessing.Pool(num_workers) as pool:
            mapped_results = pool.map(mapper, iter(lambda: f.read(chunk_size), ""))

    shuffled_results = shuffle([item for sublist in mapped_results for item in sublist])
    reduced_results = reducer(shuffled_results)

    return reduced_results

# Run word count on the dataset
if __name__ == "__main__":
    word_counts = parallel_map_reduce("/Users/saiteja/Downloads/wordcount/pagecounts-20160101-000000", num_workers=4)
    for word, count in sorted(word_counts.items(), key=lambda x: -x[1])[:20]:  # Display top 20 words
        print(f"{word}: {count}")
