import cooccurrence_pb2
import numpy as np
import matplotlib.pyplot as plt
import os

def load_map(filename):
    with open(filename, 'rb') as f:
        data = f.read()

    outer_maps = cooccurrence_pb2.OuterMaps()
    outer_maps.ParseFromString(data)

    result = {}
    for outer in outer_maps.outer_map:
        outer_key = outer.key.hex()  # Convert bytes to the appropriate format
        inner_map = {}
        for inner in outer.inner_map:
            inner_key = inner.key.hex()  # Convert bytes to the appropriate format
            inner_map[inner_key] = inner.value
        result[outer_key] = inner_map

    return result

def create_heatmap(data):
    keys = list(data.keys())
    size = len(keys)
    heatmap_data = np.zeros((size, size))
    
    for i, outer_key in enumerate(keys):
        for j, inner_key in enumerate(keys):
            if i == j:
                heatmap_data[i, j] = 5
            # if j > i:
                continue
            if inner_key in data[outer_key]:
                heatmap_data[i, j] = data[outer_key][inner_key] / 1e9
                
                
    for i, outer_key in enumerate(heatmap_data):
        for j, inner_key in enumerate(heatmap_data):
            if heatmap_data[i, j] != heatmap_data[j, i]:
                print("dhsdfhdfs")
                break
        

    plt.imshow(heatmap_data, cmap='hot', interpolation='nearest')
    
    cb = plt.colorbar()
    cb.set_label('Overlap Time (seconds)')
    plt.xticks(ticks=np.arange(size), labels=keys, rotation=90)
    plt.yticks(ticks=np.arange(size), labels=keys)
    plt.tick_params(axis='both', labelsize=3)  # Set the size of the tick labels
    plt.xlabel("Transport IDs (per TCP session)")
    plt.ylabel("Transport IDs (per TCP session)")

    plt.title('Heatmap of Co-occurrences')
    plt.tight_layout()

    # Save the heatmap as an image
    plt.savefig("../images/heatmap.png", dpi=300)  # Adjust the dpi value as needed
    plt.close()  # Close the plot to free memory

# Example usage
if __name__ == "__main__":
    filename = "../data/cooccurrence_data_small.pb"  # Replace with your actual file name
    loaded_data = load_map(filename)
    # print("Loaded data:", loaded_data)

    # Create the heatmap
    create_heatmap(loaded_data)
