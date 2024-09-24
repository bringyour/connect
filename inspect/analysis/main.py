import cooccurrence_pb2
import numpy as np
import matplotlib.pyplot as plt
import os

import numpy as np
from sklearn.cluster import OPTICS
from scipy.spatial.distance import pdist, squareform
import seaborn as sns

ENABLE_LABELS = False


def load_map(filename):
    with open(filename, "rb") as f:
        data = f.read()

    outer_maps = cooccurrence_pb2.OuterMaps()
    outer_maps.ParseFromString(data)

    max_value = -1

    result = {}
    for outer in outer_maps.outer_map:
        outer_key = outer.key.hex()
        inner_map = {}
        for inner in outer.inner_map:
            inner_key = inner.key.hex()
            inner_map[inner_key] = inner.value
            if inner.value > max_value:
                max_value = inner.value
        result[outer_key] = inner_map

    return result, max_value


def create_heatmap(data, max_value):
    keys = list(data.keys())
    size = len(keys)
    heatmap_data = np.ones((size, size))

    for i, key_i in enumerate(keys):
        for j, key_j in enumerate(keys):
            if key_j in data[key_i]:
                if heatmap_data[i, j] + heatmap_data[j, i] < 2:
                    # non-zero value exists for data[key_i][key_j] and data[key_j][key_i]
                    print(f"Duplicate entry found for {key_i} and {key_j}")
                    return
                large = max(i, j)
                small = min(i, j)
                # time_in_sec = data[key_i][key_j]  # / 1e9
                # final_val = 1 / (time_in_sec + 1e-9)
                final_val = data[key_i][key_j] / max_value
                heatmap_data[large, small] = final_val
                heatmap_data[small, large] = final_val

    optics = OPTICS(
        min_samples=10, metric="precomputed"  # , max_eps=0.97
    )  # use 'precomputed' since we're providing a distance matrix
    optics.fit(heatmap_data)

    labels = optics.labels_  # Cluster labels for each ID
    # for i, label in enumerate(labels):
    #     print(f"ID {keys[i]} is in cluster {label}")

    clusters = {}
    for id_, cluster_id in enumerate(labels):
        if cluster_id not in clusters:
            clusters[cluster_id] = []
        clusters[cluster_id].append(id_)

    print(clusters)

    # Reorder the shared time matrix
    ordered_ids = []
    for cluster_id in sorted(clusters.keys()):
        ordered_ids.extend(clusters[cluster_id])

    reordered_matrix = heatmap_data[np.ix_(ordered_ids, ordered_ids)]
    plt.imshow(reordered_matrix, cmap="hot", interpolation="nearest")

    cb = plt.colorbar()
    cb.set_label("Overlap time (seconds)")
    if ENABLE_LABELS:
        truncated_keys = []
        for cluster_id in sorted(clusters.keys()):
            for id_ in clusters[cluster_id]:
                truncated_keys.append(f"…{keys[id_][-5:]}({cluster_id})")
        plt.xticks(ticks=np.arange(size), labels=truncated_keys, rotation=90)
        plt.yticks(ticks=np.arange(size), labels=truncated_keys)
        plt.tick_params(axis="both", labelsize=4)
    xy_label = "Truncated transport IDs (per TCP session)"
    plt.xlabel(xy_label)
    plt.ylabel(xy_label)

    plt.title("Heatmap of Co-occurrences")
    plt.tight_layout()

    plt.savefig("../images/heatmap.png", dpi=300)
    plt.close()


if __name__ == "__main__":
    filename = "../data/cooccurrence_data_2k.pb"
    # filename = "../data/cooccurrence_data_small.pb"
    loaded_data, max_value = load_map(filename)
    create_heatmap(loaded_data, max_value)
