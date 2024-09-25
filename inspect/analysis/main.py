import cooccurrence_pb2
import numpy as np
import matplotlib.pyplot as plt
import os

import numpy as np
from sklearn.cluster import OPTICS
from scipy.spatial.distance import pdist, squareform
import seaborn as sns

ENABLE_LABELS = True


def load_map(filename1, filename2):
    with open(filename1, "rb") as f:
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

    with open(filename2, "rb") as f:
        cluster_data = f.read()

    clusters = cooccurrence_pb2.Clusters()
    clusters.ParseFromString(cluster_data)
    cluster_result = {}
    for clust in clusters.cluster:
        cluster_result[clust.clusterID] = [
            transport.hex() for transport in clust.transports.transportID
        ]

    return result, max_value, cluster_result


def create_heatmap(data, max_value, clusters):
    keys = list(data.keys())
    size = len(keys)
    heatmap_data = np.ones((size, size))

    for i, key_i in enumerate(keys):
        if key_i not in data:
            continue
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

    # optics = OPTICS(
    #     min_samples=10, metric="precomputed"  # , max_eps=0.97
    # )  # use 'precomputed' since we're providing a distance matrix
    # optics.fit(heatmap_data)

    # labels = optics.labels_  # Cluster labels for each ID
    # # for i, label in enumerate(labels):
    # #     print(f"ID {keys[i]} is in cluster {label}")

    # clusters = {}
    # for id_, cluster_id in enumerate(labels):
    #     if cluster_id not in clusters:
    #         clusters[cluster_id] = []
    #     clusters[cluster_id].append(id_)

    # print(clusters)

    # Reorder the shared time matrix
    ordered_ids = []
    for cluster_id in sorted(clusters.keys()):
        ordered_ids.extend(clusters[cluster_id])

    # print(ordered_ids)
    # print()
    # print(keys)
    # exit(0)

    final_order_ids = []
    for oid in ordered_ids:
        was_set = False
        for i, key in enumerate(keys):
            if key == oid:
                final_order_ids.append(i)
                was_set = True
                break
        if not was_set:
            print(f"{oid} not in keys")

    reordered_matrix = heatmap_data[np.ix_(final_order_ids, final_order_ids)]
    plt.imshow(reordered_matrix, cmap="hot", interpolation="nearest")

    cb = plt.colorbar()
    cb.set_label("Overlap time (seconds)")
    if ENABLE_LABELS:
        truncated_keys = []
        for cluster_id in sorted(clusters.keys()):
            i = 0
            for id_ in clusters[cluster_id]:
                if i == 0:
                    fids = cluster_id
                    if cluster_id == 18446744073709551615:
                        fids = -1
                    truncated_keys.append(f"{int(fids)}")
                else:
                    truncated_keys.append(f"")
                i += 1
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
    # filename1 = "../data/cooccurrence_data_2k.pb"
    # filename2 = "../data/cluster_data_2k.pb"
    filename1 = "../data/cooccurrence_data_small.pb"
    filename2 = "../data/cluster_data_small.pb"
    loaded_data, max_value, clusters = load_map(filename1, filename2)
    create_heatmap(loaded_data, max_value, clusters)
