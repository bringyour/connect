import sys
import cooccurrence_pb2
import numpy as np
import matplotlib.pyplot as plt
import os
import statistics
import matplotlib.cm as cm
import math

import numpy as np
from sklearn.cluster import OPTICS, HDBSCAN
from sklearn.manifold import TSNE
from scipy.spatial.distance import pdist, squareform
import seaborn as sns


def load_map(filename1):
    with open(filename1, "rb") as f:
        data = f.read()

    cooc_data = cooccurrence_pb2.CooccurrenceData()
    cooc_data.ParseFromString(data)

    max_overlap = -1
    result = {}
    for outer in cooc_data.cooc_outer:
        outer_sid = outer.sid  # .hex()
        inner_dict = {}
        for inner in outer.cooc_inner:
            inner_sid = inner.sid  # .hex()
            inner_dict[inner_sid] = inner.overlap
            if inner.overlap > max_overlap:
                max_overlap = inner.overlap
        result[outer_sid] = inner_dict

    return result, max_overlap


def heatmap_labels(clusters, only_first=True, sids=None):
    new_labels = []
    for cluster_id in sorted(clusters.keys()):
        if only_first:
            # add cluster id only once
            cluster_labels = [""] * len(clusters[cluster_id])
            cluster_labels[0] = f"{int(cluster_id)}"
            new_labels.extend(cluster_labels)
        else:
            # add truncated transport IDs
            for idx in clusters[cluster_id]:
                # new_labels.append(f"…{sids[idx][-5:]}")
                new_labels.append(f"{sids[idx]}({cluster_id})")
                # new_labels.append(cluster_id)
    return new_labels


def stats(list_of_lists):
    # Flatten the list of lists
    flat_list = [item for sublist in list_of_lists for item in sublist]

    # Using the statistics module for basic statistics
    mean = statistics.mean(flat_list)
    median = statistics.median(flat_list)
    variance = statistics.variance(flat_list)
    stdev = statistics.stdev(flat_list)

    # Using numpy for other statistics
    min_val = np.min(flat_list)
    max_val = np.max(flat_list)
    percentiles = np.percentile(flat_list, [25, 50, 75])  # 25th, 50th, 75th percentiles

    # Print the statistics
    print()
    print("Statistics about heatmap:")
    print(f"  Dimensions: {len(list_of_lists)}x{len(list_of_lists)}")
    print(f"  Mean: {mean}")
    print(f"  Median: {median}")
    print(f"  Variance: {variance}")
    print(f"  Standard Deviation: {stdev}")
    print(f"  Min: {min_val}")
    print(f"  Max: {max_val}")
    print(f"  25th, 50th, 75th Percentiles: {percentiles}")


def graph_data(sids, data, clusters, print_stats):
    heatmap_data = np.ones(
        (len(sids), len(sids))
    )  # fill with ones as max distance values

    for i, sid_1 in enumerate(sids):
        if sid_1 not in data:
            continue
        for j, sid_j in enumerate(sids):
            if sid_j in data[sid_1]:
                if heatmap_data[i, j] + heatmap_data[j, i] < 2:
                    # non-zero value exists for data[sid_j][sid_j] and data[sid_j][sid_j]
                    print(f"Duplicate entry found for {sid_1} and {sid_j}")
                    return
                final_val = overlap_to_distance(
                    data[sid_1][sid_j], max_overlap
                )  # values are from 0 to 1
                large = max(i, j)
                small = min(i, j)
                heatmap_data[large, small] = final_val
                heatmap_data[small, large] = final_val

    if print_stats:
        stats(heatmap_data)

    # reorder based on the formed clusters
    ordered_ids = []
    for cluster_id in sorted(clusters.keys()):
        ordered_ids.extend(clusters[cluster_id])

    reordered_matrix = heatmap_data[np.ix_(ordered_ids, ordered_ids)]
    plt.imshow(reordered_matrix, cmap="hot", interpolation="nearest")

    cb = plt.colorbar()
    cb.set_label("Overlap time (seconds)")
    truncated_labels = heatmap_labels(clusters)  # , False, sids)
    plt.xticks(ticks=np.arange(len(sids)), labels=truncated_labels, rotation=90)
    plt.yticks(ticks=np.arange(len(sids)), labels=truncated_labels)
    plt.tick_params(axis="both", labelsize=4)
    xy_label = "Truncated transport IDs (per TCP session)"
    plt.xlabel(xy_label)
    plt.ylabel(xy_label)

    plt.title("Heatmap of Co-occurrences")
    plt.tight_layout()

    plt.savefig("../images/heatmap.png", dpi=300)
    plt.close()

    projection = TSNE().fit_transform(reordered_matrix)
    plt.figure(figsize=(8, 6))
    colors = cm.rainbow(np.linspace(0, 1, len(clusters)))
    for cluster_id in sorted(clusters.keys()):
        indices = clusters[cluster_id]
        plt.scatter(
            projection[indices, 0],
            projection[indices, 1],
            color=colors[cluster_id],
            label=cluster_id,
            alpha=0.6,
        )
    scatter_labels = heatmap_labels(clusters, False, sids)

    offset = 0.1  # Adjust the offset size if needed
    x_offset = np.random.uniform(-offset, offset, size=len(sids))
    y_offset = np.random.uniform(-offset, offset, size=len(sids))
    for i, label in enumerate(scatter_labels):
        # plt.text(projection[i, 0], projection[i, 1], label, fontsize=9, alpha=0.75)
        plt.text(
            projection[i, 0] + x_offset[i],
            projection[i, 1] + y_offset[i],
            label,
            fontsize=4,
            alpha=0.75,
        )
    plt.legend(title="Clusters")
    plt.title("2D Projection of Heatmap Data")
    plt.xlabel("Component 1")
    plt.ylabel("Component 2")
    plt.tight_layout()
    plt.savefig("../images/projected.png", dpi=300)
    plt.close()


# # converts overlap to a distance where distance is from [0, 1] and reciprocal of overlap (max_overlap corresponds to 0 distance)
# def overlap_to_distance(overlap, max_overlap):
#     # no overlap means max distance
#     if overlap == 0:
#         return 1
#     # max overlap means no distance
#     if overlap == max_overlap:
#         return 0
#     # normalize to [0, 1]
#     return 1 - (overlap / max_overlap)


# converts overlap to a distance where distance is from [0, 1]
# Exponential decay transformation function f=e^[-alpha*(overlap/max_overlap)]
def overlap_to_distance(overlap, max_overlap):
    alpha = 3  # adjust alpha to control the rate of decay

    # no overlap means max distance
    if overlap <= 0:
        return 1
    # max overlap means no distance
    if overlap >= max_overlap:
        return 0
    # exponential decay
    return math.exp(-alpha * (overlap / max_overlap))

    # example for alpha=3 and max_overlap=10
    # overlap = 0, distance = 1.00
    # overlap = 1, distance = 0.74
    # overlap = 2, distance = 0.54
    # overlap = 3, distance = 0.40
    # overlap = 4, distance = 0.30
    # overlap = 5, distance = 0.22
    # overlap = 6, distance = 0.16
    # overlap = 7, distance = 0.12
    # overlap = 8, distance = 0.09
    # overlap = 9, distance = 0.06
    # overlap =10, distance = 0.00


def cluster_data(samples, distance_func, processed_args):
    if cluster_method := processed_args.get("cluster_method"):
        # get args for clustering (expected a comma separated list of key->value pairs, e.g. min_samples->5,eps->0.5)
        cluster_args = processed_args.get("cluster_args")
        if not cluster_args:
            sys.exit("argument 'cluster_args' not provided")
        cluster_args = cluster_args.split(",")
        args_dict = {}
        for arg in cluster_args:
            key, value = arg.split("=", maxsplit=1)
            # try to parse value as int, float, or string
            try:
                value = int(value)
            except ValueError:
                try:
                    value = float(value)
                except ValueError:
                    pass
            args_dict[key] = value
        cluster_method = cluster_method.upper()

        if cluster_method == "OPTICS":
            clusterer = OPTICS(metric=distance_func, **args_dict)
        elif cluster_method == "HDBSCAN":
            clusterer = HDBSCAN(metric=distance_func, **args_dict)
        else:
            sys.exit(f"Unknown cluster method {cluster_method}")
        print(f"Clustering data using {cluster_method} with args {args_dict}")
        return clusterer.fit_predict(samples)
    else:
        sys.exit("argument 'cluster_method' not provided (needed to cluster data)")


def main(data, max_overlap, processed_args):
    sids = list(data.keys())
    sids.sort()  # order sids for consistent results

    def distance_func(idx1, idx2):
        sid1 = sids[int(idx1[0])]
        sid2 = sids[int(idx2[0])]
        dist = overlap_to_distance(data[sid1].get(sid2, 0), max_overlap)
        # print(f"Distance between {sid1} and {sid2} is {dist}")
        return dist

    samples = [[i] for i in range(len(sids))]
    labels = cluster_data(samples, distance_func, processed_args)

    # save which indexes are in which cluster
    clusters = {}
    for id_, cluster_id in enumerate(labels):
        if cluster_id not in clusters:
            clusters[cluster_id] = []
        clusters[cluster_id].append(id_)

    print(f"Found {len(clusters)} clusters")
    for cluster_id, cluster in clusters.items():
        print(
            f"[cluster]{cluster_id}:{[sids[index] for index in cluster]}"
        )  # list of transport IDs in each cluster

    if show_graph := processed_args.get("show_graph"):
        print_stats = show_graph.lower() == "print_stats"
        graph_data(sids, data, clusters, print_stats)


def get_args(args):
    processed_args = {}
    for arg in args:
        if "=" in arg:
            key, value = arg.split("=", maxsplit=1)
            processed_args[key] = value
    return processed_args


if __name__ == "__main__":
    processed_args = get_args(sys.argv[1:])
    # filename=../data/ts1_cooccurrence.pb
    if filename := processed_args.get("filename"):
        loaded_data, max_overlap = load_map(filename)
        main(loaded_data, max_overlap, processed_args)
    else:
        sys.exit("argument 'filename' not provided (needed to load cooccurrence data)")

# python3 main.py filename=../data/ts1_cooccurrence.pb cluster_method=OPTICS cluster_args=min_samples=3,max_eps=1.040047 show_graph=no_stats
