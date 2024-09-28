import cooccurrence_pb2
import numpy as np
import matplotlib.pyplot as plt
import os
import statistics
import matplotlib.cm as cm

import numpy as np
from sklearn.cluster import OPTICS, HDBSCAN
from sklearn.manifold import TSNE
from scipy.spatial.distance import pdist, squareform
import seaborn as sns

ENABLE_LABELS = True


def load_map(filename1):
    with open(filename1, "rb") as f:
        data = f.read()

    cooc_data = cooccurrence_pb2.CooccurrenceData()
    cooc_data.ParseFromString(data)

    max_overlap = -1
    result = {}
    for outer in cooc_data.cooc_outer:
        outer_sid = outer.sid.hex()
        inner_dict = {}
        for inner in outer.cooc_inner:
            inner_sid = inner.sid.hex()
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
                new_labels.append(f"…{sids[idx][-5:]}")
                # new_labels.append(cluster_id)
    return new_labels


def scatter_colors(clusters):
    unique_clusters = clusters.keys()
    colors = cm.rainbow(np.linspace(0, 1, len(unique_clusters)))
    cluster_colors = []
    for cluster_id in sorted(clusters.keys()):
        for idx in clusters[cluster_id]:
            cluster_colors.append(colors[cluster_id])
    return cluster_colors


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
    print("Statistics about heatmap:")
    print(f"  Mean: {mean}")
    print(f"  Median: {median}")
    print(f"  Variance: {variance}")
    print(f"  Standard Deviation: {stdev}")
    print(f"  Min: {min_val}")
    print(f"  Max: {max_val}")
    print(f"  25th, 50th, 75th Percentiles: {percentiles}")


# converts overlap to a distance where distance is from [0, 1] and reciprocal of overlap (max_overlap corresponds to 0 distance)
def overlap_to_distance(overlap, max_overlap):
    # no overlap means max distance
    if overlap == 0:
        return 1
    # max overlap means no distance
    if overlap == max_overlap:
        return 0
    # normalize to [0, 1]
    return 1 - (overlap / max_overlap)


def compute_distance_from_overlap(overlap_data, max_overlap, sid1, sid2):
    dist = overlap_to_distance(overlap_data[sid1].get(sid2, 0), max_overlap)
    # print(f"Distance between {sid1} and {sid2} is {dist}")
    return dist


def cluster(samples, distance_func):
    labels = OPTICS(min_samples=5, metric=distance_func, max_eps=0.9).fit_predict(
        samples
    )
    # labels = HDBSCAN(
    #     min_cluster_size=3,
    #     metric=distance_func,
    # ).fit_predict(samples)

    return labels


def create_heatmap(data, max_overlap):
    sids = list(data.keys())
    size = len(sids)
    heatmap_data = np.ones((size, size))  # fill with ones as max distance values

    print(f"{len(sids)} sids found in data")

    for i, sid_1 in enumerate(sids):
        if sid_1 not in data:
            continue
        for j, sid_j in enumerate(sids):
            if sid_j in data[sid_1]:
                if heatmap_data[i, j] + heatmap_data[j, i] < 2:
                    # non-zero value exists for data[sid_j][sid_j] and data[sid_j][sid_j]
                    print(f"Duplicate entry found for {sid_1} and {sid_j}")
                    return
                large = max(i, j)
                small = min(i, j)
                # time_in_sec = data[sid_j][sid_j]  # / 1e9
                # final_val = 1 / (time_in_sec + 1e-9)
                final_val = overlap_to_distance(
                    data[sid_1][sid_j], max_overlap
                )  # values are from 0 to 1
                heatmap_data[large, small] = final_val
                heatmap_data[small, large] = final_val

    stats(heatmap_data)

    def distance_func(idx1, idx2):
        return compute_distance_from_overlap(
            data, max_overlap, sids[int(idx1[0])], sids[int(idx2[0])]
        )

    samples = [[i] for i in range(len(sids))]
    labels = cluster(samples, distance_func)
    # for i, label in enumerate(labels):
    #     print(f"ID {sids[i]} is in cluster {label}")

    # save which indexes are in which cluster
    clusters = {}
    for id_, cluster_id in enumerate(labels):
        if cluster_id not in clusters:
            clusters[cluster_id] = []
        clusters[cluster_id].append(id_)

    print(f"Found {len(clusters)} clusters: {clusters}")

    # reorder based on the formed clusters
    ordered_ids = []
    for cluster_id in sorted(clusters.keys()):
        ordered_ids.extend(clusters[cluster_id])

    reordered_matrix = heatmap_data[np.ix_(ordered_ids, ordered_ids)]
    plt.imshow(reordered_matrix, cmap="hot", interpolation="nearest")

    cb = plt.colorbar()
    cb.set_label("Overlap time (seconds)")
    if ENABLE_LABELS:
        truncated_labels = heatmap_labels(clusters)
        plt.xticks(ticks=np.arange(size), labels=truncated_labels, rotation=90)
        plt.yticks(ticks=np.arange(size), labels=truncated_labels)
        plt.tick_params(axis="both", labelsize=4)
    xy_label = "Truncated transport IDs (per TCP session)"
    plt.xlabel(xy_label)
    plt.ylabel(xy_label)

    plt.title("Heatmap of Co-occurrences")
    plt.tight_layout()

    plt.savefig("../images/heatmap.png", dpi=300)
    plt.close()

    projection = TSNE().fit_transform(reordered_matrix)
    cluster_colors = scatter_colors(clusters)
    plt.scatter(projection[:, 0], projection[:, 1], color=cluster_colors)
    scatter_labels = heatmap_labels(clusters, False, sids)

    offset = 0.333  # Adjust the offset size if needed
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
    plt.title("2D Projection of Heatmap Data")
    plt.xlabel("Component 1")
    plt.ylabel("Component 2")
    plt.tight_layout()
    plt.savefig("../images/projected.png", dpi=300)
    plt.close()


if __name__ == "__main__":
    # filename = "../data/cooccurrence_data_2k.pb"
    # filename = "../data/cooccurrence_data_small.pb"
    filename = "../data/ts1_cooccurrence.pb"
    loaded_data, max_overlap = load_map(filename)
    create_heatmap(loaded_data, max_overlap)
