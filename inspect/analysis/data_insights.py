import sys
import numpy as np
import matplotlib.pyplot as plt
import statistics
import matplotlib.cm as cm
from sklearn.manifold import TSNE

from overlap import overlap_to_distance
from load_protos import get_args, load_cooc, load_clusters


def graph_data(cooc_data, sids, clusters, max_overlap, print_stats):
    """
    Saves the coocurrence data as a heatmap and a 2D projection where points are colored based on clustering.

    Parameters
    ----------
    cooc_data : the cooccurrence data in the form of a dictionary.
    sids : list of the session IDs in the cooccurence data.
    clusters : dictionary of cluster_id -> list of indices in int (where int is the index of the sid in the sids).
    max_overlap : the maximum value of overlap in the cooccurrence data.
    print_stats : a boolean to print statistics about the heatmap.
    """

    sids.sort()  # order sids for consistent results
    heatmap_data = np.ones(
        (len(sids), len(sids))
    )  # fill with ones as max distance values

    for i, sid_i in enumerate(sids):
        if sid_i not in cooc_data:
            continue
        for j, sid_j in enumerate(sids):
            if sid_j in cooc_data[sid_i]:
                if heatmap_data[i, j] + heatmap_data[j, i] < 2:
                    # non-zero value exists for data[sid_j][sid_j] and data[sid_j][sid_j]
                    print(f"Duplicate entry found for {sid_i} and {sid_j}")
                    return
                final_val = overlap_to_distance(
                    cooc_data[sid_i][sid_j], max_overlap
                )  # values are from 0 to 1
                large = max(i, j)
                small = min(i, j)
                heatmap_data[large, small] = final_val
                heatmap_data[small, large] = final_val

    if print_stats:
        _stats(heatmap_data)

    # reorder based on the formed clusters
    ordered_ids = []
    for cluster_id in sorted(clusters.keys()):
        ordered_ids.extend(clusters[cluster_id])

    reordered_matrix = heatmap_data[np.ix_(ordered_ids, ordered_ids)]
    plt.imshow(reordered_matrix, cmap="hot", interpolation="nearest")

    cb = plt.colorbar()
    cb.set_label("Overlap time (seconds)")
    truncated_labels = _heatmap_labels(clusters)  # , False, sids)
    plt.xticks(ticks=np.arange(len(sids)), labels=truncated_labels, rotation=90)
    plt.yticks(ticks=np.arange(len(sids)), labels=truncated_labels)
    plt.tick_params(axis="both", labelsize=4)
    xy_label = "Truncated transport IDs (per TCP session)"
    plt.xlabel(xy_label)
    plt.ylabel(xy_label)

    plt.title("Heatmap of Co-occurrences")
    plt.tight_layout()

    plt.savefig("../images/data_insights/heatmap.png", dpi=300)
    plt.close()

    projection = TSNE().fit_transform(reordered_matrix)
    plt.figure(figsize=(8, 6))
    colors = cm.rainbow(np.linspace(0, 1, len(clusters)))

    idx = 0
    sorted_cluster_ids = sorted(clusters.keys())
    for i, cluster_id in enumerate(sorted_cluster_ids):
        start = idx
        idx += len(clusters[cluster_id])
        indices = range(start, idx)
        plt.scatter(
            projection[indices, 0],
            projection[indices, 1],
            color=colors[i],
            label=cluster_id,
            alpha=0.6,
        )
    scatter_labels = _heatmap_labels(clusters, False, sids)

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
    plt.savefig("../images/data_insights/projected.png", dpi=300)
    plt.close()


def _heatmap_labels(clusters, only_first=True, sids=None):
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


def _stats(list_of_lists):
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


if __name__ == "__main__":
    processed_args = get_args(sys.argv[1:])
    cooc_path = processed_args.get("cooccurrence")
    clusters_path = processed_args.get("clusters")
    if cooc_path and clusters_path:
        cooc_data, max_overlap = load_cooc(cooc_path)
        sids = list(cooc_data.keys())
        sids.sort()  # order sids for consistent results
        clusters = load_clusters(clusters_path, sids)
        print_stats = processed_args.get("print_stats")
        graph_data(cooc_data, sids, clusters, max_overlap, print_stats == "True")
    else:
        sys.exit("argument 'cooccurrence' or 'clusters' not provided")

# python3 data_insights.py cooccurrence=../data/ts1/ts1_cooccurrence.pb clusters=../data/ts1/ts1_clusters.pb print_stats=True
