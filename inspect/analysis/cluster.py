import os
import sys
import math
import statistics
import numpy as np
import seaborn as sns
import matplotlib.cm as cm
import matplotlib.pyplot as plt
from sklearn.cluster import OPTICS
import hdbscan

from overlap import overlap_to_distance
from data_insights import graph_data
from load_protos import load_cooc, get_args


def cluster_data(samples, distance_func, processed_args, show_graph):
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
            labels = clusterer.fit_predict(samples)
            # calculate probabilites from core distances
            # reachability = clusterer.reachability_[clusterer.ordering_]
            core_dist = clusterer.core_distances_[clusterer.ordering_]
            probabilities = [
                1 - (core_dist[i] / max(core_dist)) for i in range(len(core_dist))
            ]

            if show_graph:
                # plotting the reachability distances
                reachability = clusterer.reachability_[clusterer.ordering_]
                labels1 = clusterer.labels_[clusterer.ordering_]
                plt.figure(figsize=(10, 8))
                space = np.arange(len(samples))
                for cluster_id in np.unique(labels1):
                    plt.bar(
                        space[labels1 == cluster_id],
                        reachability[labels1 == cluster_id],
                        label=f"Cluster {cluster_id}",
                    )
                plt.xlabel("Points (ordered)")
                plt.ylabel("Reachability Distance")
                plt.title("Reachability Plot")
                plt.legend(loc="lower right")
                plt.savefig("../images/cluster_process.png", dpi=300)
                plt.close()

        elif cluster_method == "HDBSCAN":
            clusterer = hdbscan.HDBSCAN(metric=distance_func, **args_dict)
            labels = clusterer.fit_predict(samples)
            probabilities = clusterer.probabilities_

            if show_graph:
                plt.figure(figsize=(10, 8))
                # clusterer.single_linkage_tree_.plot()
                clusterer.condensed_tree_.plot(
                    select_clusters=True, selection_palette=sns.color_palette("deep", 8)
                )
                plt.savefig("../images/cluster_process.png", dpi=300)
                plt.close()

        else:
            sys.exit(f"Unknown cluster method {cluster_method}")
        print(f"Clustering data using {cluster_method} with args {args_dict}")
        # print(f"Numbers of features seen during fit: {clusterer.n_features_in_}")
        return labels, probabilities
    else:
        sys.exit("argument 'cluster_method' not provided (needed to cluster data)")


def main(cooc_data, max_overlap, processed_args):
    sids = list(cooc_data.keys())
    sids.sort()  # order sids for consistent results

    def distance_func(idx1, idx2):
        sid1 = sids[int(idx1[0])]
        sid2 = sids[int(idx2[0])]
        dist = overlap_to_distance(cooc_data[sid1].get(sid2, 0), max_overlap)
        # print(f"Distance between {sid1} and {sid2} is {dist}")
        return dist

    show_graph = processed_args.get("show_graph")
    samples = [[i] for i in range(len(sids))]
    labels, probabilities = cluster_data(
        samples, distance_func, processed_args, show_graph
    )

    # save which indexes are in which cluster
    clusters = {}
    for id_, cluster_id in enumerate(labels):
        if cluster_id not in clusters:
            clusters[cluster_id] = []
        clusters[cluster_id].append(id_)

    print(f"Found {len(clusters)} clusters")
    for cluster_id, cluster in clusters.items():
        print(
            f"[cluster]{cluster_id}:{[sids[index] for index in cluster]} {[probabilities[index] for index in cluster]}"
        )  # list of transport IDs in each cluster

        # print(f"Cluster {cluster_id} has {len(cluster)} elements")
        # for index in cluster:
        #     print(f"{index}  {sids[index]} ({probabilities[index]})")

    if show_graph:
        print_stats = show_graph.lower() == "print_stats"
        graph_data(cooc_data, sids, clusters, max_overlap, print_stats)


if __name__ == "__main__":
    processed_args = get_args(sys.argv[1:])
    # filename=../data/ts1/ts1_cooccurrence.pb
    if filename := processed_args.get("filename"):
        loaded_cooc, max_overlap = load_cooc(filename)
        main(loaded_cooc, max_overlap, processed_args)
    else:
        sys.exit("argument 'filename' not provided (needed to load cooccurrence data)")

# python3 cluster.py filename=../data/ts1/ts1_cooccurrence.pb cluster_method=OPTICS cluster_args=min_samples=3,max_eps=1.040047 show_graph=no_stats
