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
import hdbscan


def load_times(filename1):
    with open(filename1, "rb") as f:
        data = f.read()

    times_data = cooccurrence_pb2.TimesData()
    times_data.ParseFromString(data)

    result = {}
    for times in times_data.times:
        times_sid = times.sid  # .hex()
        result[times_sid] = times.time

    return result


def main(sid_times):
    sids = list(sid_times.keys())
    sids.sort()  # order sids for consistent results
    y_values = range(len(sids))  # Assign a numeric value for each SID

    # Create the plot
    plt.figure(figsize=(10, 10))
    colors = [plt.cm.tab10(i % 10) for i in range(len(sids))]

    # Iterate through each sid and its times, plotting the points
    for i, (sid, color) in enumerate(zip(sids, colors)):
        times = sid_times[sid]
        plt.scatter(
            times, [i] * len(times), s=5, label=sid, marker=".", color=color
        )  # plot times at y = i for each sid

    # Customize the plot
    plt.yticks(y_values, sids, fontsize=3)  # Show sids on y-axis
    for i, color in enumerate(colors):
        plt.gca().get_yticklabels()[i].set_color(color)
    plt.xlabel("Time")
    # plt.ylabel("SID")
    plt.title("Times per SID")
    # plt.grid(True)
    plt.savefig("../images/times.png", dpi=300)
    plt.close()


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
        loaded_data = load_times(filename)
        main(loaded_data)
    else:
        sys.exit("argument 'filename' not provided (needed to load cooccurrence data)")

# python3 times.py filename=../test_data/ts2_times.pb
