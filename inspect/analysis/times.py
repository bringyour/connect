import sys
import numpy as np
import matplotlib.pyplot as plt
import matplotlib.cm as cm
from tqdm import tqdm

from load_protos import load_times, get_args


def main(sid_times):
    sids = list(sid_times.keys())
    sids.sort()  # order sids for consistent results
    # sids = [sid for sid in sids if "nyt" in sid]  # filter for ceratin sids
    y_values = range(len(sids))  # assign a numeric value for each SID

    print("List of sids in the data:")
    for sid in reversed(sids):
        print(sid, len(sid_times[sid]))
    print()

    # create the plot
    plt.figure(figsize=(10, 10))
    colors = [
        plt.cm.tab10(i % 10) for i in range(len(sids))
    ]  # as big as the number of sids
    all_times = []

    # iterate through each sid and its times, plotting the points
    for i, (sid, color) in tqdm(
        enumerate(zip(sids, colors)), total=len(sids), desc="Adding times to plot"
    ):
        times = sid_times[sid]
        all_times.extend(times)
        plt.scatter(
            np.array(times) / 1e9,  # convert nanoseconds to seconds
            [i] * len(times),
            s=12,
            label=sid,
            marker=".",
            color=color,
            alpha=0.2,
            edgecolors="none",
        )  # plot times at y = i for each sid

    # customize the plot
    plt.yticks(y_values, sids, fontsize=3)  # show sids on y-axis
    for i, color in tqdm(
        enumerate(colors), total=len(colors), desc="Setting label colors"
    ):
        plt.gca().get_yticklabels()[i].set_color(color)

    plt.xlabel("Time (seconds)")
    min_time = min(all_times) / 1e9
    max_time = max(all_times) / 1e9
    label_step = 25
    total_secs = int(max_time - min_time)
    labels = np.arange(0, total_secs + 1, label_step)
    ticks = [label + min_time for label in labels]
    if (total_secs % label_step) / label_step > 0.5:
        labels = np.append(labels, total_secs)
        ticks = np.append(ticks, total_secs + min_time)
    plt.xticks(ticks, labels, rotation=45)
    plt.xlim(min_time - 5, max_time + 5)

    plt.title("Times per SID")
    plt.grid(axis="x", alpha=0.15)
    print("Saving plot...")
    plt.savefig("../images/times.png", dpi=300)
    plt.close()


if __name__ == "__main__":
    processed_args = get_args(sys.argv[1:])
    if filename := processed_args.get("filename"):
        loaded_data = load_times(filename)
        # print(loaded_data["wikipedia.org"])
        main(loaded_data)
    else:
        sys.exit("argument 'filename' not provided (needed to load cooccurrence data)")

# python3 times.py filename=../data/ts2/ts2_times.pb
