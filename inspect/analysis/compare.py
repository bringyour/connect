import sys
import json
import numpy as np
import pandas as pd
import matplotlib.pyplot as plt

from load_protos import load_times, get_args
from overlap import overlap_area, overlap_to_distance

OVERLAP_SAVE_PATH = "../images/coverlap_matrix_"


def total_overlap(times1, times2, std_dev, cutoff):
    total = 0
    for t1 in times1:
        for t2 in times2:
            total += overlap_area(t1, std_dev, t2, std_dev, cutoff)
    return total


def calculate_pairwise_overlaps(sid_times, sids, std_devs, cutoff_factor):
    max_width = max(len(sid) for sid in sids)
    overlap_results = {}

    for std_dev in std_devs:
        print(f"Calculating overlaps for std_dev={std_dev:_}ns")
        overlap_matrix = np.zeros((len(sid_times), len(sid_times)))
        for i, sid_i in enumerate(sids):
            for j, sid_j in enumerate(sids):
                if i >= j:
                    continue
                overlap_matrix[i][j] = total_overlap(
                    sid_times[sid_i],
                    sid_times[sid_j],
                    std_dev,
                    std_dev * cutoff_factor,
                )
                overlap_matrix[j][i] = overlap_matrix[i][j]
                # print(
                #     f"  {sid_i:<{max_width}} x {sid_j:<{max_width}} = {overlap_matrix[i][j]:_}"
                # )
        overlap_results[std_dev] = overlap_matrix
        print()

    return overlap_results


def save_overlap_matrices(overlap_results, selected_sids):
    for std_dev, matrix in overlap_results.items():
        # convert to DataFrame for easier manipulation and saving
        df = pd.DataFrame(matrix, columns=selected_sids, index=selected_sids)

        # save as heatmap
        plt.figure(figsize=(10, 10))
        plt.imshow(matrix, cmap="hot", interpolation="nearest")
        plt.colorbar()
        plt.title(f"Overlap Matrix Heatmap (std_dev={std_dev:_}ns)")
        plt.xticks(
            ticks=np.arange(len(selected_sids)), labels=selected_sids, rotation=90
        )
        plt.yticks(ticks=np.arange(len(selected_sids)), labels=selected_sids)
        plt.tight_layout()
        plt.savefig(f"{OVERLAP_SAVE_PATH}{std_dev}.png", dpi=300)
        plt.close()
        df.to_csv(f"{OVERLAP_SAVE_PATH}{std_dev}.csv")  # save as CSV

        print(f"Saved overlap matrix for std_dev={std_dev:_}ns")


def plot_times(sid_times, sids):
    y_values = range(len(sids))  # assign a numeric value for each SID

    print("List of sids in the data:")
    for sid in sids:
        print(sid, len(sid_times[sid]))
    print()

    # create the plot
    plt.figure(figsize=(10, 10))
    colors = [plt.cm.tab10(i % 10) for i in range(len(sids))]
    all_times = []

    # iterate through each sid and its times, plotting the points
    for i, (sid, color) in enumerate(zip(reversed(sids), colors)):
        times = sid_times[sid]
        all_times.extend(times)
        plt.scatter(
            np.array(times) / 1e9,  # convert nanoseconds to seconds
            [i] * len(times),
            # s=12,
            label=sid,
            marker=".",
            color=color,
            alpha=0.2,
            edgecolors="none",
        )  # plot times at y = i for each sid

    # customize the plot
    plt.yticks(y_values, reversed(sids))  # show sids on y-axis
    for i, color in enumerate(colors):
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
    plt.tight_layout()
    plt.savefig("../images/cctimes.png", dpi=300)
    plt.close()


def create_distance_matrix(matrix):
    max_overlap = np.max(matrix)

    # change matrix from overlap to distance
    distance_matrix = np.ones(matrix.shape)
    for i in range(matrix.shape[0]):
        for j in range(matrix.shape[1]):
            distance_matrix[i][j] = overlap_to_distance(matrix[i][j], max_overlap)
    return distance_matrix


def plot_distances(std_devs, selected_sids):
    overlap_results = {}
    for std_dev in std_devs:
        # load overlap matrix from csv file
        try:
            overlap_matrix = pd.read_csv(f"{OVERLAP_SAVE_PATH}{std_dev}.csv")
            # check if selected sids match the matrix
            header = overlap_matrix.columns[1:]
            if len(selected_sids) != len(header) or any(
                sid != header[i] for i, sid in enumerate(selected_sids)
            ):
                raise ValueError(
                    f"Selected SIDs do not match the overlap matrix for std_dev={std_dev:_}ns"
                )
            overlap_results[std_dev] = overlap_matrix.to_numpy()
        except FileNotFoundError:
            raise ValueError(
                f"Overlap matrix not found: {OVERLAP_SAVE_PATH}{std_dev}.csv"
            )

    for std_dev, matrix in overlap_results.items():
        distance_matrix = create_distance_matrix(matrix[:, 1:])
        # convert to DataFrame for easier manipulation and saving
        df = pd.DataFrame(distance_matrix, columns=selected_sids, index=selected_sids)

        # save as heatmap
        plt.figure(figsize=(10, 10))
        plt.imshow(distance_matrix, cmap="hot", interpolation="nearest")
        plt.colorbar()
        plt.title(f"Distance Matrix Heatmap (std_dev={std_dev:_}ns)")
        plt.xticks(
            ticks=np.arange(len(selected_sids)), labels=selected_sids, rotation=90
        )
        plt.yticks(ticks=np.arange(len(selected_sids)), labels=selected_sids)
        plt.tight_layout()
        plt.savefig(f"../images/cdistance_matrix_{std_dev}.png", dpi=300)
        plt.close()
        df.to_csv(f"../images/cdistance_matrix_{std_dev}.csv")  # save as CSV

        print(f"Saved distance matrix for std_dev={std_dev:_}ns")


def get_config(args):
    processed_args = get_args(args)

    if "config" not in processed_args:
        raise ValueError("Missing JSON argument")
    if "mode" not in processed_args:
        raise ValueError("Missing mode argument. Use 'overlap(o)' or 'distance(d)'")

    config_file_path = processed_args["config"]
    data_dict = {}
    try:
        with open(config_file_path, "r") as f:
            data_dict = json.load(f)
    except FileNotFoundError:
        raise ValueError(f"Config file not found: {config_file_path}")
    except json.JSONDecodeError:
        raise ValueError(f"Invalid JSON format in config file: {config_file_path}")

    mandatory_args = ["filename", "selected_sids", "std_devs"]
    for arg in mandatory_args:
        if arg not in data_dict:
            raise ValueError(f"Missing mandatory field in json config: {arg}")
        if arg == "std_devs":
            data_dict[arg] = [int(1_000_000_000 * float(sd)) for sd in data_dict[arg]]

    return data_dict, processed_args["mode"]


if __name__ == "__main__":

    # parse config file
    config, mode = get_config(sys.argv[1:])
    filename = config["filename"]
    selected_sids = config["selected_sids"]
    std_devs = config["std_devs"]

    if mode in ["distance", "d"]:
        ### --- DISTANCE MODE --- ###
        plot_distances(std_devs, selected_sids)
        exit(0)
    if mode not in ["overlap", "o"]:
        raise ValueError("Invalid mode argument. Use 'overlap(o)' or 'distance(d)'")

    ### --- OVERLAP MODE --- ###

    # load times for selected sids
    sid_times = {}
    initial_times = load_times(filename)
    for sid in selected_sids:
        if curr_times := initial_times.get(sid):
            sid_times[sid] = curr_times
        else:
            print(f"SID '{sid}' not found in data")

    # subtract minimum found time from all times
    min_time = min(min(times) for times in sid_times.values())
    for sid, times in sid_times.items():
        sid_times[sid] = [t - min_time for t in times]

    # plot the times of the selected sids
    plot_times(sid_times, selected_sids)

    # calculate overlaps and save results
    cutoff_factor = 4  # in terms of std_dev
    overlap_results = calculate_pairwise_overlaps(
        sid_times, selected_sids, std_devs, cutoff_factor
    )
    save_overlap_matrices(overlap_results, selected_sids)

# python3 compare.py config=../data/compare_config.json mode=o
# python3 compare.py config=../data/compare_config.json mode=d
