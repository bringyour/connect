import matplotlib.pyplot as plt
import matplotlib.patches as patches
import sys


def plot_overlap_fixed_margin(times1, times2, fixed_margin, testNum, expected):
    fig, ax = plt.subplots(figsize=(10, 3))

    # Plot times1 and times2 as scatter plots
    y_times1 = [1] * len(times1)  # y position for times1
    y_times2 = [2] * len(times2)  # y position for times2

    ax.scatter(times1, y_times1, color="blue", label="Times1")
    ax.scatter(times2, y_times2, color="orange", label="Times2")

    # Add the fixed margin as rectangles (2 * fixed_margin length)
    for t in times1:
        rect = patches.Rectangle(
            (t - fixed_margin, 0.9),
            2 * fixed_margin,
            0.2,
            linewidth=1,
            edgecolor="blue",
            facecolor="blue",
            alpha=0.3,
        )
        ax.add_patch(rect)

    for t in times2:
        rect = patches.Rectangle(
            (t - fixed_margin, 1.9),
            2 * fixed_margin,
            0.2,
            linewidth=1,
            edgecolor="orange",
            facecolor="orange",
            alpha=0.3,
        )
        ax.add_patch(rect)

    # Set the limits, labels, and legend
    ax.set_ylim(0.5, 2.5)
    ax.set_xlim(
        min(min(times1), min(times2)) - fixed_margin - 10,
        max(max(times1), max(times2)) + fixed_margin + 10,
    )
    ax.set_yticks([1, 2])
    ax.set_yticklabels(["Times1", "Times2"])
    ax.set_xticks(
        range(
            int(min(min(times1), min(times2)) - fixed_margin),
            int(max(max(times1), max(times2)) + fixed_margin) + 1,
            10,
        ),
    )
    plt.xticks(rotation=45)
    ax.set_xlabel("Time")
    ax.set_title(f"Overlap Plot with Fixed Margin (T{testNum} - expected {expected})")
    # ax.legend()

    plt.tight_layout()
    plt.savefig("./images/overlap.png", dpi=300)
    plt.close()


if __name__ == "__main__":
    testNum = int(sys.argv[1]) if len(sys.argv) >= 2 else 1

    # T1 (default)
    times1 = [100, 200, 300]
    times2 = [150, 250, 350]
    fixed_margin = 10
    expected = 0

    if testNum == 2:
        # T2
        times1 = [100, 200, 300]
        times2 = [150, 250, 350]
        fixed_margin = 30
        expected = 50
    elif testNum == 3:
        # T3
        times1 = [100, 200]
        times2 = [110, 210]
        fixed_margin = 10
        expected = 20
    elif testNum == 4:
        # T4
        times1 = [100, 200, 300]
        times2 = [150, 250, 350]
        fixed_margin = 60
        expected = 270
    elif testNum == 5:
        # T5
        times1 = [100, 200, 300]
        times2 = [150, 250, 350]
        fixed_margin = 100
        expected = 350
    else:
        testNum = 1

    print("Running test", testNum)
    plot_overlap_fixed_margin(times1, times2, fixed_margin, testNum, expected)
