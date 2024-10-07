import numpy as np
import matplotlib.pyplot as plt
import math
from scipy.integrate import quad
from scipy.stats import norm


def gaussian(x, mean, std_dev):
    return norm.pdf(x, mean, std_dev)


def overlap_area(mean1, std_dev1, mean2, std_dev2, cutoff):
    # define the limits for integration (cutoff applied)
    lower_limit = max(mean1 - cutoff, mean2 - cutoff)
    upper_limit = min(mean1 + cutoff, mean2 + cutoff)

    if lower_limit >= upper_limit:
        return 0.0  # No overlap

    # define the integrand as the minimum of the two Gaussians
    def integrand(x):
        return min(gaussian(x, mean1, std_dev1), gaussian(x, mean2, std_dev2))

    # Calculate the area using integration
    area, _ = quad(integrand, lower_limit, upper_limit)
    return area


def plot_curves(ax, color, x, times, gauss, std_dev):
    for t, y in zip(times, gauss):
        ax.plot(x, y, color="blue", alpha=0.6)  # Plot Gaussian with blue color
        ax.fill_between(x, y, alpha=0.1, color=color)
        ax.plot(
            [t, t],
            [0, gaussian(t, t, std_dev)],
            color=color,
            linestyle="--",
            linewidth=1,
            alpha=0.6,
        )


def plot_areas(
    ax,
    color_text,
    color_area,
    x,
    times1,
    times2,
    gauss1,
    gauss2,
    std_dev,
    cutoff,
    same_times=False,
):
    total = 0
    i = 0
    for t1, y1 in zip(times1, gauss1):
        j = 0
        for t2, y2 in zip(times2, gauss2):
            if same_times and i == j:
                continue
            area_overlap = overlap_area(t1, std_dev, t2, std_dev, cutoff)
            if area_overlap > 0:
                if not same_times:
                    print(f"Overlap between {t1} and {t2}: {area_overlap}")
                    total += area_overlap
                ax.text(
                    (t1 + t2) / 2,
                    0.5 * gaussian((t1 + t2) / 2, t1, std_dev),
                    f"{area_overlap:.4f}",
                    fontsize=12,
                    color=color_text,
                    ha="center",
                    rotation=30,
                )
                ax.fill_between(
                    x,
                    0,
                    np.minimum(y1, y2),
                    where=(x >= (t1 - cutoff)) & (x <= (t2 + cutoff)),
                    color=color_area,
                    alpha=0.6,
                )
            j += 1
        i += 1
    return total


def plot_gaussian_overlap(times1, times2, std_dev, cutoff):
    max_cutoff = 4 * std_dev
    cutoff = min(
        cutoff, max_cutoff
    )  # cutoff cannot be bigger than 4*std_dev since that includes 99.99% of the distribution

    fig, ax = plt.subplots(figsize=(10, 5))

    # define x-axis range
    x = np.linspace(
        min(min(times1), min(times2)) - max_cutoff,
        max(max(times1), max(times2)) + max_cutoff,
        1000,  # granularity
    )

    # calculate gaussian distributions
    gauss1 = [gaussian(x, t, std_dev) for t in times1]
    gauss2 = [gaussian(x, t, std_dev) for t in times2]

    # plot gaussian distributions
    plot_curves(ax, "blue", x, times1, gauss1, std_dev)
    plot_curves(ax, "orange", x, times2, gauss2, std_dev)

    # plot overlap areas and calculate overlap
    print("NOTICE: Not printing 0 overlap")
    total = plot_areas(
        ax, "purple", "red", x, times1, times2, gauss1, gauss2, std_dev, cutoff
    )
    # plot_areas(
    #     ax, "green", "green", x, times1, times1, gauss1, gauss1, std_dev, cutoff, True
    # )
    # plot_areas(
    #     ax, "orange", "orange", x, times2, times2, gauss2, gauss2, std_dev, cutoff, True
    # )
    print(f"Total overlap: {total}")

    # Set labels and title
    min_time = min(min(times1), min(times2))
    ax.plot(min_time, 0, color="blue", alpha=0.6, label="Times 1")
    ax.plot(min_time, 0, color="orange", alpha=0.6, label="Times 2")
    ax.plot(min_time, 0, color="red", alpha=0.6, label="Overlap Area")
    ax.set_xlabel("Time")
    ax.set_ylabel("Amplitude")
    # ax.set_xticks(
    #     range(
    #         int(min(min(times1), min(times2)) - cutoff),
    #         int(max(max(times1), max(times2)) + cutoff) + 1,
    #         10,
    #     ),
    # )
    plt.xticks(rotation=90)
    ax.set_title("Gaussian Overlap Visualization")
    ax.legend()

    # plt.tight_layout()
    plt.savefig("./images/overlap.png", dpi=300)
    plt.close()


times1 = [1727265107213928000]
times2 = [1727265107157590000]
std_dev = 0.05 * 1_000_000_000


# times1 = [100, 180, 300]
# times2 = [150, 250, 330]
# std_dev = 30
cutoff = 4 * std_dev

# 1 stdev contains 68% of the distribution
# 2 stdev contains 95% of the distribution
# 2.5 stdev contains 99% of the distribution
# 3 stdev contains 99.7% of the distribution
# 4 stdev contains 99.99% of the distribution

plot_gaussian_overlap(times1, times2, std_dev, cutoff)
