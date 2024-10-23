import math
from scipy.stats import norm


def overlap_area(mean1, std_dev1, mean2, std_dev2, cutoff):
    # define the limits for integration (cutoff applied)
    lower_limit = max(mean1, mean2) - cutoff
    upper_limit = min(mean1, mean2) + cutoff

    if lower_limit >= upper_limit:
        return 0.0  # no overlap

    middle = (mean1 + mean2) / 2
    bigger_mean_gaussian = 0
    if mean1 > mean2:
        bigger_mean_gaussian = norm.cdf(middle, mean1, std_dev1)
    else:
        bigger_mean_gaussian = norm.cdf(middle, mean2, std_dev2)
    return bigger_mean_gaussian * 2


# converts overlap to a distance where distance is from [0, 1]
# def overlap_to_distance(overlap, max_overlap):
#     if overlap == 0:
#         return 1
#     return 0


# converts overlap to a distance where distance is from [0, 1] and reciprocal of overlap (max_overlap corresponds to 0 distance)
# def overlap_to_distance(overlap, max_overlap):
#     # no overlap means max distance
#     if overlap == 0:
#         return 1
#     # max overlap means no distance
#     if overlap == max_overlap:
#         return 0
#     # normalize to [0, 1]
#     return 1 - (math.log(overlap) / math.log(max_overlap))


# converts overlap to a distance where distance is from [0, 1]
# exponential decay transformation function f=e^[-alpha*(overlap/max_overlap)]
def overlap_to_distance(overlap, max_overlap):
    alpha = 13  # adjust alpha to control the rate of decay
    # no overlap means max distance
    if overlap <= 0:
        return 1
    # max overlap means no distance
    if overlap >= max_overlap:
        return 0
    # exponential decay
    return math.exp(-alpha * (overlap / max_overlap))
