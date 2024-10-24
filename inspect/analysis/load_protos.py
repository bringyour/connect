import cooccurrence_pb2
import sys


def get_args(args):
    """
    Splits the arguments in the form of key=value pairs and returns them as a dictionary.
    """

    processed_args = {}
    for arg in args:
        if "=" in arg:
            key, value = arg.split("=", maxsplit=1)
            processed_args[key] = value
    return processed_args


def load_cooc(filename):
    """
    Loads the cooccurrence data from a file and returns it as a dictionary together with the maximum overlap value encountered.
    """

    with open(filename, "rb") as f:
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


def load_times(filename):
    """
    Loads the times data from a file and returns it as a dictionary.
    """

    with open(filename, "rb") as f:
        data = f.read()

    times_data = cooccurrence_pb2.TimesData()
    times_data.ParseFromString(data)

    result = {}
    for times in times_data.times:
        times_sid = times.sid
        result[times_sid] = times.time

    return result


def load_clusters(filename, sids):
    """
    Returns a dictionary of cluster_id -> list of indices in int (where int is the index of the sid in the ordereded sids).
    """

    sids.sort()  # order sids for consistent results

    # make dict where key is sid and value is index
    sids_dict = {}
    for i, sid in enumerate(sids):
        sids_dict[sid] = i

    with open(filename, "rb") as f:
        data = f.read()

    clusters_data = cooccurrence_pb2.ClustersData()
    clusters_data.ParseFromString(data)

    result = {}
    for cluster in clusters_data.clusters:
        cluster_id = cluster.id
        result[cluster_id] = []
        for sid in cluster.sids:
            sid_idx = sids_dict.get(sid)
            if sid_idx is None:
                sys.exit(f"SID {sid} not found in cooccurrence data")
            result[cluster_id].append(sid_idx)
    return result
