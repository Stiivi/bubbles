import os.path

def data_path(filename):
    """Returns a full path to data file with name `filename`"""
    root_path = os.path.join(os.path.dirname(__file__), "data")
    return os.path.join(root_path, filename)

