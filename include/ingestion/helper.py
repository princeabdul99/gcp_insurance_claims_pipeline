from urllib.parse import urlparse
import os
import re

def derive_folder_from_filename(filename: str) -> str:
    name = os.path.splitext(filename)[0]
    name = re.sub(r"_\d{4}-\d{2}-\d{2}$", "", name)
    return name.lower()


def get_filename_from_url(url: str) -> str:
    path = urlparse(url).path
    return os.path.basename(path)