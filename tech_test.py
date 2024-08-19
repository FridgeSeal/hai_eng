import json
import typing
from typing import Dict, Any, Optional
import tarfile
import reassemble_dicom as Dicom
import glob
import sys


def find_tars(p: str) -> typing.List[str]:
    data = list(
        filter(lambda x: x.endswith(".tar"), glob.iglob(pathname=p, recursive=True))
    )
    return data


def find_key(k: str, tars: typing.List[str]) -> Optional[bytes]:
    for tar in tars:
        with tarfile.open(tar, "r") as t:
            try:
                tar_member = t.getmember(k)
                tar_data = t.extractfile(tar_member).read()
                return tar_data
            except KeyError:
                continue
    return None


def get_metadata(k: str) -> Optional[Dict[str, Any]]:
    key = f"{k}.json"
    metadata_path = "data/text/**"
    tars = find_tars(metadata_path)
    tar_data = find_key(key, tars)
    if tar_data is not None:
        return json.loads(tar_data)
    else:
        return None


def get_image_data(k: str) -> Optional[bytes]:
    key = f"{k}.j2c"
    image_path = "data/images/**"
    tars = find_tars(image_path)
    return find_key(key, tars)


def assemble_dicom(
    metadata: Optional[Dict[str, Any]], image_data: Optional[bytes]
) -> Optional[bytes]:
    if (metadata is not None) and (image_data is not None):
        return Dicom.reassemble_dicom(metadata, image_data)
    else:
        return None


def assemble_key(k: str):
    metadata = get_metadata(k)
    img_data = get_image_data(k)
    output_bytes = assemble_dicom(metadata, img_data)
    if output_bytes is not None:
        output_path = f"{k}.dcm"
        with open(output_path, "wb") as f:
            f.write(output_bytes)


def main(key: str):
    assemble_key(key)


if __name__ == "__main__":
    main(sys.argv[1])
    # main('2.25.32906972551432148964768')
