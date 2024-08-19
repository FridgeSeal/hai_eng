import io
from typing import Dict, Any

import pydicom


def reassemble_dicom(headers: Dict[str, Any], pixel_data: bytes) -> bytes:
    """Reassemble a DICOM from its headers and image data.

    The resulting DICOM is returned as bytes; to be e.g.
    written out to a file.
    """
    # In reality there's a lot more nuance to it than this,
    # but this basic reassembly will do for the purposes of the exercise.
    dcm = pydicom.Dataset.from_json(headers)
    dcm.is_little_endian = True
    dcm.is_implicit_VR = False
    dcm.PixelData = pixel_data
    output = io.BytesIO()
    dcm.save_as(output)
    return output.getvalue()


if __name__ == "__main__":
    import sys
    import json

    with open(sys.argv[1]) as f:
        headers = json.loads(f.read())
    with open(sys.argv[2], "rb") as f:
        image = f.read()
    with open(sys.argv[3], "wb") as f:
        f.write(reassemble_dicom(headers, image))
