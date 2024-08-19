#
# A script to generate stub DICOM archive data for the tech task.
#
# We can't use *real* data here for obvious confidentiality reasons,
# so we've used some test files distributed with PyDICOM in order
# to produce sample archives.
#
# These DICOMs are available under the terms of the MIT license and
# are copyright the PyDICOM contributors. A copy of the license is
# included in the directory containing this file.
#

import io
import tarfile
import hashlib
import random
from pathlib import Path

import pydicom

DATA_DIR = Path(__file__).parent

NUM_ARCHIVES = 3

# These are a handful of test .dcm files distributed with PyDICOM.
# We expand them into a larger set of samepl files by re-writing
# some of the key identifiers.
SAMPLE_DICOMS = [
    "CT_small.dcm",
    "MR_small.dcm",
]


def randomish_uid(filename, count, context):
    """Deterministically generate a random-looking DICOM UID."""
    hasher = hashlib.sha256(filename.encode("ascii"))
    hasher.update(b"\n")
    hasher.update(str(count).encode("ascii"))
    hasher.update(b"\n")
    hasher.update(context.encode("ascii"))
    hasher.update(b"\n")
    return "2.25." + str(int(hasher.hexdigest()[1:20], 16))


def randomize_dicom_uids(filename, count, dcm):
    """Deterministically alter UIDs in a DICOM to make it look unique."""
    dcm.PatientID = randomish_uid(filename, count, "PatientID")
    dcm.AccessionNumber = randomish_uid(filename, count, "AccessionNumber")[5:20]
    dcm.StudyInstanceUID = randomish_uid(filename, count, "StudyInstanceUID")
    dcm.SeriesInstanceUID = randomish_uid(filename, count, "SeriesInstanceUID")
    dcm.SOPInstanceUID = randomish_uid(filename, count, "SOPInstanceUID")
    if dcm.file_meta.MediaStorageSOPInstanceUID:
        dcm.file_meta.MediaStorageSOPInstanceUID = randomish_uid(
            filename, count, "SOPInstanceUID"
        )
    return dcm


def generate_dicoms():
    """Yield same DICOMs for processing."""
    # Expand each dicom into a few different copies,
    # to make the data more interesting.
    for filename in SAMPLE_DICOMS:
        dcm_file = pydicom.data.get_testdata_file(filename)
        for i in range(10):
            dcm = pydicom.dcmread(dcm_file)
            assert dcm.is_little_endian
            assert not dcm.is_implicit_VR
            dcm = randomize_dicom_uids(filename, i, dcm)
            yield dcm


def write_text_archives():
    """Generate the sample "text" archives, containing DICOM headers."""
    archives = []
    try:
        for n in range(1, NUM_ARCHIVES + 1):
            archives.append(tarfile.open(DATA_DIR / "text" / f"text{n}.tar", "w"))
        # Stripe the dicoms across the different archives.
        # We want to shuffle them, but deterministically.
        r = random.Random(3)
        dicoms = list(generate_dicoms())
        r.shuffle(dicoms)
        for i, dcm in enumerate(dicoms):
            filename = f"{dcm.SOPInstanceUID}.json"
            del dcm.PixelData
            text_content = dcm.to_json().encode("ascii")
            tarinfo = tarfile.TarInfo(filename)
            tarinfo.size = len(text_content)
            tarinfo.type = tarfile.REGTYPE
            archives[i % NUM_ARCHIVES].addfile(tarinfo, io.BytesIO(text_content))
    finally:
        for archive in archives:
            archive.close()


def write_image_archives():
    archives = []
    try:
        for n in range(1, NUM_ARCHIVES + 1):
            archives.append(tarfile.open(DATA_DIR / "images" / f"images{n}.tar", "w"))
        # Stripe the dicoms across the different archives.
        # We want to shuffle them, but deterministically.
        # Use a different seed so that the images don't always end up
        # in the same archive as the headers.
        r = random.Random(7)
        dicoms = list(generate_dicoms())
        r.shuffle(dicoms)
        for i, dcm in enumerate(dicoms):
            # Actually I'm not sure these are always j2c format,
            # but it doesn't matter for our purposes here.
            filename = f"{dcm.SOPInstanceUID}.j2c"
            tarinfo = tarfile.TarInfo(filename)
            tarinfo.size = len(dcm.PixelData)
            tarinfo.type = tarfile.REGTYPE
            archives[i % NUM_ARCHIVES].addfile(tarinfo, io.BytesIO(dcm.PixelData))
    finally:
        for archive in archives:
            archive.close()


if __name__ == "__main__":
    write_text_archives()
    write_image_archives()
