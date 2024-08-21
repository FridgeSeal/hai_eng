# Technical Assessment Task for data-focussed Software Engineering roles at harrison.ai

This is a data-focussed technical assessment task for Software Engineering roles at [harrison.ai](harrison.ai).
It consists of two parts: a coding exercise, and a design exercise.

You should take a copy of this repo, add your solutions for each part, and push them to GitHub for review and discussion in a follow-up interview.
You're welcome to use a private repository, as long as it's visible to the following users:

* @rfk
* @timlesie
* @oliverdaff
* @TanyaSrinidhi
* @suzy-hai
* @daniellohrey-hai

# Notes to Reviewers

Please find my python file for task 1 in `tech_test.py`.
There are no external dependencies, bar the provided `pydicom` module.

The document for the second task is found in `system_design.md`.
Please note, it references 2 images (found in `documentation_assets/` for handy diagrams), if moved around, please move these accordingly, lest the references break.



## Background: DICOM

Digital Imaging and Communications in Medicine (DICOM) is a standard format for sharing medical image information.
As a world-leading medical AI company, we work with DICOM *a lot*.
This tech task is a simplified version of a real data-processing task the team has faced.

Don't worry, you don't need to be familar with DICOM in order to complete the task!

DICOM files include both image data (e.g. pixels from a CT scan) and headers (e.g. patient
and study identifiers, and information about the originating device). These are typically
stored together in a binary file with extension `.dcm`, which is the preferred format for
our AI models to work with.

However, when we integrate with partner systems, we sometimes find the headers and image
data stored as two separate files:

* DICOM headers stored as JSON data in a text file
* DICOM image data stored as raw image data in a binary file

You can find an example of such data in `./data/sample.json` (DICOM headers)
and `./data/sample.j2c` (image data).

In such cases, we need to *re-assemble* the DICOM by combining the data from the two
files. The file `reassemble_dicom.py` provides a function for doing this reassembly.
You can try it out like so:

```
python ./reassemble_dicom.py ./data/sample.json ./data/sample.j2c ./sample.dcm
```

This should combine the data from the two files to produce `sample.dcm`.
If you're curious what these files look like in practice, try opening the resulting
`sample.dcm` in the free [Weasis DICOM viewer](https://nroduit.github.io/en/index.html).


## Question One: Reassemble DICOMs from Archives

This is a small coding task, which we expect should take no more than an hour or two
of your time. It's a chance for you to demonstrate your familiarity with Python,
but more importantly, it's an opportunity to familiarise yourself with the data model
for the second question.

---

We have obtained some DICOM data from a partner and want to use it for AI model training.
The partner's systems store DICOMs as separate header and image files; after the files have
been processed to remove any identifying information, the result is data spread across two
separate sets of tar archives:

* The "text" archives in `./data/text/` contain de-identified DICOM header data
* The "image" archives in `./data/images/` contain de-identified DICOM image data

Each DICOM has a unique identifier called its "SOP Instance UID". Your task is to write
a command-line script that can, given a SOP Instance UID, find and reassemble the data
for that DICOM from the provided tar archives.

For example:

* The sample data contains a DICOM with SOP Instance UID `2.25.32906972551432148964768`.
* The headers for this DICOM are found in a file
  named `2.25.32906972551432148964768.json` in the text archives.
* The image data for this DICOM is found in a file
  named `2.25.32906972551432148964768.j2c` in the image archives.

Your script should be able to accept `2.25.32906972551432148964768` as a command-line argument,
locate the corresponding files in the tar archives, and re-assemble them to produce
an output file named `2.25.32906972551432148964768.dcm`.

Your script should be written in Python, and may use third-party dependencies if desired.


## Question Two: Now let's do it a billion times

This is a system-design task, which will be explored together in more detail
as part of the second interview. There's no one right answer here - we're mainly
interested in seeing the way you think about the problem and the various
challenges/tradeoffs that might be involved.

---

In practice, we have obtained several billion de-identified DICOMs from the partner.
They are stored in several million "text" and "image" tar archives that are
similar in shape to the sample data from question 1, but much bigger. The total
data volume across all archives is several petabytes.

The archives are currently stored in Amazon S3 Glacier Flexible Retrieval awaiting further processing.
To be able to use these DICOMs for model training, they will need to be re-assembled
and made available in a separate S3 bucket.

How can we make these billions of DICOMs available for AI model training
in an efficient and cost-effective manner?

Provide a design document for a data-processing pipeline that can re-assemble these
DICOMs at scale and make the resulting `.dcm` files available as objects in
an S3 bucket. The system will need to run in Amazon AWS, and you're encouraged
to be specific about technology choices.

Please commit the design document and any supporting material, diagrams, etc to the
github repository as part of your submission.

There's no one right answer here, and some parts of the problem are deliberately
under-specified. The aim is for you to demonstrate how you'd approach the problem,
and to have something we can discuss together, and expand on, in a follow-up interview.
