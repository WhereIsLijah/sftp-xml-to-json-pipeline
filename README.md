# sftp-xml-to-json-pipeline
Automated Pipeline: Download XML from the SFTP, transform into JSON, and forward data to AWS S3, handling errors and data filtering.


SFTP to S3 Data Pipeline

The daily pipeline downloads XML files from an SFTP server, performs some job of regularization on them for JSON format, and then arbitrages the result into an AWS S3 bucket.

## Overview

There are three general steps in the pipeline:
1. **Download Files**: Log in to the SFTP server and download the most recent XML document, and erase it from the server.
2. **Data Conversion**: Application to convert data from XML to JSON and split up records through an average calculation of age.
3. **Upload files**: Upload JSON source files to an AWS S3 bucket.

## Preconditions

Prerequisites Make sure the code is run :
- A Python 3.x installed machine.
Required Python Libraries: `paramiko`, `xml.etree.ElementTree`, `boto3`, and `json`.
The SFTP server and AWS S3 Bucket along with its credentials are provided.

## Installations

1. Clone the repository:

```bash
git clone https://github.com/yourusername/sftp-to-s3-pipeline.git
cd sftp-to-s3-pipeline
```

2. Install the necessary libraries:
```bash
pip install paramiko boto3
```

# Key Features

## 1. Download Files
The following script connects to an sftp server, checks whether a file was modified today, downloads that file for local saving, and deletes it from the server.

SFTP Host Data:
- **Host:** testFTP.dv.com
- **User:** testuser
- Password: 123456
- **Path:** /data

## 2. Convert Files
First, the downloaded XML was simply converted to line-by-line JSON format with a list of four fields: `UserId`, `UserName`, `UserAge`, `EventTime`. Then, based on the column `UserAge` compared to average age, the records were divided into two JSONs:

- **above_average_output.json:** This will have records with `UserAge` above average.

- **below_average_output.json:** The records whose `UserAge` is less than or equal to the average.

## 3. View Files

The created JSON files will be uploaded to an AWS S3 bucket afterward.

S3 Bucket Details:

 # Running the Pipeline The following main script will suffice to run a pipeline: 
```bash
python main.py
```