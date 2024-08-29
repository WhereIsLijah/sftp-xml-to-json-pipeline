import paramiko
import os
import json
import xml.etree.ElementTree as ET
from datetime import datetime, timezone
import boto3
from botocore.exceptions import NoCredentialsError, ClientError

# Define the credentials and paths for SFTP and AWS S3
SFTP_HOST = 'testFTP.dv.com'
SFTP_USER = 'testuser'
SFTP_PASSWORD = '123456'
SFTP_PATH = '/data'
LOCAL_DOWNLOAD_PATH = './downloaded_file.xml'

S3_BUCKET = 'testbucket'
S3_PATH = 'output'
AWS_ACCESS_KEY = 'testKeyString'
AWS_SECRET_KEY = 'testKeyString'

def download_file_from_sftp():
    """Connect to the SFTP server, find the latest file, download it, and delete it from the server."""
    try:
        # Establish a connection to the SFTP server
        transport = paramiko.Transport((SFTP_HOST, 22))
        transport.connect(username=SFTP_USER, password=SFTP_PASSWORD)
        sftp = paramiko.SFTPClient.from_transport(transport)

        # List all files in the specified directory on the SFTP server
        files = sftp.listdir(SFTP_PATH)

        for file in files:
            file_path = os.path.join(SFTP_PATH, file)
            file_attr = sftp.stat(file_path)
            last_modified = datetime.fromtimestamp(file_attr.st_mtime, timezone.utc)

            # Check if the file was modified today
            if last_modified.date() == datetime.now(timezone.utc).date():
                # Download the file locally and delete it from the server
                sftp.get(file_path, LOCAL_DOWNLOAD_PATH)
                sftp.remove(file_path)
                print(f"Downloaded and deleted file: {file}")
                break

        # Close the SFTP connection
        sftp.close()
        transport.close()

    except Exception as e:
        print(f"Failed to download file from SFTP: {e}")

def transform_xml_to_json():
    """Convert the downloaded XML file into JSON format, split by average age, and save to separate files."""
    try:
        # Parse the XML file
        tree = ET.parse(LOCAL_DOWNLOAD_PATH)
        root = tree.getroot()

        users = []
        # Extract user data from the XML structure
        for user in root.findall('User'):
            user_data = {
                'UserID': user.find('UserID').text,
                'UserName': user.find('UserName').text,
                'UserAge': int(user.find('UserAge').text),
                'EventTime': datetime.strptime(user.find('EventTime').text, '%Y-%m-%dT%H:%M:%S').strftime('%Y-%m-%dT%H:%M:%S.000Z')
            }
            users.append(user_data)

        # Calculate the average age of users
        user_age_avg = sum(user['UserAge'] for user in users) / len(users)

        # Separate users into above and below average age groups
        above_average_users = [user for user in users if user['UserAge'] > user_age_avg]
        below_average_users = [user for user in users if user['UserAge'] <= user_age_avg]

        # Write the results to JSON files, one for each group
        with open('above_average_output.json', 'w') as f:
            for user in above_average_users:
                f.write(json.dumps(user) + '\n')

        with open('below_average_output.json', 'w') as f:
            for user in below_average_users:
                f.write(json.dumps(user) + '\n')

        print("Transformation completed successfully.")

    except Exception as e:
        print(f"Failed to transform XML to JSON: {e}")

def upload_files_to_s3():
    """Upload the JSON files to the specified AWS S3 bucket."""
    try:
        # Initialize the S3 client
        s3_client = boto3.client('s3', aws_access_key_id=AWS_ACCESS_KEY, aws_secret_access_key=AWS_SECRET_KEY)

        # Upload each JSON file to the S3 bucket
        for file_name in ['above_average_output.json', 'below_average_output.json']:
            s3_client.upload_file(file_name, S3_BUCKET, f"{S3_PATH}/{file_name}")
            print(f"Uploaded {file_name} to S3 bucket {S3_BUCKET}.")

    except FileNotFoundError:
        print("The file was not found")
    except NoCredentialsError:
        print("Credentials not available")
    except ClientError as e:
        print(f"Failed to upload files to S3: {e}")

if __name__ == "__main__":
    # Step 1: Download the latest file from the SFTP server
    download_file_from_sftp()

    # Step 2: Transform the XML file into JSON format
    transform_xml_to_json()

    # Step 3: Upload the JSON files to the AWS S3 bucket
    upload_files_to_s3()
