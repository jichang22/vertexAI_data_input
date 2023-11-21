# this function loads github wiki data into gcp bucket

from google.cloud import storage
from google.cloud.storage import Client, transfer_manager
import os
from git import Repo
import shutil

# help function to extract a list of filenames to be uploaded
def list_mds(data_repo):
    md_list = []
    file_counts = len(os.listdir(data_repo))
    print(file_counts)

    for file in os.listdir(data_repo):
     if file.endswith(".md"): 
         md_list.append(file)
         #print(filename)
    
    #md_list_filtered = [ x for x in md_list if "_" not in x ]
    print(len(md_list))
    return md_list

# helper function to test and view the list of buckets in the project
def list_bucket_names(project_id):
    storage_client = storage.Client(project=project_id)
    buckets = storage_client.list_buckets()
    print("Buckets:")
    for bucket in buckets:
        print(bucket.name)
    print("Listed all storage buckets.")

# helper function to download wiki files to local drive
def download_wiki_local(git_link, local_dir):
    if os.path.exists(local_dir):
        shutil.rmtree(local_dir)
    os.makedirs(local_dir)
    
    repo_url = git_link
    repo = Repo.clone_from(repo_url, local_dir)
    
# upload a single file to bucket
def upload_blob(bucket_name, local_dir, destination_blob_name, project_id):
    storage_client = storage.Client(project=project_id)
    bucket_check = storage_client.bucket(bucket_name)
    if bucket_check.exists():
        bucket = bucket_check
    else:
        bucket = storage_client.create_bucket(bucket_name)
    blob = bucket.blob(destination_blob_name)

    generation_match_precondition = 0
    blob.upload_from_filename(local_dir, if_generation_match=generation_match_precondition)
    print(
        f"File {local_dir} uploaded to {destination_blob_name}."
    )

# upload multiple files to bucket
def upload_blobs(bucket_name, local_dir, project_id, workers=8):
    storage_client = storage.Client(project=project_id)
    bucket = storage_client.bucket(bucket_name)
    filenames = list_mds(local_dir)

    results = transfer_manager.upload_many_from_filenames(
        bucket, filenames, source_directory=local_dir, max_workers=workers
    )

    for name, result in zip(filenames, results):
        if isinstance(result, Exception):
            print("Failed to upload {} due to exception: {}".format(name, result))
        else:
            print("Uploaded {} to {}.".format(name, bucket.name))

# main function
def retrieve_wikis(bucket_name, local_dir, project_id, git_link):
    print("download wiki files")
    download_wiki_local(git_link, local_dir)
    print("begin uploading")
    upload_blobs(bucket_name, local_dir, project_id)
    print("finished uploading")


if __name__ == '__main__':
  git_link = "https://github.com/PHACDataHub/Wiki.git"
  local_dir = './data'
  destination_blob_name = "test-wiki"
  # bucket name has to be gloabally unique
  bucket_name = "your-bucket-name"
  test_file = local_dir + "/Cloud-Support.md"
  project_id = 'gcp-project-id'
  
  #download_wiki_local(git_link, local_dir)
  #list_mds(local_dir)
  #upload_blob(bucket_name,test_file,destination_blob_name)
  #list_bucket_names(project_id)
  #upload_blob(bucket_name, test_file, destination_blob_name, project_id)
  #upload_blobs(bucket_name, local_dir, project_id)
  retrieve_wikis(bucket_name, local_dir, project_id, git_link)
