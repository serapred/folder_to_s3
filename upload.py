# folder to s3
# Multi-tear function for recursive upload of hierarchical structures
# a process pool handles parallelism while the transfert module use
# threading to leverage concurrency in case of large files


import boto3
import botocore
import os
import utils.path
from concurrent.futures import ProcessPoolExecutor
from concurrent.futures.process import BrokenProcessPool


# checks if the bucket exist


def __bucket_exists(bucket):

    print('reaching %s...' % bucket)

    try:
        s3 = boto3.resource('s3')
        s3.meta.client.head_bucket(Bucket=bucket)
    except botocore.exceptions.ClientError as e:
        if e.response['Error']['Code'] == '404':
            print('(404) %s not found' % bucket)
            return False
        else:
            raise e
    else:
        return True


# deelete a folder inside of a bucket

def delete_folder(bucket, folder, region):

    client = boto3.client('s3', region_name=region)

    # paginator allows to overcome the 1000 object per
    # request limit of delete_object()
    paginator = client.get_paginator('list_objects_v2')
    pages = paginator.paginate(Bucket=bucket, Prefix=folder)

    # to limit the number of requests, the objects are deleted
    # in batches of 1000

    to_delete = dict(Objects=[])
    for item in pages.search('Contents'):
        to_delete['Objects'].append(dict(Key=item['Key']))

        # flush once aws limit reached
        if len(to_delete['Objects']) >= 1000:
            client.delete_objects(Bucket=bucket, Delete=to_delete)
            to_delete = dict(Objects=[])

    # flush rest
    if len(to_delete['Objects']):
        client.delete_objects(Bucket=bucket, Delete=to_delete)


# upload a file into an s3 bucket


def file_to_s3(bucket, src, dst, conf, callback=utils.path.progress):

    s3 = boto3.resource('s3')
    try:
        s3.meta.client.upload_file(src,
                                   bucket,
                                   dst,
                                   Config=conf,
                                   Callback=callback(src)
                                   )
    except botocore.exceptions.ClientError as e:
        raise e


def folder_to_s3(src, dst, region, max_parallelism=1, force_copy=False, **kwargs):
    """
    uploads a local directory (src) into the bucket.
    the root folder can either be the bucket itself
    or a folder inside a bucket

    Args:
        src (string): local folder
        dst (string): destination endpoint (i.e. bucket/folder)
        region (string): destinatary region code (i.e. eu-west-2)
        max_parallelism (int): number of concurrent processes
        force_copy (bool, optional): force bucket creation if non-existent
        **kwargs: transfert configuration keyword arguments
    """
    bucket, root = utils.path.reverse_split(dst)

    s3 = boto3.resource('s3')

    # check if the bucket exists
    if not __bucket_exists(bucket):

        if force_copy:
            print('creating bucket: ' + bucket)

            try:
                s3.create_bucket(Bucket=bucket,
                                 CreateBucketConfiguration={'LocationConstraint': region})
            except botocore.exceptions.ClientError as e:
                raise e
        else:
            exit(-1)

    # instanciate transfer configuration
    conf = boto3.s3.transfer.TransferConfig(use_threads=True, **kwargs)

    # start uploading
    with ProcessPoolExecutor(max_workers=max_parallelism) as executor:
        try:
            for file in utils.path.dir_tree(src):
                # removes the root so that it can be
                # later added to the input string
                suffix = file.replace(src, '')
                executor.submit(file_to_s3,
                                bucket,
                                file,
                                os.path.join(root, suffix),
                                conf,
                                utils.path.progress
                                )

        except (BrokenProcessPool):
            try:
                # deleting the bucket if created
                # to do so, the bucket must be empty
                print("removing %s from %s" % (root, bucket))
                delete_folder(bucket, root, region)
                if force_copy:
                    print("attempting to delete %s" % bucket)
                    s3.Bucket(bucket).delete()

            except botocore.exceptions.ClientError as e:
                print("operation failed: %s" % e)
                exit(-1)

            else:
                print("operation aborted. exiting...")
                exit(0)


def main(args):
    import utils.parser
    args = utils.parser.configure(args)
    folder_to_s3(**vars(args))


if __name__ == '__main__':

    '''
    Usage:
    python upload.py local_folder bucket / root_folder[-options]
    '''

    import sys
    main(sys.argv[1:])
