# argument parser for folder_to_s3

import argparse

# max size in bytes before uploading in parts (gigabytes)
MAX_SIZE = 1

# size of parts when uploading in parts (megabytes)
PART_SIZE = 512

# maximum number of threads that will be making requests
MAX_CONCURRENCY = 8

# maximum number of processes that will share the workload
MAX_PARALLELISM = 8

# AWS region
REGION = 'eu-west-2'


def configure(args):

    # new actions for agrparser

    class MBtoB(argparse.Action):
        def __init__(self, option_strings, dest, nargs=None, **kwargs):
            if nargs is not None:
                raise ValueError("nargs not allowed")
            super(MBtoB, self).__init__(option_strings, dest, **kwargs)

        def __call__(self, parser, namespace, values, option_string=None):
            setattr(namespace, self.dest, values * (2 ** 10))

    class GBtoB(argparse.Action):
        def __init__(self, option_strings, dest, nargs=None, **kwargs):
            if nargs is not None:
                raise ValueError("nargs not allowed")
            super(GBtoB, self).__init__(option_strings, dest, **kwargs)

        def __call__(self, parser, namespace, values, option_string=None):
            setattr(namespace, self.dest, values * (2 ** 20))

    desc = ('This script uploads a nested folder on s3.\n'
            'The target folder must be in the pwd.\n'
            'Target folder can be mapped directly onto\n'
            'the bucket, or inside it.\n\nExamples:\n'
            '  -upload.py src_folder/ bucket\n'
            '  -upload.py src_folder/ bucket/folder\n')

    parser = argparse.ArgumentParser(description=desc,
                                     formatter_class=argparse.RawTextHelpFormatter)

    parser.add_argument('src',
                        help='target folder',
                        default='')

    parser.add_argument('dst',
                        help='target endpoint',
                        default='')

    parser.add_argument('-mt',
                        '--multipart_threshold',
                        metavar='threshold',
                        help='file size threshold for multipart (GB)',
                        type=int,
                        action=GBtoB,
                        default=MAX_SIZE * (1024 ** 3))

    parser.add_argument('-ms',
                        '--multipart_chunksize',
                        metavar='chunksize',
                        help='chunk size for multipart (MB)',
                        type=int,
                        action=MBtoB,
                        default=PART_SIZE * (1024 ** 2))

    parser.add_argument('-mp',
                        '--max_parallelism',
                        metavar='parallelism',
                        help='maximum number of processes per folder',
                        type=int,
                        default=MAX_PARALLELISM)

    parser.add_argument('-mc',
                        '--max_concurrency',
                        metavar='concurrency',
                        help='maximum number of threads for multipart',
                        type=int,
                        default=MAX_CONCURRENCY)

    parser.add_argument('-r',
                        '--region',
                        metavar='region',
                        help='set a different region to upload to',
                        default=str(REGION))

    parser.add_argument('-fc',
                        '--force_copy',
                        help='create the bucket if it doesnt exists',
                        action='store_true',
                        default=False)

    return parser.parse_args(args)
