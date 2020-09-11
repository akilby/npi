import argparse

from .download.nppes import main as nppes_download
from .process.nppes import main_process_variable, update_all

# main_process_variable(variable, update)


class CommandLine(object):
    """Sets up command line parser and invokes main functions"""

    def __init__(self):
        parser = argparse.ArgumentParser(
            description='Downloads, builds, and manages '
                        'NPI-identified physician data')
        subparsers = parser.add_subparsers(
            dest='subcommand', description='Subcommands required.')

        self.setup_download_parser(subparsers)
        self.setup_process_parser(subparsers)
        self.parser = parser

    def main(self):
        args = self.parser.parse_args()
        args.func(args)

    def setup_download_parser(self, subparsers):
        parser_download = subparsers.add_parser('download')
        parser_download.set_defaults(func=self.download)
        parser_download.add_argument(
            '--source', required=True,
            help='Source to download. Includes:\n'
            '\t--download NPPES')

    def setup_process_parser(self, subparsers):
        parser_process = subparsers.add_parser('process')
        parser_process.set_defaults(func=self.process)
        parser_process.add_argument(
            '--source', required=True,
            help='Source to process. Includes:\n'
            '\t--process NPPES')
        parser_process.add_argument(
            '--variable', required=False, default=None,
            help='If you only want to process one variable, '
            'specify here')
        parser_process.add_argument(
            '--update', required=False,
            choices=['True', 'Force', 'False'],
            default='True',
            help='If you want to only update [True], '
            'not destroy and recreate [False]. [Force] updates'
            'last 6 months even if there are not any new datafiles found.')
        parser_process.add_argument(
            '--max-jobs', required=False,
            default=6,
            help='runs this number of processing jobs from the login node '
                 'at a given time')
        parser_process.add_argument(
            '--exclude', required=False, nargs='+',
            default=[],
            help='exclude some variables')
        parser_process.add_argument(
            '--include', required=False, nargs='+',
            default=[],
            help='include only these variables')

    def download(self, args):
        if args.source.upper() == 'NPPES':
            nppes_download()

    def process(self, args):
        if args.source.upper() == 'NPPES':
            if args.variable:
                main_process_variable(args.variable, args.update)
            else:
                print('updating all variables except', args.exclude)
                update_all(max_jobs=args.max_jobs, exclude=args.exclude,
                           include=args.include, update=args.update)


def main():
    command_line = CommandLine()
    command_line.main()


if __name__ == '__main__':
    main()
