import argparse

from .download.nppes import main as nppes_download


class CommandLine(object):
    """Sets up command line parser and invokes main functions"""

    def __init__(self):
        parser = argparse.ArgumentParser(
            description='Downloads, builds, and manages '
                        'NPI-identified physician data')
        subparsers = parser.add_subparsers(
            dest='subcommand', description='Subcommands required.')

        self.setup_download_parser(subparsers)
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

    def download(self, args):
        if args.source.upper() == 'NPPES':
            nppes_download()


def main():
    command_line = CommandLine()
    command_line.main()


if __name__ == '__main__':
    main()
