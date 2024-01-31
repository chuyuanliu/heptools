# TODO placeholder logging interface
from rich import print


class log:
    @classmethod
    def info(cls, msg):
        print(msg)

    @classmethod
    def warning(cls, msg):
        print(f'[yellow]{msg}[/yellow]')

    @classmethod
    def error(cls, msg):
        print(f'[red]{msg}[/red]')

    @classmethod
    def input(cls, msg):
        print(msg, end=' ')
        return input()
