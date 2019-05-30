import sys
from scrapy import cmdline
import os, subprocess


def main(name):
    if name:
        cmdline.execute(name.split())


if __name__ == '__main__':
    print('[*] beginning main thread')
    if os.path.isfile('output.json'):
        os.remove('output.json')
    name = "scrapy crawl eruz -o output.json --logfile logfile.txt"
    main(name)
    print('[*] main thread exited')
    print('main stop====================================================')

