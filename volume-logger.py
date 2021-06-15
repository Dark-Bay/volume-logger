#!/usr/local/bin/python
""" LED Processor Logger

Loads JSON data from Megapixel Helios or Brompton Tessera 3.1+ processors and
logs interesting changes.

j.kretschmer@dark-bay.com
"""
import argparse
import logging
import logging.handlers
import re
import sys
import time

import requests
import requests.exceptions

LOG_FILE = "/var/log/led-volume.log"
DATE_FORMAT = "%Y-%m-%d %H:%M:%S"
DEFAULT_INTERVAL = 30
BROMPTON_URL = 'http://%s/api/'
MEGAPIXEL_URL = 'http://%s/api/v1/data'
LOG_BACKUP_COUNT = 99
LOG_MAX_BYTES = 10*2**10

LOG = logging.getLogger('darkbay')
LOG.addHandler(logging.StreamHandler())
LOG.handlers[-1].setFormatter(logging.Formatter('%(asctime)s %(levelname)s: %(message)s', datefmt=DATE_FORMAT))

OUTPUT = logging.getLogger('darkbay.changes')
OUTPUT.propagate = False
OUTPUT.setLevel(logging.INFO)

VETO = [re.compile(_) for _ in [
    r".*_I$",
    r".*\.(reboots|discovered|since|uptime|runtime|freq|CPUTemp|outputFreq|current-date-time)$",
    r".*\.stringId",
    r".*\.FanCtrl\(\d+\)$",
    r".*\.SFP #\d+ [TR]x$",
    r".*\.(volts|currents|counters|temps|fans)\.[^.]+$",
    r".*\.SFP #\d+ Temp$",
    r"^_.*",
]]


class Processor:
    """ Object to track processor state
    """
    max_samples = 3
    def __init__(self, host):
        self.url = MEGAPIXEL_URL % host
        self.host = host
        self.manu = "Megapixel"
        res = requests.get(self.url, params={'sys.info.serial':''})
        if not 199 < res.status_code < 300:
            LOG.debug("Failed megapixel URL check: %s", res.status_code)
            self.url = BROMPTON_URL % host
            self.manu = "Brompton"
            res = requests.get(self.url + "system/processor-type")
            res.raise_for_status()
        self.id = self.manu + " " + self.host
        ip_parts = self.host.split(".")
        if ip_parts[-1].isdigit():
            # Unique Dark Bay setup with IP starting on 0 for processor 1.
            self.id = self.manu + " " + str(int(host[-1]) + 1)
        self.data = []
        LOG.info("Initialized %s", self)

    def sample(self):
        """ Queries the host and keeps the results for comparison
        """
        timestamp = time.time()
        try:
            res = requests.get(self.url)
        except requests.exceptions.ConnectionError as error:
            LOG.warning("%s %s", self, error)
            return
        if 199 < res.status_code < 300:
            self.data.append((timestamp, res.json()))
            LOG.debug("%s appended data sample", self)
        else:
            LOG.warning("Error %s loading data from %s", res.status_code, self)
        self.data = self.data[-self.max_samples:]

    def __repr__(self):
        return "<%s>" % (self.id)

    def compare(self):
        """ Log differences between samples """
        samples = self.data[-2:]
        if len(samples) != 2:
            return

        timestamp_a, data_a = samples[0]
        timestamp_b, data_b = samples[1]
        LOG.debug("%s comparing sample from %s to %s", self, timestamp_a, timestamp_b)
        changes = dict_compare(data_a, data_b)
        for key in changes:
            OUTPUT.info("%s:%s: %s -> %s", self, key, get_value(data_a, key), get_value(data_b, key))


def get_value(cursor, address):
    """ Uses a dotted address to find a nested dictionary value
    """
    for part in address.split('.'):
        try:
            cursor = cursor[part]
        except KeyError:
            LOG.warning("KeyError: %s (%s)", part, address)
            return
    return cursor


def dict_compare(a, b, path=None):
    """ Recursively inspect a dictionary values and keys

    Ignores keys that match any VETO regex
    """
    result = []
    if path is None:
        path = []
    for k, v in a.items():
        working_path = path + [k]
        path_str = '.'.join(working_path)
        if k not in b:
            result.append(path_str)
        elif isinstance(v, dict):
            result.extend(dict_compare(v, b[k], path=working_path))
        elif any([_.match(path_str) for _ in VETO]):
            continue
        elif isinstance(v, list):
            if len(set(v) - set(b[k])) != 0 or len(set(b[k]) - set(v)) == 0:
                result.append(working_path.join('.'))
        else:
            if v != b[k]:
                result.append(path_str)
    for k in (set(b.keys()) - set(a.keys())):
        working_path = path + [k]
        result.append('.'.join(working_path))
    return result


def parse_args():
    args = argparse.ArgumentParser()
    args.add_argument('hosts', metavar='HOST', nargs='+', help="Host address of processor")
    args.add_argument('-v', '--verbose', action='store_true', help="Verbose output")
    args.add_argument('-q', '--quiet', action='store_true', help="Ignore warnings")
    args.add_argument('-d', '--debug', action='store_true', help="Show debug messages")
    args.add_argument(
        '-l', '--logfile', default=LOG_FILE,
        help="Writeable filepath for log data (default:%s)" % LOG_FILE)
    args.add_argument(
        '-i', '--interval', type=float, default=DEFAULT_INTERVAL,
        help="Check interval in seconds (default: %s)" % DEFAULT_INTERVAL)

    return args.parse_args()

def main():
    """ Loop forever logging any changes
    """
    args = parse_args()
    level = logging.WARNING
    if args.quiet:
        level = logging.ERROR
    if args.verbose:
        level = logging.INFO
    if args.debug:
        level = logging.DEBUG
    LOG.setLevel(level)

    log_handler = logging.handlers.RotatingFileHandler(
        args.logfile, maxBytes=LOG_MAX_BYTES, backupCount=LOG_BACKUP_COUNT)
    OUTPUT.addHandler(log_handler)
    OUTPUT.handlers[-1].setFormatter(logging.Formatter('%(asctime)s %(message)s', datefmt=DATE_FORMAT))

    processors = []
    LOG.debug("Initializing processor objects")
    for host in args.hosts:
        try:
            processors.append(Processor(host))
        except (requests.exceptions.ConnectionError, requests.exceptions.HTTPError) as error:
            LOG.warning("Ignoring %s due to %s", host, error)
            continue
    if len(args.hosts) != len(processors):
        LOG.error("%d host(s) will be ignored for this session.", len(args.hosts) - len(processors))

    OUTPUT.info("(Process started)")
    while True:
        try:
            for proc in processors:
                proc.sample()
                proc.compare()
        except (KeyboardInterrupt, SystemExit) as error:
            OUTPUT.info("(Process exiting)")
            sys.exit(0)
        LOG.debug("Sleeping %s %0.1f", args.interval)
        time.sleep(args.interval)


if __name__ == '__main__':
    main()
