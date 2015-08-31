#!/usr/bin/env python
"""
This script is run by cron once a minute - and starts parties if they are due.
"""
import logging
from datetime import datetime, timedelta
import os

from ricapi.events import submit_event
from multi.party_models import PartiesManager

logger = logging.getLogger(__name__)


def get_multi(start_at, num_users):
    cursur = start_at
    for i in range(0,99):
        get_texts(cursur ,num_users)
        cursur = cursur + num_users + 1

def get_texts(start_at, num_users):
    """Call the script that announces a party for the given partyid."""
    os.system('/opt/application/rounds/ricapi/scripts/rounds_script.sh python /opt/application/rounds/ricapi/scripts/export_text.py %s %s &' % (start_at, num_users))




if __name__ == '__main__':
    get_multi(30000008, 10000)
