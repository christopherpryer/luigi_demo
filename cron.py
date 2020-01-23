import datetime

import luigi

from apscheduler.schedulers.blocking import BlockingScheduler
sched = BlockingScheduler()

import scripts.top_artists
import scripts.top_opportunities

# TODO: ...