#!/usr/bin/env python
# -*- coding: utf-8 -*-
import sys
from time import sleep
from apscheduler.schedulers.background import BackgroundScheduler as Scheduler
import datetime as dt

# TODO: Запихнуть rules из таблицы в Task queue. Что значит добавить rule в Task Queue?

sched = Scheduler()
sched.start()  # start the scheduler

def my_job(text):
    print dt.datetime.now(), text


def main():
    sched.add_job(my_job, 'interval', seconds=1, args=('text',))

    while True:
        sleep(2)
        sys.stdout.write(str(dt.datetime.now()) + '\n');
        sys.stdout.flush()
        task_lists = sched.get_jobs()
        pass


##############################################################

if __name__ == "__main__":
    main()
