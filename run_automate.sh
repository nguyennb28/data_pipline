#! /bin/bash
echo "Current time: $(date)" >> /home/coding/Workspace/data_pipline/cron_log.txt 2>&1
/home/coding/Workspace/data_pipline/myvenv/bin/python3 /home/coding/Workspace/data_pipline/automate.py >> /home/coding/Workspace/data_pipline/cron_log.txt 2>&1