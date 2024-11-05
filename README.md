# data_pipline

## Description
This README file provides an overview and documentation for the 'data_pipline' project. It includes information on the project's purpose, setup instructions, usage guidelines, and other relevant details to help users understand and effectively utilize the data pipeline.

## Table of Contents
- [Description](#description)
- [Installation](#installation)
- [Configuration](#configuration)
- [Running the Scripts](#running-the-scripts)
- [Scheduling with Cron](#scheduling-with-cron)
- [Logging](#logging)
- [Troubleshooting](#troubleshooting)
- [License](#license)

## Installation
To install the necessary packages, run the following command:
```bash
pip install -r requirements.txt
```

## Configuration
Ensure that you have a SQLite database named `config.db` in the `/home/coding/Workspace/data_pipline/` directory. This database should contain a table named `config_db` with the necessary configuration details.

### Example Configuration Table
```sql
CREATE TABLE config_db (
    db_type TEXT,
    host TEXT,
    port INTEGER,
    user TEXT,
    password TEXT,
    database TEXT
);
```

## Running the Scripts
Before running the `automate.py` script, you must run the `sqlserver_postgres.py` script to save the database configuration.

### Step 1: Run `sqlserver_postgres.py`
To run the `sqlserver_postgres.py` file, use the following command:
```bash
python /home/coding/Workspace/data_pipline/sqlserver_postgres.py
```

### Step 2: Run `automate.py`
After saving the database configuration, run the `automate.py` file using the following command:
```bash
python /home/coding/Workspace/data_pipline/automate.py
```

## Scheduling with Cron
To schedule the script to run every 10 minutes using cron, follow these steps:

1. Open the crontab editor:
   ```bash
   crontab -e
   ```

2. Add the following line to schedule the script:
   ```bash
   */10 * * * * /home/coding/Workspace/data_pipline/run_automate.sh
   ```

### Example `run_automate.sh` Script
```bash
#! /bin/bash
export PATH=/usr/bin:/bin:/usr/sbin:/sbin:/usr/local/bin
echo "Current time: $(date)" >> /home/coding/Workspace/data_pipline/cron_log.txt 2>&1
python3 /home/coding/Workspace/data_pipline/automate.py >> /home/coding/Workspace/data_pipline/cron_log.txt 2>&1
```

## Logging
The script logs its output to `cron_log.txt` in the `/home/coding/Workspace/data_pipline/` directory. You can check this file to see the output and any errors that occur during execution.

### Viewing the Log
To view the log, use the following command:
```bash
cat /home/coding/Workspace/data_pipline/cron_log.txt
```

## Troubleshooting
If you encounter issues when running the script with cron, consider the following steps:

1. **Check Cron Logs**: View the cron logs to see if there are any errors.
   ```bash
   grep CRON /var/log/syslog
   ```

2. **Check Permissions**: Ensure that the script has executable permissions.
   ```bash
   chmod +x /home/coding/Workspace/data_pipline/run_automate.sh
   ```

3. **Verify Paths**: Ensure that all paths in the script are absolute and correct.

4. **Environment Variables**: Make sure all necessary environment variables are set in the script.

## License
This project is licensed under the MIT License. See the LICENSE file for more details.
