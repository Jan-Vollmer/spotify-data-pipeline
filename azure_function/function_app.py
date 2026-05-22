import azure.functions as func
import logging
import sys
import os

sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..'))

from spotify_data_pipeline.Bronze.fill_bronze import fill_bronze

app = func.FunctionApp()

@app.timer_trigger(schedule="10 * * * * *", arg_name="myTimer", run_on_startup=True,
              use_monitor=False) 
def fill_bronze_trigger(myTimer: func.TimerRequest) -> None:
    logging.warning("TRIGGER START")
    if myTimer.past_due:
        logging.info('The timer is past due!')
    logging.info('Starting bronze ingestion...')
    fill_bronze(limit_top=20, limit_recent=50)
    logging.info('Bronze ingestion complete.')