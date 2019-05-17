import os
import re
import progressbar
import tempfile
from datetime import datetime

from slackclient import SlackClient

bar = None
tempFile = None

QUERY_STRING = "\"pipeline has stopped\" in:bank-ops"
MSG_PATTERN = "\<\!here\> Failure of (\w+) (\w\s)+\, (\w+) (\w+)\. Operator attention required"

def fetch_messages():
    global tempFile

    tempFile = tempfile.NamedTemporaryFile(mode='a', delete=False)

    print("--- Downloading Slack messages ---")
    print("\tFile: " + tempFile.name)

    print("--- Farm messages ---")
    farm_messages()

    print("\tFinished. Fin. Finito. Koniec.")



def farm_messages():
    search(
        QUERY_STRING,
        "Progress ",
        msg_processor
    )

def search(query_string, progress_prefix, record_processor):
    slack_token = os.environ["SLACK_API_TOKEN"]
    sc = SlackClient(slack_token)

    has_matches = True
    current_page = 1
    while has_matches:
        results = sc.api_call(
            "search.messages",
            query = query_string,
            sort = "timestamp",
            sort_dir = "asc",
            page = current_page
        )
        has_matches = len(results['messages']['matches'])
        total_pages = results['messages']['pagination']['page_count']
        print_progress_bar(
            iteration=current_page,
            total=total_pages,
            prefix=progress_prefix
        )
        current_page = current_page + 1

        record_processor(results['messages']['matches'])


def msg_processor(message_matches):

    for msg in message_matches:
        # timestamp format: 1554287378.728900
        int_ts = int(msg['ts'].split('.')[0])
        utc_ts = datetime.utcfromtimestamp(int_ts)

        # extract full ISO-8601
        ts = utc_ts.isoformat()

        # extract fields from message
        msg_match = re.search(MSG_PATTERN, msg['text'], re.IGNORECASE)
        machinery_name = msg_match.group(1) if msg_match else ''
        machinery_make = msg_match.group(2) if msg_match else ''
        farm_type = msg_match.group(3) if msg_match else ''
        farm_id = msg_match.group(4) if msg_match else ''

        print_out(','.join([ts, machinery_name, machinery_make, farm_type, farm_id]))


def print_out(line):
    global tempFile

    tempFile.write(line + "\n")


# Print iterations progress
def print_progress_bar (iteration, total, prefix):
    global bar

    if bar == None:
        bar = progressbar.ProgressBar(maxval=total, \
                                    widgets=[progressbar.Bar('=', prefix + '[', ']'), ' ', progressbar.Percentage()])
        bar.start()

    if iteration < total:
        bar.update(iteration)
    elif iteration == total:
        bar.finish()
    else:
        bar = None


if __name__ == "__main__":
    fetch_messages()
