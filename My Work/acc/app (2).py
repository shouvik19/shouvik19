from math import ceil
import datetime
import os
import time

import numpy as np
import pandas as pd
import trellis

import general_util.email_utils as email_utils
from general_util.utils import query_to_df
from general_util import utils
from garden_jobs.credit_card.emails.card_email_upload import _dir_path


def main(user_dry_run: bool):

    step_one = utils.dremio_query(
        "starting_population_autopay_ach.sql"
    )

    df1 = step_one
    print(f"Length of ach: {len(df1)}")

    step_two = utils.dremio_query(
        "starting_population_autopay_check_debit.sql"
    )

    df2 = step_two
    print(f"Length of debit and check: {len(df2)}")

    data = pd.concat([df1, df2])
    print(f"Length of total: {len(data)}")

    data.customer_id = data.customer_id.astype(int)
    data['measurement_date'] = pd.to_datetime(data.measurement_date)
    data['run_date'] = pd.to_datetime(data.run_date)
    data = data.fillna(0)


    if (not user_dry_run) & (not data.empty):
        email_utils.main(df=data, email_name='COL_Credit_Card_Autopay', remote_path_head="upload/us")

    return data


if __name__ == "__main__":
    trellis.start()

    is_dry_run = trellis.inputs("is_dry_run")

    output = main(is_dry_run)

    trellis.output(
        "dry_run_output",
        output.to_csv(index=False),
        filename=f"avant_card_autopay_{utils.TODAY:%Y%m%d}.csv"
    )

    trellis.finish()
