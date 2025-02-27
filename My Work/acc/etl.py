import pandas as pd
from collections import OrderedDict


from ..segment_post_utils import (
    read_sql,
    dremio_query_prod,
    generate_identify_object,
    generate_track_object,
)


def create_responsys_df(data):
    responsys_df = data.drop(
        [
            "userId",
            "firstName",
            "lastName",
            "createdAt",
            "email",
            "email_subscription_state",
            "address_state",
            "eligible_date",
            "expiry_date",
        ],
        axis=1,
        inplace=False,
    )
    print(f"responsys_df's columns: {responsys_df.columns}")
    return responsys_df


def extract_dataframe_from_query_original():
    step_ach = dremio_query_prod(
        read_sql(
            "./credit_card/emails/card_email_upload/col_credit_card_autopay/sqls/starting_population_autopay_ach.sql"
        )
    )
    print(f"Length of the step_ach dataset: {len(step_ach)}")

    step_two = dremio_query_prod(
        read_sql(
            "./credit_card/emails/card_email_upload/col_credit_card_autopay/sqls/starting_population_autopay_check_debit.sql"
        )
    )
    print(f"Length of the step_two dataset: {len(step_two)}")

    data = pd.concat([step_ach, step_two])

    print(f"Length of the entire original dataset: {len(data)}")

    data.customer_id = data.customer_id.astype(int)
    data["measurement_date"] = pd.to_datetime(data.measurement_date)
    data["run_date"] = pd.to_datetime(data.run_date)
    data = data.fillna(0)

    return data


def join_df_with_customer_data(responsys_data):
    cust_ids = "("
    for i in list(responsys_data["customer_id"]):
        cust_ids = cust_ids + str(i) + ", "
    cust_ids = cust_ids[:-2] + ")"

    customer_df = dremio_query_prod(
        read_sql(
            "./credit_card/emails/card_email_upload/col_credit_card_autopay/sqls/customer_data_and_other_cols_definition_autopay.sql"
        ).format(customer_id=cust_ids)
    )
    segment_data = pd.merge(responsys_data, customer_df, how="left", on="customer_id")

    print(f"Length of the segment dataset: {len(segment_data)}")

    return segment_data


def transform_columns(data):
    data.customer_id = data.customer_id.astype(int)
    data["payment_due_date"] = pd.to_datetime(data.payment_due_date)
    data["days_before_payment_due"] = data["days_before_payment_due"].astype(int)
    data = data.fillna(0)

    return data


def create_segment_objects(data, query, event_code, event_desc, test_sample_flag=False):
    segment_events = OrderedDict()

    segment_events[event_code] = {
        "event": event_desc,
        "query": query,
        "context": False,
        "context": False,
        "properties": {
            "product_type": "string",
            "cc_account_id": "string",
            "days_before_payment_due": "integer",
            "payment_due_date": "timestamp",
            "eligible_date": "timestamp",
            "expiry_date": "timestamp",
        },
    }
    identify = generate_identify_object(data)
    track = generate_track_object(
        event_config=segment_events[event_code],
        df=data,
    )

    if test_sample_flag:
        print("Only posting 100 records to Segment")
        data = data.head(100)
        identify = generate_identify_object(data)
        track = generate_track_object(
            event_config=segment_events[event_code],
            df=data,
        )

    print(f"identify for {event_code}: {identify[0]}")
    print(f"track for {event_code}: {track[0]}")

    return identify, track
