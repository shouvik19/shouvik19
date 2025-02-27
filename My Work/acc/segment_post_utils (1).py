import sys
import os
import json

import io
from io import BytesIO

import datetime
from datetime import date, datetime, timedelta

from dateutil.tz import tzutc

from collections import OrderedDict

from requests.auth import HTTPBasicAuth
from requests import sessions
from pytz import timezone

from gzip import GzipFile

import logging

import numpy as np
import pandas as pd

from datalakeutils.connectors.dremio_db import DremioDB
from datalakeutils.analysis.dremio.table import AppendableTable

import trellis

import time
import paramiko
import tempfile

# import analytics
from segment.analytics.version import VERSION
from segment.analytics.utils import remove_trailing_slash

_session = sessions.Session()

CUR_DIR = os.path.dirname(os.path.abspath(__file__))
SQL_DIR = os.path.join(CUR_DIR, "sql")

# @retry(stop_max_attempt_number=3)
# def dremio_sql(query, env='prod'):
#     with DremioDB(env) as dremio:
#         result = dremio.execute_df(query)
#     return result

# def read_sql(query_name):
#     query_path = os.path.join(SQL_DIR, query_name)
#     with open(query_path) as f:
#         txt = f.read()
#     return txt


def dremio_query_prod(query, env="prod"):
    with DremioDB(env) as dremio:
        result = dremio.execute_df(query)
    return result


def read_sql(f_name):
    f_path = os.path.join(f_name)
    with open(f_path, "r") as f:
        contents = f.read()
    return contents


prod_conn = trellis.connect("us_prod.application_user_ro")
prod_conn_b = trellis.connect("us_prod.follower.marketing30min")


def prod_sql(query):
    return pd.read_sql(query, prod_conn_b)


test_write_key = "S5kH4JOuLu5JbBwYOdaRWjmsXyLA8fvv"

### Event Definitions #######################################################################################################

context_config = OrderedDict()

context_config["campaign"] = {
    "properties": {
        "source": "utm_source",
        "name": "utm_campaign",
        "term": "utm_term",
        "medium": "utm_medium",
        "content": "utm_content",
        "gclid": "gclid",
    }
}

context_config["traits"] = {"properties": {"email": "email"}}


### Segment HTTPS API functions #############################################################################################


class DatetimeSerializer(json.JSONEncoder):
    def default(self, obj):
        if isinstance(obj, (date, datetime)):
            return obj.isoformat()

        return json.JSONEncoder.default(self, obj)


def post_to_segment_https(
    write_key, host=None, gzip=False, timeout=15, proxies=None, endpoint=None, **kwargs
):
    """Post the `kwargs` to the API"""

    log = logging.getLogger("segment")

    body = kwargs
    body["sentAt"] = datetime.utcnow().replace(tzinfo=tzutc()).isoformat()

    url = remove_trailing_slash(host or "https://api.segment.io") + "/v1/{}".format(
        endpoint
    )
    print("Posting to:", url)

    auth = HTTPBasicAuth(write_key, "")
    data = json.dumps(body, cls=DatetimeSerializer)

    log.debug("making request: %s", data)

    headers = {
        "Content-Type": "application/json",
        "User-Agent": "analytics-python/" + VERSION,
    }

    if gzip:
        headers["Content-Encoding"] = "gzip"
        buf = BytesIO()
        with GzipFile(fileobj=buf, mode="w") as gz:
            # 'data' was produced by json.dumps(),
            # whose default encoding is utf-8.
            gz.write(data.encode("utf-8"))
        data = buf.getvalue()

    kwargs = {
        "data": data,
        "auth": auth,
        "headers": headers,
        "timeout": 15,
    }

    # print(kwargs)

    if proxies:
        kwargs["proxies"] = proxies

    res = _session.post(url, data=data, auth=auth, headers=headers, timeout=timeout)

    print(res.status_code)

    if res.status_code == 200:
        log.debug("data uploaded successfully")
        return res

    try:
        payload = res.json()
        print(payload)
        log.debug("received response: %s", payload)

        raise APIError(res.status_code, payload["code"], payload["message"])
    except ValueError:
        raise APIError(res.status_code, "unknown", res.text)


### post_to_segment #########################################################################################################


def split_list(list_a, chunk_size):
    for i in range(0, len(list_a), chunk_size):
        yield list_a[i : i + chunk_size]


# TODO: DELETE "post_to_segment" to make it an internal function
# TODO: This function should only be called through the post_segment_data function
def post_to_segment(call=None, payload=None, endpoint=None, envwritekey=None):
    _post_to_segment(
        call=call, payload=payload, endpoint=endpoint, envwritekey=envwritekey
    )


def _post_to_segment(call=None, payload=None, endpoint=None, envwritekey=None):
    print("Posting {} to Segment".format(call))

    print("Number of records : {}".format(len(payload)))
    print("Size in bytes     : {}".format(sys.getsizeof(payload)))

    if len(payload) >= 500:
        split_payload = list(split_list(payload, 500))

        print("Splitting {} into chunks: {}".format(call, len(split_payload)))

        for i, payload_chunk in enumerate(split_payload):
            print("Chunk {}".format(i))
            print(len(payload_chunk))

            batch = {"batch": payload_chunk}

            post_to_segment_https(
                write_key=envwritekey,
                host=None,
                gzip=False,
                timeout=100,
                proxies=None,
                endpoint="batch",
                **batch,
            )

            time.sleep(2)

    else:
        batch = {"batch": payload}

        post_to_segment_https(
            write_key=envwritekey,
            host=None,
            gzip=False,
            timeout=100,
            proxies=None,
            endpoint="batch",
            **batch,
        )


##########################


def create_context(row, context_config):
    contexts = {}

    for k, v in context_config.items():
        contexts[k] = {}

        if len(v["properties"]) > 0:
            for p, o in v["properties"].items():
                if o in row:
                    if len(row[o]) > 0:
                        contexts[k].update({p: row[o]})
    #                    else:
    #                     contexts[k].update({p : ''})

    if len(contexts) > 0:
        d = dict([(k, v) for k, v in contexts.items() if len(v) > 0])

        return d
    else:
        return {}


def create_address(row):
    address = {}

    if "address_state" in row:
        if row["address_state"] != "":
            address_elements = {}
            address_elements["state"] = row["address_state"]

        if len(address_elements) > 0:
            return address_elements
        else:
            return np.nan


raw_traits_list = [
    "userId",
    "firstName",
    "lastName",
    "createdAt",
    "customer_id",
    "email",
    "address_state",
    "email_subscription_state",
]
traits_list = [
    "firstName",
    "lastName",
    "createdAt",
    "customer_id",
    "email",
    "address",
    "email_subscription_state",
]


def execute_queries(segment_events, SQL_DIR, prod=False):
    users_list = []
    events_dict = {}

    for k, v in segment_events.items():
        event_name = v["event"]
        query_file = v["query"]

        print("Event slug        : {}".format(k))
        print("Event name        : {}".format(event_name))
        print("Query file        : {}".format(query_file))

        query = read_sql(query_file)

        if prod == False:
            df_raw = dremio_sql(query)
        else:
            print("Running prod query")
            df_raw = prod_sql(query)

        df_raw = df_raw.rename(
            columns={
                "userid": "userId",
                "firstname": "firstName",
                "lastname": "lastName",
                "createdat": "createdAt",
            }
        )

        print("Number of records : {}".format(len(df_raw)))

        if len(df_raw) > 0:
            # df_raw = df_raw.iloc[:5]

            for cols in df_raw.select_dtypes(include="datetime").columns:
                df_raw[cols] = pd.to_datetime(df_raw[cols])
                # df_raw[cols] = df_raw[cols].apply(lambda x: x.tz_localize(tz='US/Central').isoformat())
                df_raw[cols] = df_raw[cols].apply(lambda x: x.isoformat())

            users_temp = df_raw[raw_traits_list]
            users_list.append(users_temp)

            events_dict[k] = df_raw

            print(df_raw.info())

        else:
            print("No user / event records found for {}".format(event_name))

        print("Finished: {}".format(event_name))
        print("=" * 80)

    return users_list, events_dict


def process_users(users_list):
    df_identify = pd.concat(users_list, axis=0)
    df_identify = df_identify.reset_index(drop=True)

    print("Total users        : {}".format(len(df_identify)))

    df_identify = df_identify.drop_duplicates(["userId", "email"], keep="first")

    print("Unique users       : {}".format(len(df_identify)))

    df_identify.loc[:, "address"] = df_identify.apply(
        lambda row: create_address(row), axis=1
    )

    df_identify["traits"] = df_identify[traits_list].to_dict("records")
    df_identify = df_identify[["userId", "traits"]]
    df_identify["type"] = "identify"

    identify_blob = df_identify.to_dict("records")

    print("Number of identify : {}".format(len(identify_blob)))

    return identify_blob


def process_events(event_type, df, event_config):
    print("Event         : {}".format(event_type))

    print("Total events  : {}".format(len(df)))

    df = df.replace("\n", "", regex=True)
    df = df.drop_duplicates(["userId", "event", "timestamp"], keep="first")

    print("Unique events : {}".format(len(df)))

    event_properties = event_config["properties"]
    print("Properties : {}".format(event_properties))

    print(" ")
    print(df.columns)
    print(" ")

    df_track = df.copy(deep=True)
    df_track = df_track.fillna("")

    df_track.loc[:, "properties"] = df_track[event_properties].to_dict("records")

    if event_config.get("context", False) == True:
        print("Processing context", end="\n\n")

        df_track.loc[:, "context"] = df_track.apply(
            lambda row: create_context_test(row, context_config), axis=1
        )
        df_track.loc[(df_track["context"] == {}), "context"] = np.nan
        df_track = df_track[["userId", "event", "properties", "context", "timestamp"]]

    else:
        print("No requirement for context", end="\n\n")

        df_track = df_track[["userId", "event", "properties", "timestamp"]]

    df_track["type"] = "track"

    # track_blob = df_track.to_dict('records')
    event_blob = [v.dropna().to_dict() for k, v in df_track.iterrows()]

    print("Length of event blob: {}".format(len(event_blob)))

    return event_blob


def process_profiles(profiles_df):
    df_ = profiles_df.copy()

    df_ = df_[raw_traits_list]

    print("Total profiles        : {}".format(len(df_)))

    df_ = df_.drop_duplicates(["userId", "email"], keep="first")
    df_ = df_.reset_index(drop=True)

    print("Unique profiles       : {}".format(len(df_)))

    df_.loc[:, "address"] = df_.apply(lambda row: create_address(row), axis=1)

    df_["traits"] = df_[traits_list].to_dict("records")
    df_ = df_[["userId", "traits"]]
    df_["type"] = "identify"

    identify_blob = df_.to_dict("records")

    print("Number of profiles : {}".format(len(identify_blob)))

    return identify_blob


def process_events_b(df_, event_config):
    event_name = event_config["event"]

    print("Event name    : {}".format(event_name))
    print("Total events  : {}".format(len(df_)))

    df_ = df_.replace("\n", "", regex=True)
    df_ = df_.drop_duplicates(["userId", "event", "timestamp"], keep="first")

    print("Unique events : {}".format(len(df_)))

    event_properties = event_config["properties"]
    print("Properties : {}".format(event_properties))

    print(" ")
    print(df_.columns)
    print(" ")

    df_track = df_.copy(deep=True)
    df_track = df_track.fillna("")

    df_track.loc[:, "properties"] = df_track[event_properties].to_dict("records")

    if event_config.get("context", False) == True:
        print("Processing context", end="\n\n")

        df_track.loc[:, "context"] = df_track.apply(
            lambda row: create_context(row, context_config), axis=1
        )
        df_track = df_track[["userId", "event", "properties", "context", "timestamp"]]

    else:
        print("No requirement for context", end="\n\n")

        df_track = df_track[["userId", "event", "properties", "timestamp"]]

    df_track["type"] = "track"

    # track_blob = df_track.to_dict('records')
    event_blob = [v.dropna().to_dict() for k, v in df_track.iterrows()]

    print("Length of event blob: {}".format(len(event_blob)))

    return event_blob


def generate_identify_object(df):
    df_ = df.copy()
    df_ = df_[raw_traits_list]

    df_ = df_.drop_duplicates(["userId", "email"], keep="first")
    df_ = df_.reset_index(drop=True)

    for cols in df_.select_dtypes(include="datetime").columns:
        df_[cols] = pd.to_datetime(df_[cols])
        df_[cols] = df_[cols].apply(lambda x: x.isoformat())

    df_.loc[:, "address"] = df_.apply(lambda row: create_address(row), axis=1)

    df_["traits"] = df_[traits_list].to_dict("records")
    df_["type"] = "identify"
    df_ = df_[["userId", "traits", "type"]]

    identify_blob = df_.to_dict("records")

    print("Number of profiles : {}".format(len(identify_blob)))

    return identify_blob


def generate_track_object(event_config, df):
    event_name = event_config["event"]

    print("Event         : {}".format(event_name))

    df_ = df.copy()
    df_["event"] = event_name

    df_["timestamp"] = pd.Timestamp.now()
    df_["timestamp"] = pd.to_datetime(df_["timestamp"])
    df_["timestamp"] = df_["timestamp"].apply(lambda x: x.isoformat())

    event_properties_list = list(event_config["properties"])
    event_properties_dict = event_config["properties"]

    print("Properties : {}".format(event_properties_list))

    print(df_.columns)

    for colname, coltype in event_properties_dict.items():
        if coltype == "string":
            df_[colname] = df_[colname].astype("str")
        elif coltype == "integer":
            df_[colname] = df_[colname].fillna(0).astype("int")
        elif coltype == "bool":
            df_[colname] = df_[colname].astype("bool")
        elif (coltype == "timestamp") | (df_[colname].dtype == "<M8[ns]"):
            df_[colname] = pd.to_datetime(df_[colname])
            df_[colname] = df_[colname].apply(lambda x: x.isoformat())

    df_.loc[:, "properties"] = df_[event_properties_list].to_dict("records")
    df_["type"] = "track"

    df_ = df_[["userId", "event", "properties", "timestamp", "type"]]

    event_obj = [v.dropna().to_dict() for k, v in df_.iterrows()]

    print("Length of event object: {}".format(len(event_obj)))

    return event_obj


def _dremio_appendable_table(
    data,
    table_name,
    run_date,
    org="ops",
    partner="avant",
    env="prod",
    overwrite=False,
):
    dataset_name = f'{table_name}_{str(run_date).replace("-", "_")}'
    print(f"Creating table: {dataset_name}")

    t = AppendableTable(table=table_name, org=org, partner=partner, env=env)
    print(f"Deleting previous versions of the table: {t}")
    try:
        for x in t.list_datasets():
            t.clear_dataset(x)
    except KeyError as e:
        print(f"There is no previous data in the table: {e}")
    print(f"Uploading: {t}")
    try:
        res = t.upload_df(
            df=data,
            dataset_name=dataset_name,
            overwrite=overwrite,
            explicit_cast=True,
            timeout=600,
        )
        print(f"Upload result: {res}")
    except ValueError as e:
        print(
            f"ERROR UPLOADING DATAFRAME TO {t} - We could be facing an issue with the datalakeutils and datalaketools versions."
        )
        print(e)
    except Exception as e:
        print(f"ERROR UPLOADING DATAFRAME TO {t}")
        print(e)


def check_for_eom(date_today, num_days_prior):
    day_prior = date_today - datetime.timedelta(days=num_days_prior)
    month_end = day_prior + pd.offsets.MonthEnd(0)
    days_to_month_end = (month_end.date() - day_prior).days

    return days_to_month_end == 0


def generate_measurement_date(date_today):
    if date_today.weekday() == 6:
        if check_for_eom(date_today, 1):
            m_date = date_today - timedelta(days=1)
        else:
            m_date = date_today - timedelta(days=2)
    else:
        if date_today.weekday() == 0:
            if check_for_eom(date_today, 2):
                m_date = date_today - timedelta(days=2)
            else:
                m_date = date_today - timedelta(days=1)
        else:
            m_date = date_today - timedelta(days=1)

    return m_date


def merge_original_data_with_customer(original_data, event_desc):
    cust_ids = "("
    for i in list(original_data["customer_id"]):
        cust_ids = cust_ids + str(i) + ", "
    cust_ids = cust_ids[:-2] + ")"

    customer_df = dremio_query_prod(
        read_sql("./credit_card/emails/card_email_upload/customer_data.sql").format(
            customer_id=cust_ids, event_desc=event_desc
        )
    )
    original_data.drop(
        [
            "userId",
            "firstName",
            "lastName",
            "createdAt",
            "address_state",
            "email",
            "email_subscription_state",
            "event",
            "product_type",
            "eligible_date",
            "expiry_date",
            "timestamp",
            "first_name",
            "last_name",
            "customer_state",
        ],
        axis=1,
        errors="ignore",
        inplace=True,
    )
    segment_data = pd.merge(original_data, customer_df, how="left", on="customer_id")

    return segment_data


def _post_to_responsys(
    df, rundate, file_name="file.csv", responsys_path="upload/dev/hold/"
):
    ####. Responsys Keys. ####

    cred = trellis.keys("responsys.sftp")
    pkey = paramiko.RSAKey.from_private_key(io.StringIO(cred["private_key"]))
    sftp_hostname = cred["host"]
    sftp_username = cred["user"]

    rundate = pd.Timestamp.now(timezone("US/Central")).strftime("%Y%m%d_%H%M%S")

    #####. Establishing the Connection   ####

    with paramiko.SSHClient() as ssh:
        ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())

        ssh.connect(
            hostname=sftp_hostname,
            port=22,
            username=sftp_username,
            pkey=pkey,
            look_for_keys=False,
            disabled_algorithms={"pubkeys": ["rsa-sha2-256", "rsa-sha2-512"]},
        )

        with ssh.open_sftp() as ftp:
            # ###  Create a temp file
            _, tmp_file_path = tempfile.mkstemp(suffix=".csv")

            df.to_csv(tmp_file_path, index=False)

            ### Upload to responsys remote path
            full_file_name = f"{file_name}_{rundate}.csv"
            print(
                f"Uploading data to Responsys - path: {responsys_path} - file: {full_file_name}"
            )
            ftp.chdir(responsys_path)
            ftp.put(tmp_file_path, full_file_name)


def post_responsys_data(
    responsys_df,
    event_code,
    responsys_code,
    responsys_path,
    run_date,
    post_to_responsys_flag=True,
    post_to_dremio_flag=True,
    test_sample_flag=False,
):
    if post_to_dremio_flag and len(responsys_df) > 0:
        print(f"Posting {len(responsys_df)} records to Dremio")
        _dremio_appendable_table(
            data=responsys_df,
            table_name=f"{event_code}_responsys",
            run_date=run_date,
            overwrite=True,
        )
    if post_to_responsys_flag and len(responsys_df) > 0:
        print("POSTING TO RESPONSYS")
        if test_sample_flag:
            print("Only posting 100 records to Responsys")
            responsys_df = responsys_df.head(100)
        _post_to_responsys(
            responsys_df,
            run_date,
            file_name=responsys_code,
            responsys_path=responsys_path,
        )


def post_segment_data(
    segment_df,
    identify,
    track,
    event_code,
    segment_key,
    run_date,
    post_to_segment_flag=True,
    post_to_dremio_flag=True,
):
    if post_to_dremio_flag:
        print(f"Posting {len(segment_df)} records to Dremio")
        _dremio_appendable_table(
            data=segment_df,
            table_name=f"{event_code}_segment",
            run_date=run_date,
            overwrite=True,
        )

    if post_to_segment_flag:
        print(f"Posting {len(identify)} identify records to Dremio")
        post_to_segment(
            call="identify",
            payload=identify,
            endpoint="batch",
            envwritekey=segment_key,
        )
        print(f"Posting {len(track)} track records to Dremio")
        post_to_segment(
            call="track",
            payload=track,
            endpoint="batch",
            envwritekey=segment_key,
        )
