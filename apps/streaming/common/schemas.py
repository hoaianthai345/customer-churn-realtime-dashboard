from pyspark.sql.types import (
    DateType,
    DoubleType,
    IntegerType,
    StringType,
    StructField,
    StructType,
)

transaction_event_schema = StructType(
    [
        StructField("msno", StringType(), False),
        StructField("payment_method_id", IntegerType(), True),
        StructField("payment_plan_days", IntegerType(), True),
        StructField("plan_list_price", DoubleType(), True),
        StructField("actual_amount_paid", DoubleType(), True),
        StructField("is_auto_renew", IntegerType(), True),
        StructField("transaction_date", DateType(), True),
        StructField("membership_expire_date", DateType(), True),
        StructField("is_cancel", IntegerType(), True),
    ]
)

user_log_event_schema = StructType(
    [
        StructField("msno", StringType(), False),
        StructField("date", DateType(), True),
        StructField("num_25", IntegerType(), True),
        StructField("num_50", IntegerType(), True),
        StructField("num_75", IntegerType(), True),
        StructField("num_985", IntegerType(), True),
        StructField("num_100", IntegerType(), True),
        StructField("num_unq", IntegerType(), True),
        StructField("total_secs", DoubleType(), True),
    ]
)

member_schema = StructType(
    [
        StructField("msno", StringType(), False),
        StructField("city", IntegerType(), True),
        StructField("bd", IntegerType(), True),
        StructField("gender", StringType(), True),
        StructField("registered_via", IntegerType(), True),
        StructField("registration_init_time", DateType(), True),
    ]
)
