from datetime import timedelta as Duration

from pyflink.datastream import StreamExecutionEnvironment
from pyflink.table import (
    DataTypes,
    EnvironmentSettings,
    StreamTableEnvironment,
    TableEnvironment,
)
from pyflink.table.expressions import col, lit
from pyflink.table.window import Session


def create_counts_aggregated_sink(t_env):
    table_name = "taxi_events_counts_aggregated"
    sink_ddl = f"""
        CREATE TABLE {table_name} (
            PULocationID INTEGER,
            DOLocationID INTEGER,
            window_start TIMESTAMP(3),
            window_end TIMESTAMP(3),
            total_trips BIGINT
        ) WITH (
            'connector' = 'jdbc',
            'url' = 'jdbc:postgresql://postgres:5432/postgres',
            'table-name' = '{table_name}',
            'username' = 'postgres',
            'password' = 'postgres',
            'driver' = 'org.postgresql.Driver'
        );
        """
    t_env.execute_sql(sink_ddl)
    return table_name


def create_events_aggregated_sink(t_env):
    table_name = "taxi_events_aggregated"

    sink_ddl = f"""
        CREATE TABLE {table_name} (
            PULocationID INTEGER,
            DOLocationID INTEGER,
            window_start TIMESTAMP(3),
            longest_streak BIGINT,
            PRIMARY KEY (PULocationID, DOLocationID) NOT ENFORCED
        ) WITH (
            'connector' = 'jdbc',
            'url' = 'jdbc:postgresql://postgres:5432/postgres',
            'table-name' = '{table_name}',
            'username' = 'postgres',
            'password' = 'postgres',
            'driver' = 'org.postgresql.Driver'
        );
        """
    t_env.execute_sql(sink_ddl)
    return table_name


def create_taxi_events_sink_postgres(t_env):
    table_name = "taxi_events"
    # drop the table if it exists
    sink_ddl = f"""
        CREATE OR REPLACE TABLE {table_name} (
            lpep_pickup_datetime VARCHAR,
            lpep_dropoff_datetime VARCHAR,
            pickup_date DATE,
            PULocationID INTEGER,
            DOLocationID INTEGER,
            passenger_count INTEGER,
            trip_distance DOUBLE,
            tip_amount DOUBLE
        ) WITH (
            'connector' = 'jdbc',
            'url' = 'jdbc:postgresql://postgres:5432/postgres',
            'table-name' = '{table_name}',
            'username' = 'postgres',
            'password' = 'postgres',
            'driver' = 'org.postgresql.Driver'
        );
        """
    t_env.execute_sql(sink_ddl)
    return table_name


def create_events_source_kafka(t_env):
    table_name = "green_trips"
    pattern = "yyyy-MM-dd HH:mm:ss"
    source_ddl = f"""
        CREATE TABLE {table_name} (
            lpep_pickup_datetime VARCHAR,
            lpep_dropoff_datetime VARCHAR,
            pickup_date as TO_DATE(lpep_pickup_datetime, '{pattern}'),
            PULocationID INTEGER,
            DOLocationID INTEGER,
            passenger_count INTEGER,
            trip_distance DOUBLE,
            tip_amount DOUBLE,
            dropoff_timestamp AS TO_TIMESTAMP(lpep_dropoff_datetime),
            WATERMARK FOR dropoff_timestamp AS dropoff_timestamp - INTERVAL '5' SECONDS
        ) WITH (
            'connector' = 'kafka',
            'properties.bootstrap.servers' = 'redpanda-1:29092',
            'topic' = 'green_trips',
            'scan.startup.mode' = 'earliest-offset',
            'properties.auto.offset.reset' = 'earliest',
            'format' = 'json'
        );
        """
    t_env.execute_sql(source_ddl)
    return table_name


def log_processing():
    # Set up the execution environment
    env = StreamExecutionEnvironment.get_execution_environment()
    env.enable_checkpointing(10 * 1000)
    env.set_parallelism(1)

    # Set up the table environment
    settings = EnvironmentSettings.new_instance().in_streaming_mode().build()
    t_env = StreamTableEnvironment.create(env, environment_settings=settings)
    try:
        # Create Kafka table
        source_table = create_events_source_kafka(t_env)

        counts_aggregated_table = create_counts_aggregated_sink(t_env)

        windowed_rev = t_env.sql_query(
            f"""
            SELECT
                PULocationID,
                DOLocationID,
                SESSION_START(dropoff_timestamp, INTERVAL '5' MINUTE) AS window_start,
                SESSION_END(dropoff_timestamp, INTERVAL '5' MINUTE) AS window_end,
                COUNT(*) AS total_trips
            FROM {source_table}
            GROUP BY PULocationID, DOLocationID, SESSION(dropoff_timestamp, INTERVAL '5' MINUTE)
            """
        )

        windowed_rev.execute_insert(counts_aggregated_table).wait()
        print("Writing records from Kafka to JDBC succeeded.")

    except Exception as e:
        print("Writing records from Kafka to JDBC failed:", str(e))


if __name__ == "__main__":
    log_processing()
