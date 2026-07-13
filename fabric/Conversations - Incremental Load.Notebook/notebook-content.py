# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {
# META     "lakehouse": {
# META       "default_lakehouse": "ca4fe890-3553-443c-8f8b-9b85448a6c1f",
# META       "default_lakehouse_name": "CopilotObservability",
# META       "default_lakehouse_workspace_id": "f9f403c2-b93a-416d-a8b1-681c219af03a",
# META       "known_lakehouses": [
# META         {
# META           "id": "ca4fe890-3553-443c-8f8b-9b85448a6c1f"
# META         }
# META       ]
# META     }
# META   }
# META }

# MARKDOWN ********************

# # Conversations Notebook - Incremental Load

# MARKDOWN ********************

# ### Parameter – Dataverse Lakehouse name
# Defines the `lakehousename` string used to build SQL queries against the Dataverse Lakehouse (e.g. `conversationtranscript`, `systemuser`).
# Set the time zone adjustment for times in your timezone

# PARAMETERS CELL ********************

lakehousename = "dataverse_contosojbend_cds2_workspace_unq1d69ab079f7ff011a7007c1e52172"
timezone_adjustment = "-4"

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ### Initialize CDF baseline and compute start version
# Reads the `cdf.baseline_version` table property from the curated `copilotconversation` table, validates that an initial baseline exists, and derives `start_version` (baseline + 1) that will be used to read new changes from Dataverse CDF.

# CELL ********************

# Retrieve the baseline version stored by the initial load notebook
baseline_row = spark.sql(f"SHOW TBLPROPERTIES copilotconversation ('cdf.baseline_version')").collect()

if not baseline_row or baseline_row[0]["value"].startswith("Table"):
    raise Exception(
        f"No 'cdf.baseline_version' property found on copilotconversation. "
        "Run the Initial Ingestion notebook first to establish the baseline."
    )

baseline_version = int(baseline_row[0]["value"])
start_version = baseline_version + 1
print(f"Reading CDF from '{lakehousename}'.conversationtranscript starting at version {start_version}")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ### Read Change Data Feed from source
# Uses Delta Change Data Feed to read new changes from `{lakehousename}.conversationtranscript` starting at `start_version`, based on the current table history, and exits the notebook early via `notebookutils.notebook.exit()` when there are no new change records to process.

# CELL ********************

from pyspark.sql.functions import col, array
from delta.tables import DeltaTable

table_name = f"{lakehousename}.conversationtranscript"

# Get the current latest version of the table
delta_table = DeltaTable.forName(spark, table_name)
latest_version = (
    delta_table.history(1)
    .select("version")
    .collect()[0][0]
)

print(f"Latest table version: {latest_version}, requested start_version: {start_version}")

# If start_version is beyond the latest committed version, there's nothing new
if start_version > latest_version:
    notebookutils.notebook.exit("No new changes to process.")

# Safe to read change data feed now
cdf_df = (
    spark.read.format("delta")
    .option("readChangeFeed", "true")
    .option("startingVersion", start_version)
    .table(table_name)
)

change_count = cdf_df.count()
print(f"Found {change_count} change records to process starting from version {start_version}")

if change_count == 0:
    notebookutils.notebook.exit("No new changes to process.")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ### Filter for insert records and select core transcript columns
# Filters `cdf_df` to retain only rows where `_change_type == "insert"` (this pipeline ignores updates/deletes), and projects the main transcript metadata (`id`, conversation start time/date, bot name/ID) plus the raw JSON `content` column into `upsert_df` for downstream JSON parsing.

# CELL ********************

from pyspark.sql.functions import col

# Filter for insert records only — no updates or deletes expected
upsert_df = cdf_df.filter(
    col("_change_type") == "insert"
).select(
    "id",
    col("conversationstarttime").alias("conversation_starttime"),
    col("conversationstarttime").cast("date").alias("conversation_startdate"),
    "bot_conversationtranscriptidname",
    "bot_conversationtranscriptId",
    "content"  # keep as-is; can be parsed into a STRUCT in the JSON parsing step
)

print(f"New inserts: {upsert_df.count()}")
display(upsert_df)


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ### Parse JSON `content` and flatten top-level fields
# Samples a non-null JSON string from `content` to infer its schema, parses `content` into a struct (`content_json`), and flattens its top-level fields into `parsed_df` alongside conversation metadata. If no non-null JSON values exist, it falls back to a minimal `parsed_df` containing only the basic metadata columns.

# CELL ********************

from pyspark.sql.functions import col, from_json, schema_of_json

# 1. Sample one non-null JSON string from the `content` column to infer schema
sample_row = (
    upsert_df
    .where(col("content").isNotNull())
    .select("content")
    .limit(1)
    .collect()
)

if not sample_row:
    # No JSON to parse – create an empty DataFrame with same non-JSON columns
    parsed_df = upsert_df.select("conversationstarttime", "bot_conversationtranscriptidname")
else:
    sample_json = sample_row[0]["content"]

    # 2. Infer schema from the sample JSON
    json_schema = schema_of_json(sample_json)

    # 3. Parse JSON into a struct column
    ct_with_json = upsert_df.withColumn("content_json", from_json(col("content"), json_schema))

    # 4. Flatten JSON fields into top-level columns, keeping original fields as needed
    parsed_df = ct_with_json.select(
        "id",
        "conversation_starttime",
        "conversation_startdate",
        "bot_conversationtranscriptidname",
        "bot_conversationtranscriptId",
        # expand all JSON fields as individual columns
        "content_json.*"
    )

# 5. Display the resulting DataFrame with individual JSON fields as columns
display(parsed_df)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ### Detect and explode array field for conversation parts
# Inspects `parsed_df` to find array-type columns, chooses the first such column as the conversation-parts container (e.g., activities/messages), explodes it into one row per conversation part, filters to `type == "message"`, and stores the nested struct as `conversation_part_json`. If no array-type columns are present, it simply passes `parsed_df` through unchanged as `conversation_df`.

# CELL ********************

from pyspark.sql.types import ArrayType
from pyspark.sql.functions import col, explode_outer

# This cell assumes `parsed_df` was created in the previous cell and contains:
# - top-level metadata columns (e.g., id, conversationstarttime, bot_conversationtranscriptidname)
# - flattened JSON columns from `content_json.*` (some of which may still be nested arrays/structs)

# 1. Detect array-type columns that likely hold the per-turn conversation data
array_columns = [
    field.name
    for field in parsed_df.schema.fields
    if isinstance(field.dataType, ArrayType)
]

# If there are no array columns, just show parsed_df as-is
if not array_columns:
    # No nested conversation array detected; nothing to explode
    conversation_df = parsed_df
else:
    # 2. Choose the array column to explode as "conversation parts"
    # If your JSON schema has a specific array column name (e.g. "activities" or "messages"),
    # you can set it explicitly here instead of auto-picking the first one.
    conversation_array_col = array_columns[0]

    # 3. Explode the array so each nested element (conversation turn/part)
    # becomes its own row, while preserving `id` and other metadata
    exploded_df = (
        parsed_df
        .withColumn("conversation_part", explode_outer(col(conversation_array_col)))
    )

    # 4. Keep only those nested elements where conversation_part.type == "message"
    filtered_df = exploded_df.where(col("conversation_part.type") == "message")

    # 5. Keep the nested element as a StructType column instead of converting it to JSON string.
    #    This ensures `conversation_part_json` is NOT a string, but a StructType, and if you later
    #    group/collect it, it can become an ArrayType(StructType()).
    conversation_df = filtered_df.select(
        "id",
        "conversation_starttime",
        "conversation_startdate",
        "bot_conversationtranscriptidname",
        "bot_conversationtranscriptId",
        col("conversation_part").alias("conversation_part_json")
    )

display(conversation_df)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ### Extract message-level fields from `conversation_part_json`
# Projects key fields from the `conversation_part_json` struct (channel, text, sender AAD object ID, sender role, timestamp), converts the epoch timestamp into a proper Spark `timestamp`, and produces `conversation_df_with_fields` ordered by most recent message first.

# CELL ********************

from pyspark.sql.functions import col, from_unixtime, expr

# conversation_part_json is a STRUCT, so we access its fields directly instead of using get_json_object
conversation_df_with_fields = (
    conversation_df
    .select(
        "id",
        # Adjust conversation_starttime by timezone offset
        expr(f"conversation_starttime + INTERVAL {timezone_adjustment} HOURS").alias("conversation_starttime"),
        "conversation_startdate",
        "bot_conversationtranscriptidname",
        "bot_conversationtranscriptId",
        # top-level fields on the struct
        col("conversation_part_json.channelId").alias("channelId"),
        col("conversation_part_json.text").alias("text"),
        col("conversation_part_json.timestamp").cast("double").alias("conversation_part_timestamp"),
        # nested 'from' struct fields
        col("conversation_part_json.from.aadObjectId").alias("from_aadObjectId"),
        col("conversation_part_json.from.id").alias("from_id"),
        col("conversation_part_json.from.role").alias("from_role"),
    )
    # Convert epoch seconds to timestamp and adjust by timezone offset
    .withColumn(
        "conversation_part_starttime",
        from_unixtime(col("conversation_part_timestamp")).cast("timestamp") + expr(f"INTERVAL {timezone_adjustment} HOURS")
    )
)

# Order the results by the millisecond-based timestamp (most recent first)
conversation_df_with_fields = conversation_df_with_fields.orderBy(col("conversation_part_starttime").desc())

display(conversation_df_with_fields)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ### Load system user data from Dataverse
# Queries the Dataverse `systemuser` table in the same Lakehouse to retrieve user identity attributes (AAD object ID, full name, email) into `user_df`, filtering to rows with a non-null `azureactivedirectoryobjectid` so they can be reliably joined to conversation messages.

# CELL ********************

userquery = f"SELECT id, azureactivedirectoryobjectid, fullname, internalemailaddress  FROM {lakehousename}.systemuser where azureactivedirectoryobjectid IS NOT NULL"
user_df = spark.sql(userquery)
display(user_df)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ### Join conversations with user details and derive friendly sender name
# Renames key user columns in `user_df`, left-joins `conversation_df_with_fields` to user data on AAD object ID, and builds readable sender columns: `from` (bot name or user full name) and `from_icon` (🤖 for bot, 👤 for user), resulting in the enriched `conversation_with_user` DataFrame.

# CELL ********************

from pyspark.sql.functions import col, when, lit

# Rename columns in user_df
user_df_renamed = (
    user_df
    .select(
        col("id").alias("userid"),
        col("azureactivedirectoryobjectid").alias("userentraid"),
        col("fullname").alias("userfullname"),
        col("internalemailaddress").alias("useremailaddress")
    )
)

# Left join conversation_df_simplified to user_df_renamed
conversation_with_user = (
    conversation_df_with_fields.alias("c")
    .join(
        user_df_renamed.alias("u"),
        col("c.from_aadObjectId") == col("u.userentraid"),
        how="left"
    )
    .withColumn(
        "from",
        when(col("c.from_role") == 0, col("c.bot_conversationtranscriptidname"))
        .when(col("c.from_role") == 1, col("u.userfullname"))
    )
    .withColumn(
        "from_icon",
        when(col("c.from_role") == 0, lit("🤖"))
        .when(col("c.from_role") == 1, lit("👤"))
        .otherwise(lit(""))
    )
)

display(conversation_with_user)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ### Persist enriched conversation data to curated table
# Appends the `conversation_with_user` DataFrame to the managed Delta table `copilotconversation` in the default Lakehouse (`CopilotObservability`) and refreshes `dbo.copilotconversation` so downstream queries see the latest schema and data.

# CELL ********************

# Write the conversation_with_user DataFrame to the default Lakehouse,
# allowing the table schema to be updated (e.g., new columns like conversation_startdate)
conversation_with_user.write.mode("append").saveAsTable("copilotconversation")

spark.sql(f"REFRESH TABLE dbo.copilotconversation")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ### Advance CDF baseline checkpoint
# Finds the latest version of `{lakehousename}.conversationtranscript` via `DESCRIBE HISTORY` and updates the `copilotconversation` table property `cdf.baseline_version` to that version so the next incremental run starts from the correct point and does not reprocess prior changes.

# CELL ********************

# Update the stored baseline version to the current latest version of the source table
latest_version = (
    spark.sql(f"DESCRIBE HISTORY {lakehousename}.conversationtranscript LIMIT 1")
    .select("version")
    .collect()[0]["version"]
)

spark.sql(f"""
    ALTER TABLE copilotconversation
    SET TBLPROPERTIES ('cdf.baseline_version' = '{latest_version}')
""")

print(f"Updated cdf.baseline_version to {latest_version}")
print(f"Next run will start at version {latest_version + 1}")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
