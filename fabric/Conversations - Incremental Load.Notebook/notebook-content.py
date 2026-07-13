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

# ### Parameters – Lakehouse name
# Sets the `lakehousename` variable used to build SQL queries against the Dataverse Lakehouse.

# PARAMETERS CELL ********************

lakehousename = "dataverse_contosojbend_cds2_workspace_unq1d69ab079f7ff011a7007c1e52172"

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ### Load to Curated & Advance Checkpoint
# Append the transformed records to the gold target table, then update the stored baseline version so the next run picks up only newer changes.

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
# Reads the Delta Change Data Feed from `{lakehousename}.conversationtranscript` starting at `start_version` (one above the stored baseline). The CDF reader returns all change records along with metadata columns (`_change_type`, `_commit_version`, `_commit_timestamp`).
# 
# If no change records are found (count == 0), the notebook exits early via `dbutils.notebook.exit()` to avoid unnecessary processing downstream.

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

# ### Filter for insert records and select columns
# Filters `cdf_df` to retain only rows where `_change_type == "insert"` — updates and deletes are not expected in this pipeline. Projects the key metadata columns (`id`, `conversation_starttime`, `conversation_startdate`, `bot_conversationtranscriptidname`, `bot_conversationtranscriptId`, `content`) and wraps the `content` string column in a single-element `array()` so the downstream JSON parsing cell can uniformly index it as `content[0]`, matching the initial load structure.

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

# ### Parse JSON `content` column and flatten top-level fields
# Samples a non-null JSON value from the `content` column, infers its schema, parses it into a struct (`content_json`), and flattens its top-level fields into `parsed_df` while keeping key transcript metadata (`id`, `conversation_starttime`, `conversation_startdate`, `bot_conversationtranscriptidname`, `bot_conversationtranscriptId`).
# 
# If no non-null JSON values are found in `content`, the code skips JSON parsing and simply returns a `parsed_df` that contains only the basic conversation metadata columns.

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
# Automatically detects array-type columns in `parsed_df`, chooses the **first** array column as the conversation-parts container, explodes it to create one row per conversation part, filters to only `type == "message"`, and stores the result as `conversation_df` with a `conversation_part_json` struct column.
# 
# If no array-type columns are present in `parsed_df`, the code leaves the data unmodified and assigns `conversation_df = parsed_df`.

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
# Projects key fields out of the `conversation_part_json` struct (channel, text, sender details, timestamp), converts the epoch timestamp to a proper Spark `timestamp`, orders messages by newest first, and produces `conversation_df_with_fields` while keeping the original `conversation_part_json` struct for further exploration if needed.

# CELL ********************

from pyspark.sql.functions import col, from_unixtime

# conversation_part_json is a STRUCT, so we access its fields directly instead of using get_json_object
conversation_df_with_fields = (
    conversation_df
    .select(
        "id",
        "conversation_starttime",
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
    # Convert epoch seconds to timestamp (if populated)
    .withColumn(
        "conversation_part_starttime",
        from_unixtime(col("conversation_part_timestamp")).cast("timestamp")
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

# ### Load system user data
# Queries the Dataverse `systemuser` table in the same Lakehouse to retrieve user identity information (AAD object ID, full name, email) into `user_df`. Only rows with a non-null `azureactivedirectoryobjectid` are loaded so that they can be joined reliably with conversation messages later.

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
# Renames key user columns in `user_df`, left-joins `conversation_df_with_fields` with user data using the AAD object ID (`from_aadObjectId` ⇔ `userentraid`), and constructs a readable `from` field:
# - Bot messages (`from_role == 0`) show the bot name (`bot_conversationtranscriptidname`)
# - User messages (`from_role == 1`) show the user full name from Dataverse (`userfullname`).
# 
# The code also adds a `from_icon` column (🤖 for bot, 👤 for user) and stores the result in `conversation_with_user`.

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

# ### Persist enriched conversation data
# Writes the `conversation_with_user` DataFrame as a managed Delta table named `copilotconversation` in the default Lakehouse (`CopilotObservability`), overwriting any existing table with that name and allowing schema evolution via `overwriteSchema = true`. After writing, it issues a `REFRESH TABLE dbo.copilotconversation` to make sure downstream queries see the latest metadata and data.

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
# Captures the current (latest) version of the source table via `DESCRIBE HISTORY` and stores it as the `cdf.baseline_version` table property on the target table. The next incremental run will read CDF starting at this version + 1, ensuring no changes are double-processed or missed.

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
