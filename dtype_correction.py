from google.cloud import bigquery

# Initialize a BigQuery client
client = bigquery.Client()

# Specify your table
table_id = "de-project-397922.trips_data_all.green_tripdata"

# Get the table
table = client.get_table(table_id)

# Get the schema
schema = table.schema

# print(schema)
# Modify the type of the 'passenger_count' column
for field in schema:
    if field.name == 'passenger_count':
        field.field_type = 'INT64'

# Update the table schema
table.schema = schema
table = client.update_table(table, ["schema"])

print("Updated table schema:")
print(table.schema)