from delta.tables import DeltaTable
from pyspark.sql.functions import col
from pyspark.sql.types import LongType, StructType, StructField, StringType
from pyspark.sql.utils import AnalysisException

class SecureCoreDataLoader:
    def __init__(self, spark, params):
        self.spark = spark
        self.params = params

    """create_schema - Function to create schema within a catalog. The storage location of the schema will be the storage root of the catalog/schema_name."""
    def create_schema(self, catalog, schema):
        try:
            self.spark.sql(f"DESCRIBE SCHEMA EXTENDED `{catalog}`.`{schema}`")
            schema_exists = True
        except AnalysisException:
            schema_exists = False

        df_catalog_props = self.spark.sql(f"DESCRIBE CATALOG EXTENDED `{catalog}`")
        catalog_storage_root = (
        df_catalog_props
        .filter(df_catalog_props["info_name"] == "Storage Root")
        .select("info_value")
        .collect()[0][0]
        )

        self.spark.sql(f"""CREATE SCHEMA IF NOT EXISTS `{catalog}`.`{schema}`
                                MANAGED LOCATION '{catalog_storage_root}/{schema}'""")

    """create_individual_attribute_table - Function to create table for each PCI / PII attribute that will contain the attribute value in clear text and its hash value"""    
    def create_individual_attribute_table(self, catalog, schema, table):
        attribute_name = table.replace('-', '_').lower()
        create_table_statement = f"""
                            CREATE TABLE IF NOT EXISTS 
                            `{catalog}`.`{schema}`.`{attribute_name}`
                            (
                              {attribute_name} STRING NOT NULL,
                              {attribute_name}_hash STRING NOT NULL
                            )
                            TBLPROPERTIES(
                            'delta.columnMapping.mode' = 'name',
                            'delta.enableIcebergCompatV2' = 'true',
                            'delta.universalFormat.enabledFormats' = 'iceberg');"""
        self.spark.sql(create_table_statement)

    """get_latest_write_version = Get the latest write version from a table since the last retrieved version"""
    def get_latest_write_version(self, catalog, schema, table, last_loaded_version):
        try:
            path = f"`{catalog}`.`{schema}`.`{table}`"
            delta_table = DeltaTable.forName(self.spark, path)
            df_history = (
                delta_table.history()
                .filter(f"version > {last_loaded_version} AND operation = 'WRITE'")
                .orderBy("version", ascending=False)
            )
            write_versions = df_history.select("version").collect()
            if write_versions:
                return write_versions[0]["version"]
            else:
                return None
        except Exception as e:
            print(f"Error while checking write operations: {e}")
            return None
    
    """get_tables_with_new_versions - Function to identify if there are new write versions in a table since last retrieved. The last retrieved version numbers are maintained in the tracking table. For all tables maintained in the tracking table, the latest write version is fetched."""
    def get_tables_with_new_versions(self, tracking_table):
        #df_last_loaded = self.get_last_loaded_versions(tracking_table)
        df_last_loaded = self.spark.read.table(
            f"`{self.params['secure_core_catalog_name']}`.default.`{tracking_table}`")
        table_rows = df_last_loaded.collect()
        results = []

        for row in table_rows:
            catalog, schema, table, last_loaded_version = row['catalog_name'], row['schema_name'], row['table_name'], row['last_loaded_version']
            latest_version = self.get_latest_write_version(catalog, schema, table, last_loaded_version)
            if latest_version is not None and latest_version > last_loaded_version:
                results.append((catalog, schema, table, last_loaded_version, latest_version))

        schema = StructType([
            StructField("catalog_name", StringType(), True),
            StructField("schema_name", StringType(), True),
            StructField("table_name", StringType(), True),
            StructField("last_loaded_version", LongType(), True),
            StructField("latest_version", LongType(), True)
        ])

        df = self.spark.createDataFrame(results, schema)
        df.createOrReplaceTempView("tables_with_new_versions")
        return df

    """get_distinct_tables_to_load - Filter the dataframe to identify the distinct tables to be processed downstream"""
    def get_distinct_tables_to_load(self, df_tables):
        df_tables_to_load = (df_tables
            .dropDuplicates(["catalog_name", "schema_name", "table_name", "target_table_status", "new_data_available"])
            .select("catalog_name", "schema_name", "table_name", "target_table_status", "new_data_available")
        )
        return df_tables_to_load
    
    """get_kek_dsk - Filter for distinct tag_value, kek_name and dsk_name"""
    def get_kek_dsk(self, df_tables):
        df_kek_dsk = (df_tables
            .dropDuplicates(["tag_value", "kek_name", "dsk_name"])
            .dropna(subset=["tag_value", "kek_name", "dsk_name"])
            .select("tag_value", "kek_name", "dsk_name")
        )
        return df_kek_dsk

    """get_column_tags - Fetch all column tags from all tables in a given catalog"""
    def get_column_tags(self, catalog):
        return self.spark.sql(f"""
            SELECT catalog_name, schema_name, table_name, column_name, tag_name, tag_value 
            FROM system.information_schema.column_tags
            WHERE catalog_name = '{catalog}'
        """)

    """get_cdf_enabled_status** - For any table created in the landing catalog, check if CDF is enabled. CDF will be used for incrementally loading new data to tables in vault catalog"""
    def get_cdf_enabled_status(self,catalog, schema, table):
        table_props_df = self.spark.sql(f"DESCRIBE TABLE EXTENDED `{catalog}`.`{schema}`.`{table}`")

        props_row = table_props_df \
            .filter(col("col_name") == "Table Properties") \
            .select("data_type") \
            .first()

        is_cdf_enabled = False
        if props_row:
            props_str = props_row["data_type"]
            # Look for the exact key-value
            is_cdf_enabled = "delta.enableChangeDataFeed=true" in props_str

        return is_cdf_enabled
    
    """enable_cdf - Enable CDF for tables in landing catalog and add an entry to the landing_tables_version_tracking_table"""
    def enable_cdf(self, catalog, schema, table, tracking_table):
        self.spark.sql(f"ALTER TABLE `{catalog}`.`{schema}`.`{table}` SET TBLPROPERTIES (delta.enableChangeDataFeed = true)")
      
        self.spark.sql(f"""
                INSERT INTO `{self.params["secure_core_catalog_name"]}`.default.`{tracking_table}` 
                SELECT '{catalog}', '{schema}', '{table}', (SELECT MAX(VERSION) FROM (DESCRIBE HISTORY `{catalog}`.`{schema}`.`{table}`)), current_timestamp()
                """)

    """collect_source_table_columns_datatype - This function is used for fetching all the column names and data types to generate the CREATE TABLE statment in the vault catalog"""
    def collect_source_table_columns_datatype(self, catalog, schema, table, df_tables):
        return (
            df_tables
            .filter((col("catalog_name") == catalog) & (col("schema_name") == schema) & (col("table_name") == table))
            .select("column_name", "full_data_type", "is_nullable")
            .orderBy("ordinal_position")
            .collect()
        )

    """collect_column_tags - collect the column tags to propogate to downstream tables"""
    def collect_column_tags(self, catalog, schema, table, df_col_tags):
        """Fetch the column tags to be propogated to vault tables."""
        return (
            df_col_tags
            .filter((col("catalog_name") == catalog) & (col("schema_name") == schema) & (col("table_name") == table))
            .select("column_name", "tag_name", "tag_value")
            .collect()
        )

    """collect_pci_pii_columns - Get the list of PCI/PII columns based on tags in the tables. This is used for generating the hash columns for the respective PCI/PII columns"""
    def collect_pci_pii_columns(self, catalog, schema, table, df_source_tables_with_pci_pii_columns):
        return (
            df_source_tables_with_pci_pii_columns
            .filter((col("catalog_name") == catalog) & (col("schema_name") == schema) & (col("table_name") == table))
            .dropna(subset=["tag_name", "tag_value", "kek_name", "dsk_name"])
            .select("column_name", "tag_name", "tag_value")
            .collect()
        )

    """generate_hash_statements** - Generate the hash statement for the PCI/PII attributes. The hash is generated using SHA512 algorithm by concatenating the de-crypted DSK and the PCI/PII attribute"""
    def generate_hash_statements(self, pci_pii_columns):
        """Generate hash column statements for PCI/PII columns."""
        return {
            col["column_name"]: (
                f"SHA2(CONCAT(CAST(AES_DECRYPT("
                f"(SELECT decrypted_dsk FROM decrypted_dsk_cte WHERE attribute = '{col['tag_value']}'), "
                f"'dskencryptionkey') AS STRING), {col['column_name']}), 512) "
                f"AS {col['column_name']}_hash"
            )
            for col in pci_pii_columns
        }

    """generate_create_table_statement** - Generate the CREATE TABLE statement for creating new tables in vault catalog and also enable CDF"""
    def generate_create_table_statement(self, catalog, schema, table, columns_datatype, hash_statements, target_table):
        full_table_name = target_table
        column_defs = []

        for col in columns_datatype:
            column_name = col["column_name"]
            data_type = col["full_data_type"]
            nullable = col['is_nullable'].strip().upper() != 'NO'
            column_def = f"{column_name} {data_type}{'' if nullable else ' NOT NULL'}"

            column_defs.append(column_def)
            
            # If this column has a hash version, add an _hash column
            if column_name in hash_statements:
                column_def = f"{column_name}_hash STRING NOT NULL"
                column_defs.append(column_def)

        column_def_sql = ",\n".join(column_defs)

        create_stmt = f"""CREATE TABLE IF NOT EXISTS {full_table_name} (
                            {column_def_sql}
                        )
                        USING DELTA
                        TBLPROPERTIES ('delta.enableChangeDataFeed' = 'true');
                        """

        return create_stmt
        #self.spark.sql(create_stmt)

    """generate_insert_statement - Generate the INSERT statement to insert data from tables in landing catalog to tables in vault catalog. If CDF is enabled, the SELECT clause with CDF table syntax (table_changes('table_name', version_no) WHERE _change_type = 'insert'). If CDF is not enabled, it will be SELECT * FROM table_name"""
    def generate_insert_statement(self, catalog, schema, table, target_catalog, columns, hash_statements, last_loaded_version, cdf_enabled, fetch_decrypted_dsk_sql=""):
        """Generate an insert statement ensuring hash columns appear next to source columns."""
        new_version = last_loaded_version + 1
        select_columns = []

        for col in columns:
            column_name = col["column_name"]
            select_columns.append(column_name)
            
            # If this column has a hash version, insert it immediately after
            if column_name in hash_statements:
                select_columns.append(hash_statements[column_name])

        select_columns_sql = ",\n".join(select_columns)
        
        source_clause = (
            f"table_changes('`{catalog}`.`{schema}`.`{table}`', {new_version}) WHERE _change_type = 'insert'"
            if cdf_enabled else
            f"`{catalog}`.`{schema}`.`{table}`"
        )

        if hash_statements:
            return f"""
            INSERT INTO `{target_catalog}`.`{schema}`.`{table}`
            WITH decrypted_dsk_cte AS (
            {fetch_decrypted_dsk_sql}
            )
            SELECT 
            {select_columns_sql}
            FROM {source_clause};
            """
        else:
            return f"""
            INSERT INTO `{target_catalog}`.`{schema}`.`{table}`
            SELECT
            {select_columns_sql}
            FROM {source_clause};
            """
    """add_entry_to_tracking_table - Function to add an entry in the tracking table. This will be called after running the CREATE TABLE statement. Hence, the last loaded version will be defaulted to 0"""
    def add_entry_to_tracking_table(self, catalog, schema, table, tracking_table):
        """Generate an INSERT statement to add entry to tracking table."""
        return f"""
        INSERT INTO `{self.params["secure_core_catalog_name"]}`.default.`{tracking_table}`  
        SELECT '{catalog}', '{schema}', '{table}', 0, current_timestamp();
        """

    """update_tracking_table_statement - Function to update the tracking tables. Update the last read version from the tables that were read. This is used to identify if there is a new version of data available for incremental data loads"""
    def update_tracking_table_statement(self, catalog, schema, table, tracking_table):
        """Generate an UPDATE statement to track the latest processed version."""
        return f"""
        UPDATE 
        `{self.params["secure_core_catalog_name"]}`.default.`{tracking_table}`  
        SET last_loaded_version = (
            SELECT MAX(version) FROM (DESCRIBE HISTORY `{catalog}`.`{schema}`.`{table}`)
        ), datetime = current_timestamp()
        WHERE catalog_name = '{catalog}' AND schema_name = '{schema}' AND table_name = '{table}';
        """

    """apply_masking_function_statement - Function to apply the masking function to the PCI/PII attributes in the tables in vault catalog"""
    def apply_masking_function_statement(self, schema, table, target_catalog, pci_pii_columns):
        """Generate ALTER statements to apply masking functions."""
        return [
            f"ALTER TABLE `{target_catalog}`.`{schema}`.`{table}` "
            f"ALTER COLUMN {col['column_name']} SET MASK `{self.params["secure_core_catalog_name"]}`.default.{col['tag_value'].replace('-', '_')}_mask;"
            for col in pci_pii_columns
        ]

    """propogate_tags_statement - Generate ALTER statement to propogate the tags from one table to another"""
    def propagate_tags_statement(self, schema, table, target_catalog, col_tags, hash_statements):
        propagate_tags_sql = []
        
        """Generate ALTER statements to propagate tags to the vault table."""
        for col in col_tags:
            column_name = col["column_name"]
            tag_name = col["tag_name"]
            tag_value = col["tag_value"]

            tag_sql = f"'{tag_name}' = '{tag_value}'"

            alter_sql = f"""ALTER TABLE `{target_catalog}`.`{schema}`.`{table}`
                         ALTER COLUMN {column_name} SET TAGS ({tag_sql});"""

            propagate_tags_sql.append(alter_sql)
            
            if column_name in hash_statements:
                alter_hash_column_sql = f"""
                ALTER TABLE `{target_catalog}`.`{schema}`.`{table}` 
                ALTER COLUMN {column_name}_hash SET TAGS ({tag_sql});
                """
                
                propagate_tags_sql.append(alter_hash_column_sql)

        return propagate_tags_sql