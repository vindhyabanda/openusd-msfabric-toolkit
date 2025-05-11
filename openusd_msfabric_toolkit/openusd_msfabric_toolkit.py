# Fixed imports
from pxr import Usd, Sdf
import logging
import os
from pyspark.sql import SparkSession
import pyspark.sql.functions as F
from pyspark.sql.functions import col, udf, lit
from pyspark.sql.types import StringType
from thefuzz import fuzz

class OpenUSDToolkit:
    """
    Toolkit for working with OpenUSD files in Microsoft Fabric
    """

    @staticmethod
    def flatten_value(value):
        """Helper function to flatten complex types like dictionaries and lists"""
        if isinstance(value, dict):
            return ', '.join(f"{k}: {v}" for k, v in value.items())
        elif isinstance(value, list):
            return ', '.join(map(str, value))
        else:
            return str(value)
        
    @staticmethod
    def read_usd_metadata(spark, usd_file):
        """Takes in a USD file and returns a DataFrame with metadata"""
        # Open the USD stage
        stage = Usd.Stage.Open(usd_file, Usd.Stage.LoadAll)

        # Extract root layer metadata
        root_layer = stage.GetRootLayer()
        layer_metadata = {
            "identifier": root_layer.identifier,
            "version": root_layer.version,
            "subLayers": root_layer.subLayerPaths,
        }

        # Extract stage-level metadata
        stage_metadata = {
            "start_time_code": stage.GetStartTimeCode(),
            "end_time_code": stage.GetEndTimeCode(),
            "frames_per_second": stage.GetFramesPerSecond(),
            "time_codes_per_second": stage.GetTimeCodesPerSecond(),
        }

        # Extract metadata for prims where type is "Xform"
        prim_metadata = {}
        for prim in stage.Traverse():
            if prim.GetTypeName() == "Xform":  # Only include "Xform" types
                prim_metadata[prim.GetPath().pathString] = {"type": "Xform"}

        # Prepare the data to be flattened
        flattened_data = []
        for asset_path, metadata_dict in prim_metadata.items():
            flattened_data.append(("prim_metadata", asset_path, OpenUSDToolkit.flatten_value(metadata_dict)))

        # Convert the flattened data into a DataFrame
        df = spark.createDataFrame(flattened_data, ['metadata_type', 'metadata_key', 'metadata_value'])

        # Drop unnecessary columns
        df = df.drop("metadata_type", "metadata_value")

        # Transform the DataFrame
        df_new = df.withColumn("USDAssetID", F.split(F.col("metadata_key"), "/").getItem(F.size(F.split(F.col("metadata_key"), "/")) - 1)) \
                .withColumnRenamed("metadata_key", "sourcePath")

        # Write the DataFrame as a Delta table
        table_name = "ExtractedUSDMetadata"
        df_new.write.mode("overwrite").format("delta").saveAsTable(table_name)

        # Return the transformed DataFrame
        return df_new

    @staticmethod
    def read_entity_instances(spark, dtbname, entityname):
        """Read entity instances from a specified DTB an entity name
        Parameters:
        - spark: SparkSession
        - dtbname: Name of DTB Item to query frpom
        - entityname: Name of Entity to retrieve list.  
        """
        try:
            # Query the entity instances from the DTB
            query = f"""
            SELECT ei.EntityInstanceDisplayId
            FROM {dtbname}dtdm.entitytype et
            JOIN {dtbname}dtdm.entityinstance ei
            ON et.ID = ei.EntityTypeId
            WHERE et.Name = '{entityname}'
            """
            df = spark.sql(query)

            # Create a temporary view for SQL queries
            df.createOrReplaceTempView(entityname)

            # Return the DataFrame
            return df

        except Exception as e:
            if "Table or view not found" in str(e):
                print(f"Error: The DTB '{dtbname}' was not found or is not connected.")
                print("Please connect the DTB data source to this notebook before proceeding.")
            else:
                print(f"An error occurred: {str(e)}")
            return None
        
    @staticmethod
    def fuzzy_match_usd_assets(spark, df_usd_metadata, df_asset_data, asset_id_col="EntityInstanceDisplayId", threshold=80):
        """
        Takes in two data frames and runs a fuzzy match on them.
        Allows dynamic specification of asset ID column names.
        
        Parameters:
        - spark: SparkSession
        - df_usd_metadata: DataFrame with USD metadata
        - df_asset_data: DataFrame with asset information
        - usd_id_col: column in df_usd_metadata to match from (default "USDAssetID")
        - asset_id_col: column in df_asset_data to match to (default "AssetID")
        - threshold: minimum fuzzy match score to consider a valid match
        """
        # This is hardcoded by the read_usd_metadata function
        usd_id_col="USDAssetID"

        # Collect the asset IDs from df_asset_data to the driver
        asset_list = df_asset_data.select(asset_id_col).rdd.flatMap(lambda x: x).collect()
        
        # Broadcast the asset list
        broadcast_assets = spark.sparkContext.broadcast(asset_list)
        
        # Define the UDF to find best fuzzy match
        def get_best_match(usd_asset_id):
            best_match = None
            best_score = 0
            for candidate in broadcast_assets.value:
                score = fuzz.token_sort_ratio(usd_asset_id, candidate)
                if score > best_score:
                    best_score = score
                    best_match = candidate
            return best_match if best_score >= threshold else None

        fuzzy_udf = udf(get_best_match, StringType())

        # Apply the UDF to create the match
        matched_df = df_usd_metadata.withColumn("DTBAssetID", fuzzy_udf(col(usd_id_col)))

        # Get successful matches
        result_df = matched_df.filter(col("DTBAssetID").isNotNull()) \
                            .select(col(usd_id_col).alias("USDAssetID"), "DTBAssetID")

        # Get non-matches
        unmatched_df = matched_df.filter(col("DTBAssetID").isNull()) \
                                .select(usd_id_col)
        
        # Collect and print unmatched USDAssetIDs
        unmatched_list = unmatched_df.rdd.flatMap(lambda x: x).collect()
        if unmatched_list:
            print("⚠️ Unmatched USDAssetIDs:")
            for asset in unmatched_list:
                print(f" - {asset}")
        else:
            print("✅ All USDAssetIDs successfully matched.")

        return result_df


    @staticmethod
    def exact_match_usd_assets(df_usd_metadata, df_asset_data):
        """Perform exact matching on USD assets based on contextualization results"""
        # This function needs implementation
        # Return placeholder for now
        return None
        
    @staticmethod
    def process_contextualization_job_results(dtdm_name, lakehouse_name):
        """Process contextualization data from a specified DTDM and save results to a specified Lakehouse"""
        spark = SparkSession.builder.getOrCreate()
        
        try:
            # Query entity and relationship instances from the provided DTDM
            entityInstanceDf = spark.sql(f"SELECT * FROM {dtdm_name}.entityinstance")
            relationshipsDf = spark.sql(f"SELECT * FROM {dtdm_name}.relationshipinstance")
            
            # Create temporary views for SQL query
            relationshipsDf.createOrReplaceTempView("relationshipinstance")
            entityInstanceDf.createOrReplaceTempView("entityinstance")
            
            # Execute query to map USD entities to asset entities
            query = """
            SELECT 
                e1.EntityInstanceDisplayId AS USDDisplayID,
                e2.EntityInstanceDisplayId AS AssetDisplayID
            FROM relationshipinstance r
            LEFT JOIN entityinstance e1 
                ON r.FirstEntityInstanceId1 = e1.Id1 AND r.FirstEntityInstanceId2 = e1.Id2
            LEFT JOIN entityinstance e2
                ON r.SecondEntityInstanceId1 = e2.Id1 AND r.SecondEntityInstanceId2 = e2.Id2
            """
            
            # Execute the query and save results
            resultDf = spark.sql(query)
            
            # Save results to the specified Lakehouse
            result_table = f"{lakehouse_name}.ContextualizationResults"
            resultDf.write.mode("overwrite").saveAsTable(result_table)
            
            # Return the saved results
            return spark.sql(f"SELECT * FROM {result_table}")
            
        except Exception as e:
            if "Table or view not found" in str(e):
                print(f"Error: The DTDM '{dtdm_name}' was not found or is not connected.")
                print("Please connect the DTDM data source to this notebook before proceeding.")
            else:
                print(f"An error occurred: {str(e)}")
            return None

    @staticmethod
    def enrich_usd_with_dtb_assets(usd_file_path, output_usd_file_path, df_usd_metadata, result_df):
        """Enrich USD file with DTB_ID attributes based on fuzzy-matched results"""
        # Join metadata and fuzzy match results
        df_matched = df_usd_metadata.join(
            result_df,
            on="USDAssetID",
            how="inner"
        ).select("sourcePath", "USDAssetID", "DTBAssetID")

        # Convert to Pandas for iteration
        matched_assets = df_matched.toPandas()

        # Load the USD stage
        stage = Usd.Stage.Open(usd_file_path)

        count = 0
        for _, row in matched_assets.iterrows():
            prim_path = row["sourcePath"]
            dtb_asset_id = row["DTBAssetID"]

            prim = stage.GetPrimAtPath(prim_path)
            if prim and prim.IsValid():
                attr = prim.CreateAttribute("DTB_ID", Sdf.ValueTypeNames.String)
                attr.Set(dtb_asset_id)
                count += 1
            else:
                logging.warning(f"Invalid or missing prim at path: {prim_path}")

        # Save modified USD file
        stage.GetRootLayer().Export(output_usd_file_path)
        print(f"✅ Enriched USD file saved to: {output_usd_file_path}")
        print(f"🔧 Total prims enriched with DTB_ID: {count}")

    @staticmethod
    def print_usd_file_details(usd_file_path, onlyDTBID=False):
        """Print detailed information about a USD file.
        
        Parameters:
        - usd_file_path: Path to the USD file.
        - onlyDTBID: If True, only print prims that have the 'DTB_ID' attribute.
        """
        # Open the USD file
        stage = Usd.Stage.Open(usd_file_path)

        if not stage:
            print(f"❌ Failed to open USD file at: {usd_file_path}")
            return

        print(f"\n📂 Inspecting USD File: {usd_file_path}")
        print("-" * 100)

        # Filter prims of type 'Xform'
        xform_prims = [prim for prim in stage.Traverse() if prim.GetTypeName() == "Xform"]

        if not xform_prims:
            print("⚠️ No 'Xform' prims found in the file.")
            return

        for prim in xform_prims:
            attributes = prim.GetAttributes()
            has_dtb_id = any(attr.GetName() == "DTB_ID" for attr in attributes)

            # If the onlyDTBID flag is set, skip prims without the DTB_ID attribute
            if onlyDTBID and not has_dtb_id:
                continue

            print(f"🔹 Prim Path: {prim.GetPath()}")

            # Metadata
            metadata = prim.GetAllMetadata()
            if metadata:
                print("📌 Metadata:")
                for key, value in metadata.items():
                    print(f"  - {key}: {value}")
            else:
                print("📌 Metadata: None")

            # Attributes
            if attributes:
                print("🔧 Attributes:")
                for attr in attributes:
                    print(f"  - {attr.GetName()} = {attr.Get()}")
            else:
                print("🔧 Attributes: None")

            # Relationships
            relationships = prim.GetRelationships()
            if relationships:
                print("🔗 Relationships:")
                for rel in relationships:
                    print(f"  - {rel.GetName()} -> {rel.GetTargets()}")
            else:
                print("🔗 Relationships: None")

            print("-" * 50)  # Separator for readability
