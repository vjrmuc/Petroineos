import pandas as pd
import logging

class Validation(object):
    def __init__(self):
        pass

    def read_schema(self, file_name="schemas/schema.csv"):
        schema_df = pd.read_csv(file_name)
        return schema_df

    def validate_data(self, validation_df):
        schema_df = self.read_schema()
        validated_df = validation_df.copy()

        for i, r in schema_df.iterrows():
            print(f"""Validation for column: {r["column_name"]}""")
            print(r)
            try:
                if r["column_name"] not in validation_df:
                    raise ValueError(f"""Column {r['column_name']} is not present in the dataset""")
                if r["is_mandatory"] == "True":
                    validated_df[r["column_name"]] = pd.to_datetime(validation_df[r["column_name"]])
                    if validation_df[r["column_name"]].isnull().values.any():
                        raise ValueError(f"""Mandatory column: {r["column_name"]} has null values""")

                if r["datatype"] == "date" or r["datatype"] == "datetime":
                    validated_df = validation_df[validation_df[r["column_name"]].notnull()]
                    pd.to_datetime(validated_df[r['column_name']], format=r["format"], errors='raise')

                elif r["datatype"] == "int":
                    validation_df[r["column_name"]] = validation_df[r["column_name"]].fillna(r["default_value"])
                    validation_df[r["column_name"]].astype('int')
                if r["allowable_values"] != "" and not pd.isna(r["allowable_values"]):
                    print(r["allowable_values"])
                    if validation_df[r["column_name"]].str.contains(r["allowable_values"], na=False).all():
                        print(validation_df[validation_df[r["column_name"]].str.contains(r["allowable_values"], na=False)])
                        raise ValueError(f"""Data in column {r['column_name']} is not in the allowable values.""")

            except ValueError as e:
                logging.error(f"""Validation failed for column: {r['column_name']}, with datatype: {r['datatype']} with Exception: {e}.
                                Data has been discarded.""")
            except Exception as e:
                # TO-DO: The dataset, which failed validation needs to be stored in a failed data storage to be analysed later,
                # TO-DO: Discard only faulty data, not the entire batch
                logging.error(
                    f"""Validation failed for column: {r['column_name']}, with datatype: {r['datatype']} with Exception: {e}.
                    Data has been discarded.""")
        return validation_df
