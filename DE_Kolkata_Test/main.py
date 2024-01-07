from src.baker_hughes_extractor.utils.etl import etl_handler
import pandas as pd

if __name__ == "__main__":
    try:
        etl_handler()
        print("Succesfully uploaded data!!!")
    except Exception as e:
        print(e)