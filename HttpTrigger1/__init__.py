"""
This function transforms bank statements in JSON format into Excel.
Originally used in Azure Data Pipeline in which bank statement pdfs are transformed into JSON files after OCR.
"""

import json
import pandas as pd
import numpy as np
import pandas as pd
import azure.functions as func
import logging
import io

def main(req: func.HttpRequest) -> func.HttpResponse:
    
    # Load bank statement JSON data
    json_input = str(req.get_body(), encoding = 'utf-8')
    logging.info('str(req.get_body) utf-8: ' + str(req.get_body(), encoding = 'utf-8'))

    # Load JSON data
    json_data = json.loads(json_input)["content"]

    # DataFrame creation
    temp_df = pd.DataFrame(json_data)
    index_len = len(temp_df.index)

    # Creation of np array
    num_of_cols = max(temp_df['column']) + 1
    num_of_rows = int(index_len / num_of_cols) + 1
    max_text_bytes = len(max(temp_df['text']))
    results_array = np.chararray(shape = (num_of_rows, num_of_cols), itemsize = max_text_bytes, unicode = True)

    # Loop that iterates through temp_df and writes to np array
    for df_index in range(index_len):
        row = int(df_index / num_of_cols)
        results_array[row, temp_df.loc[df_index]['column']] = temp_df.loc[df_index]['text']

    # Write results to pandas dataframe
    results_df = pd.DataFrame(data = results_array, dtype = str, index = None, columns = None)   
    results_df.drop_duplicates(keep = 'first', inplace = True)

    # Write df to excel file
    output = io.BytesIO()
    results_df.to_excel(output, index = False, header = False)

    return func.HttpResponse(
        output.getvalue(), 
        status_code = 200,
        headers = {'Content-Disposition':'attachment; filename = "test.xlsx"'},
        mimetype = 'application/vnd.ms-excel')