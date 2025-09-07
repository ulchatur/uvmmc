import io
import csv
import codecs
import concurrent.futures
import time
import pandas as pd
from dotenv import load_dotenv
from utils.anaplan.anaplan_connection import get_conn
from anaplan_api_updated.anaplan import execute_action

load_dotenv(dotenv_path ='/Workspace/Shared/.env')

# def download_anaplan_data(
#     conn,
#     workspace_id: str,
#     model_id: str,
#     action_id: str,
#     retry_count: int,
#     skip_char: int,
#     file_delimiter_anaplan: str,
#     use_cols: list,
#     engine: str,
# ):
#     try:
#         error_lines = []
#         print('Starting download from Anaplan')
#         # read data from anaplan
#         anaplan_response = execute_action(
#             conn=conn,
#             action_id=action_id,
#             retry_count=int(retry_count),
#         )
#         print('Got Anaplan download')
#         print(type(anaplan_response))
#         # response = anaplan_response.split("\n")
#         # # print(response[0])
#         # success_responses = []
#         # error_lines = []
#         # for line in response:
#         #     try:
#         #         response = codecs.getdecoder("unicode_escape")(line)[0]
#         #         success_responses.append(response)
#         #     except UnicodeDecodeError:
#         #         error_lines.append(line)
#         # anaplan_response = "\n".join(success_responses)
#         # print(f" malformed string lines: {error_lines}")

#         anaplan_response = anaplan_response[skip_char:]
#         # print(anaplan_response[0:1000])
#         # print(usecols)

#         # convert data from string to dataframe
#         cleaned_data = clean_csv_data(anaplan_response)
#         df_final = pd.read_csv(
#             io.StringIO(cleaned_data),
#             delimiter=',',
#             engine=engine,
#             usecols=use_cols,
#         )
#         return df_final
#     except Exception as e:
#         print("Failed to transform the data pulled from anaplan.")
#         print(e)
#         raise Exception("Failed to transform the data pulled from anaplan.")


 
def fetch_chunk(offset, size):
    # Modify your execute_action to support offset/limit if possible
    # For example, pass offset/limit as parameters
    # Here, assuming execute_action can take offset/limit (you'll need to adapt)
    response = execute_action(
        conn=conn,
        action_id=action_id,
        retry_count=int(retry_count),
        offset=offset,
        limit=size
    )
    return response
 
def download_anaplan_data(
    conn,
    workspace_id: str,
    model_id: str,
    action_id: str,
    chunk_size: int,
    retry_count: int,
    skip_char: int,
    file_delimiter_anaplan: str,
    use_cols: list,
    engine: str,
):
    # Calculate number of chunks
    responses = []
    offset = 0
    more_data = True

    with concurrent.futures.ThreadPoolExecutor(max_workers=4) as executor:
        futures = []

        while more_data:
            # Submit a fetch task for the current offset
            futures.append(executor.submit(fetch_chunk, offset, chunk_size))
            offset += chunk_size

            # Check completed futures
            done, _ = concurrent.futures.wait(futures, return_when=concurrent.futures.FIRST_COMPLETED)

            for future in done:
                try:
                    response = future.result()
                    responses.append(response)
                    # If response is smaller than chunk_size, we've reached the end
                    if len(response) < chunk_size:
                        more_data = False
                except Exception as e:
                    print(f"Error fetching chunk at offset {offset}: {e}")
                    more_data = False
                finally:
                    futures.remove(future)

        # Wait for remaining futures to complete
        for future in futures:
            try:
                response = future.result()
                responses.append(response)
            except Exception as e:
                print(f"Error in remaining fetch: {e}")
 
    # Combine responses
    combined_response = "\n".join(responses)
    # Remove skip_char
    combined_response = combined_response[skip_char:]
    # Convert to DataFrame
    cleaned_data = clean_csv_data(combined_response)
    df_final = pd.read_csv(
        io.StringIO(cleaned_data),
        delimiter=',',
        engine=engine,
        usecols=use_cols,
    )
    return df_final

def clean_csv_data(raw_data):
    cleaned_lines = []
    reader = csv.reader(io.StringIO(raw_data))
    for row in reader:
        # Re-serialize each row to ensure proper quoting
        output = io.StringIO()
        writer = csv.writer(output, quoting=csv.QUOTE_MINIMAL)
        writer.writerow(row)
        cleaned_line = output.getvalue().strip()
        cleaned_lines.append(cleaned_line)
    return "\n".join(cleaned_lines)
