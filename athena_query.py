import time
import pandas as pd


def get_var_char_values(d):
    return [obj['VarCharValue'] if len(obj) > 0 else None for obj in d['Data']]


def query_results(client, params, return_results=True, wait_seconds=100):

    print('Starting query:\n' + params['query'])
    ## This function executes the query and returns the query execution ID
    response_query_execution_id = client.start_query_execution(QueryString=params['query'],
            QueryExecutionContext={'Database': params['database']},
            ResultConfiguration={'OutputLocation': 's3://' + params['bucket'] + '/' + params['path']})

    while wait_seconds > 0:
        wait_seconds = wait_seconds - 1
        response_get_query_details = client.get_query_execution(
                QueryExecutionId=response_query_execution_id['QueryExecutionId'])
        status = response_get_query_details['QueryExecution']['Status']['State']
        if (status == 'FAILED') or (status == 'CANCELLED'):
            failure_reason = response_get_query_details['QueryExecution']['Status']['StateChangeReason']
            print(failure_reason)
            return None, None

        elif status == 'SUCCEEDED':
            location = response_get_query_details['QueryExecution']['ResultConfiguration'][
                'OutputLocation']
            print('Query successful!')

            ## Function to get output results
            response_query_result = client.get_query_results(
                    QueryExecutionId=response_query_execution_id['QueryExecutionId'])
            result_data = response_query_result['ResultSet']

            if len(result_data['Rows']) > 1:
                print('Results are available at ' + location)
                if return_results:
                    header = result_data['Rows'][0]
                    rows = result_data['Rows'][1:]
                    header = get_var_char_values(header)
                    result = pd.DataFrame([dict(zip(header, get_var_char_values(row))) for row in rows])
                    return location, result
            else:
                print('Result has zero length.')

                return location, None

        else:
            time.sleep(1)

    print('Timed out after 30 minutes.')
    return None, None