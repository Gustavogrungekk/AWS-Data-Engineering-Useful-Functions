import boto3
from datetime import datetime
import time
import pandas as pd

def get_glue_table_info(database, table_name, region='sa-east-1'):
    """Obtém informações da tabela do Glue Catalog"""
    glue = boto3.client('glue', region_name=region)
    try:
        response = glue.get_table(DatabaseName=database, Name=table_name)
        return response['Table']
    except Exception as e:
        print(f"Erro ao obter informações da tabela {database}.{table_name}: {str(e)}")
        return None

def build_quality_query(table_info):
    """Constroi a query de qualidade baseada na estrutura da tabela"""
    database = table_info['DatabaseName']
    table_name = table_info['Name']
    partitions = table_info['PartitionKeys']
    
    if partitions:
        partition_columns = [p['Name'] for p in partitions]
        concat_parts = []
        for col in partition_columns:
            concat_parts.append(f"'{col}=', CAST({col} AS VARCHAR)")
        partition_expr = "CONCAT(" + ", '/', ".join(concat_parts) + ") AS partition"
        partition_select = partition_expr + ", "
        group_by = "GROUP BY " + ", ".join(partition_columns)
    else:
        partition_select = "NULL AS partition, "
        group_by = ""
    
    return f"""
    SELECT
        '{datetime.now().strftime('%Y%m%d')}' AS anomesdia,
        '{table_name}' AS table_name,
        '{database}' AS table_schema,
        {partition_select}
        COUNT(1) AS records
    FROM {database}.{table_name}
    {group_by}
    """

def wait_for_query(execution_id, region='sa-east-1', max_attempts=30, sleep_seconds=5):
    """Aguarda a conclusão da query no Athena"""
    athena = boto3.client('athena', region_name=region)
    for _ in range(max_attempts):
        response = athena.get_query_execution(QueryExecutionId=execution_id)
        status = response['QueryExecution']['Status']['State']
        if status in ['SUCCEEDED', 'FAILED', 'CANCELLED']:
            return status == 'SUCCEEDED'
        time.sleep(sleep_seconds)
    return False

def get_query_results(execution_id, region='sa-east-1'):
    """Recupera os resultados de uma query do Athena"""
    athena = boto3.client('athena', region_name=region)
    results = []
    next_token = None
    
    while True:
        kwargs = {'QueryExecutionId': execution_id}
        if next_token:
            kwargs['NextToken'] = next_token
            
        response = athena.get_query_results(**kwargs)
        
        # Processar cabeçalhos
        columns = [col['Name'] for col in response['ResultSet']['ResultSetMetadata']['ColumnInfo']]
        
        # Processar linhas (ignorando o header)
        for row in response['ResultSet']['Rows'][1:]:
            values = [field.get('VarCharValue', '') for field in row['Data']]
            results.append(dict(zip(columns, values)))
        
        next_token = response.get('NextToken')
        if not next_token:
            break
    
    return pd.DataFrame(results)

def run_quality_check(tables: list, region='sa-east-1', output_location='s3://athena-query-results/'):
    """Executa a verificação de qualidade e retorna um DataFrame consolidado"""
    athena = boto3.client('athena', region_name=region)
    all_results = []
    
    for table in tables:
        try:
            database, table_name = table.split('.')
            
            # Obter metadados da tabela
            table_info = get_glue_table_info(database, table_name, region)
            if not table_info:
                continue
                
            # Construir query
            query = build_quality_query(table_info)
            
            # Executar query
            response = athena.start_query_execution(
                QueryString=query,
                QueryExecutionContext={'Database': database},
                ResultConfiguration={'OutputLocation': output_location}
            )
            execution_id = response['QueryExecutionId']
            print(f"Query iniciada para {table} (ID: {execution_id})")
            
            # Aguardar conclusão
            if wait_for_query(execution_id, region):
                # Coletar resultados
                df = get_query_results(execution_id, region)
                all_results.append(df)
            else:
                print(f"Falha na query para {table}")
                
        except Exception as e:
            print(f"Erro na tabela {table}: {str(e)}")
    
    return pd.concat(all_results, ignore_index=True) if all_results else pd.DataFrame()
