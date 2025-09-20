import json
import boto3
import os
from botocore.exceptions import ClientError
from dotenv import load_dotenv

# Carrega variáveis de ambiente do arquivo .env
load_dotenv()

# Inicializa clientes AWS com configuração padrão
s3 = boto3.client('s3')
sns = boto3.client('sns')

# Obtém valores sensíveis do ambiente (configurados no Lambda ou .env)
BUCKET_NAME = os.getenv('BUCKET_NAME')  # Valor do arquivo .env
FILE_NAME = "events.json"
TOPIC_ARN = os.getenv('SNS_TOPIC_ARN')  # Valor do arquivo .env

def lambda_handler(event, context):
    # Bloco 1: Validação e parsing do corpo da requisição
    body_str = event.get('body', '{}')
    
    try:
        body = json.loads(body_str)
    except json.JSONDecodeError:
        return {
            'statusCode': 400,
            'body': json.dumps({'error': 'JSON inválido'})
        }

    # Bloco 2: Validação de campos obrigatórios
    title = body.get('title')
    date = body.get('date')

    if not title or not date:
        return {
            'statusCode': 400,
            'body': json.dumps({'error': 'Campos "title" e "date" são obrigatórios'})
        }

    # Bloco 3: Leitura do arquivo existente no S3
    try:
        response = s3.get_object(Bucket=BUCKET_NAME, Key=FILE_NAME)
        events = json.loads(response['Body'].read().decode('utf-8'))
        if not isinstance(events, list):
            events = []
    except ClientError as e:
        if e.response['Error']['Code'] == 'NoSuchKey':
            events = []  # Arquivo não existe, inicia lista vazia
        else:
            raise e  # Propaga outros erros de AWS

    # Bloco 4: Adição do novo evento
    events.append(body)

    # Bloco 5: Persistência no S3
    s3.put_object(
        Bucket=BUCKET_NAME,
        Key=FILE_NAME,
        Body=json.dumps(events, ensure_ascii=False),
        ContentType="application/json"
    )

    # Bloco 6: Notificação via SNS
    sns.publish(
        TopicArn=TOPIC_ARN,
        Message=f"Novo evento cadastrado: {title} em {date}",
        Subject="Novo Evento Disponível!"
    )

    # Bloco 7: Resposta de sucesso
    return {
        'statusCode': 200,
        'body': json.dumps({'message': 'Evento criado e notificação enviada!'})
    }