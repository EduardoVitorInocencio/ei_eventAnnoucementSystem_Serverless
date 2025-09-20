import json
import boto3
import os
from botocore.exceptions import ClientError
from dotenv import load_dotenv

# Carrega variáveis de ambiente do arquivo .env
load_dotenv()

# Inicializa clientes AWS
sns = boto3.client('sns')
s3 = boto3.client('s3')

# Obtém valores sensíveis do ambiente
TOPIC_ARN = os.getenv('SNS_TOPIC_ARN')  # ARN do tópico SNS
BUCKET_NAME = os.getenv('BUCKET_NAME')  # Nome do bucket S3
FILE_NAME = "subscribers.json"  # Arquivo para armazenar inscritos

def lambda_handler(event, context):
    # Bloco 1: Validação e parsing do corpo da requisição
    try:
        body = json.loads(event['body'])
    except (KeyError, json.JSONDecodeError):
        return {
            'statusCode': 400,
            'body': json.dumps({'error': 'Corpo da requisição inválido ou ausente'})
        }

    # Bloco 2: Validação do campo email
    email = body.get('email')
    
    if not email:
        return {
            'statusCode': 400,
            'body': json.dumps({'error': 'Email é obrigatório'})
        }

    # Bloco 3: Leitura da lista de inscritos existente no S3
    try:
        response = s3.get_object(Bucket=BUCKET_NAME, Key=FILE_NAME)
        subscribers = json.loads(response['Body'].read().decode('utf-8'))
        if not isinstance(subscribers, list):
            subscribers = []
    except ClientError as e:
        if e.response['Error']['Code'] == 'NoSuchKey':
            subscribers = []  # Arquivo não existe, inicia lista vazia
        else:
            return {
                'statusCode': 500,
                'body': json.dumps({'error': 'Erro ao acessar o armazenamento'})
            }

    # Bloco 4: Adição do novo email se não existir
    if email not in subscribers:
        subscribers.append(email)
        
        # Persistência da lista atualizada no S3
        try:
            s3.put_object(
                Bucket=BUCKET_NAME,
                Key=FILE_NAME,
                Body=json.dumps(subscribers),
                ContentType="application/json"
            )
        except ClientError:
            return {
                'statusCode': 500,
                'body': json.dumps({'error': 'Erro ao salvar inscrição'})
            }

    # Bloco 5: Inscrição no tópico SNS
    try:
        sns.subscribe(
            TopicArn=TOPIC_ARN,
            Protocol="email",
            Endpoint=email
        )
    except ClientError:
        return {
            'statusCode': 500,
            'body': json.dumps({'error': 'Erro ao inscrever no tópico de notificações'})
        }

    # Bloco 6: Resposta de sucesso
    return {
        'statusCode': 200,
        'body': json.dumps({'message': f'{email} adicionado com sucesso!'})
    }