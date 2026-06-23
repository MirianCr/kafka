from kafka import KafkaProducer
import json
import time
import random

# Configuração do produtor
# Este produtor envia mensagens JSON com dados simulados de usuários.
producer = KafkaProducer(
    bootstrap_servers='localhost:9092',
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

# Dados simulados
nomes = ['Ana', 'Carlos', 'Joao', 'Maria', 'Pedro', 'Fernanda']
cidades = ['São Paulo', 'Rio de Janeiro', 'Belo Horizonte', 'Curitiba']

def gerar_usuario(user_id):
    """Gera um registro de usuário aleatório."""
    return {
        "id": user_id,
        "nome": random.choice(nomes),
        "idade": random.randint(18, 60),
        "cidade": random.choice(cidades)
    }

# Envio contínuo de dados
user_id = 1

while True:
    usuario = gerar_usuario(user_id)
    
    print(f"Enviando: {usuario}")
    
    producer.send('dados_usuarios', value=usuario)
    
    user_id += 1
    
    # Intervalo entre mensagens
    time.sleep(1)
