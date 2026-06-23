from kafka import KafkaConsumer
import json
import pandas as pd

# Configuração do consumidor
consumer = KafkaConsumer(
    'dados_usuarios',
    bootstrap_servers='localhost:9092',
    auto_offset_reset='earliest',
    enable_auto_commit=True,
    value_deserializer=lambda x: json.loads(x.decode('utf-8'))
)

BATCH_SIZE = 5  # tamanho do lote
buffer = []

print("Consumidor iniciado...")

for msg in consumer:
    usuario = msg.value
    print(f"Recebido: {usuario}")

    buffer.append(usuario)

    # Quando atingir o tamanho do lote
    if len(buffer) == BATCH_SIZE:
        print("\n📊 Criando DataFrame...")

        df = pd.DataFrame(buffer)

        print("\nDataFrame completo:")
        print(df)

        # 🔎 Filtro: idade > 30
        df_filtrado = df[df['idade'] > 30]

        print("\n📌 Usuários com idade > 30:")
        print(df_filtrado)

        # 🧠 Exemplo adicional: agrupamento por cidade
        print("\n📍 Contagem por cidade:")
        print(df.groupby('cidade').size())

        # Limpa buffer
        buffer.clear()
