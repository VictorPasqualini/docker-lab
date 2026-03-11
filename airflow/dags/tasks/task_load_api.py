import io
import json
import boto3
import pandas as pd
import pokebase as pb
from datetime import datetime, timezone
from airflow.operators.python import get_current_context

def api_to_minio_ingest_landing(**kwargs):
    table_name = kwargs.get('table_name')
    bucket_name = kwargs.get('bucket_name')
    endpoint_url = kwargs.get('endpoint_url')
    access_key = kwargs.get('access_key')  
    secret_key = kwargs.get('secret_key')

    s3_client = boto3.client(
        's3',
        endpoint_url=endpoint_url,
        aws_access_key_id=access_key,
        aws_secret_access_key=secret_key,
    )

    airflow_context = get_current_context()

    run_id = airflow_context["run_id"]
    ingest_dt = datetime.now(timezone.utc).strftime("%Y-%m-%d")
    ingest_ts = datetime.now(timezone.utc).isoformat()

    # pokemon_index = 1
    pokemon_rows = []
    for pokemon_index in range(1, 10): 
    # while True:
        
        try :
            pokemon = pb.pokemon(pokemon_index)

        except Exception as e:
            print(f"Erro ao buscar Pokémon com ID {pokemon_index}: {e}")
            break    
        
        print(f"Pokémon {pokemon.name} (ID: {pokemon.id}) processado com sucesso.")

        pokemon_dict = {
            "id": pokemon.id,
            "name": pokemon.name,
            "base_experience": pokemon.base_experience,
            "height": pokemon.height,
            "is_default": pokemon.is_default,
            "order": pokemon.order,
            "weight": pokemon.weight,
            "abilities": [
                {
                    "name": ability.ability.name,
                    "url": ability.ability.url
                }
                for ability in pokemon.abilities
            ],
            "forms": [
                {
                    "name": form.name,
                    "url": form.url
                }
                for form in pokemon.forms
            ],
            "game_indices": [
                {
                    "game_index": game_index.game_index,
                    "version": {
                        "name": game_index.version.name,
                        "url": game_index.version.url
                    }
                }
                for game_index in pokemon.game_indices
            ],
            "held_items": [
                {
                    "item": {
                        "name": held_item.item.name,
                        "url": held_item.item.url
                    }
                }
                for held_item in pokemon.held_items
            ],
            "moves": [
                {
                    "move": {
                        "name": move.move.name,
                        "url": move.move.url
                    }
                }
                for move in pokemon.moves
            ],
            "species": {
                "name": pokemon.species.name,
                "url": pokemon.species.url
            },
            "stats": [
                {
                    "base_stat": stat.base_stat,
                    "effort": stat.effort,
                    "stat": {
                        "name": stat.stat.name,
                        "url": stat.stat.url
                    }
                }
                for stat in pokemon.stats
            ],
            "types": [
                {
                    "slot": type_.slot,
                    "type": {
                        "name": type_.type.name,
                        "url": type_.type.url
                    }
                }
                for type_ in pokemon.types
            ]
        } 

        pokemon_json = json.dumps(pokemon_dict, ensure_ascii=False)

        pokemon_rows.append(
            {   
                "pokemon_id": getattr(pokemon, "id", None),
                "pokemon_name": getattr(pokemon, "name", None),
                "pokemon_info": pokemon_json,
                "run_id": run_id,
                "ingest_dt": ingest_dt,
                "ingest_ts": ingest_ts
            }
        )

        # pokemon_index += 1

    df_pokemon = pd.DataFrame(pokemon_rows)

    parquet_buffer = io.BytesIO()
    df_pokemon.to_parquet(parquet_buffer, index=False)

    print(f"DataFrame de Pokémon criado com {len(df_pokemon)} registros. Iniciando upload para MinIO...")

    partition_name = f"{table_name}/ingest_dt={ingest_dt}/pokemon.parquet"

    s3_client.put_object(Bucket=bucket_name, Key=partition_name, Body=parquet_buffer.getvalue())

    print(f"Upload do arquivo {partition_name} para o bucket {bucket_name} concluído com sucesso.")


    
