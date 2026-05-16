import redis
import json

def write_results_on_redis(data_list, query_name):
    """
    Salva i risultati su Redis sotto forma di stringa JSON.
    """
    try:
        r = redis.Redis(host='redis-server', port=6379, decode_responses=True)
        r.set(f"sabd:results:{query_name}", json.dumps(data_list))
        print(f"Risultati di {query_name} salvati con successo su Redis.")
    except Exception as e:
        print(f"Errore durante il salvataggio su Redis: {e}")