import redis
import json

def write_results_on_redis(data_list, query_name):
    
    try:
        r = redis.Redis(host='redis-server', port=6379, decode_responses=True)
        r.set(f"sabd:results:{query_name}", json.dumps(data_list))
        print(f"Results of {query_name} successfully saved on Redis")
    except Exception as e:
        print(f"Error saving results to Redis: {e}")