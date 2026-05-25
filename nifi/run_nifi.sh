#!/bin/bash

if [ -f "./.env" ]; then
    export $(cat ./.env | grep -v '#' | awk '/=/ {print $1}')
fi

NIFI_URL="https://localhost:8443/nifi-api"

if [ -z "$NIFI_USER" ] || [ -z "$NIFI_PASS" ]; then
    echo "Errore: Missing credential"
    exit 1
fi

TOKEN=$(curl -sk -X POST "$NIFI_URL/access/token" \
    -H 'Content-Type: application/x-www-form-urlencoded' \
    --data-urlencode "username=$NIFI_USER" \
    --data-urlencode "password=$NIFI_PASS")

if [ -z "$TOKEN" ] || [[ "$TOKEN" == *"Invalid"* ]]; then
    echo "Errore: Authentication failed."
    exit 1
fi

curl -sk -X PUT "$NIFI_URL/flow/process-groups/root/controller-services" \
    -H "Authorization: Bearer $TOKEN" \
    -H 'Content-Type: application/json' \
    -d '{"id":"root","state":"ENABLED"}' > /dev/null

curl -sk -X GET "$NIFI_URL/process-groups/root/processors" -H "Authorization: Bearer $TOKEN" | python3 -c "
import sys, json, urllib.request, ssl

url = '$NIFI_URL'
token = '$TOKEN'

try:
    data = json.load(sys.stdin)
    processors = data.get('processors', [])
    
    if not processors:
        print('Errore: No processor found')
        sys.exit(1)

    invoke_http_id = None
    invoke_http_version = None
    
    ctx = ssl.create_default_context()
    ctx.check_hostname = False
    ctx.verify_mode = ssl.CERT_NONE

    count = 0
    for p in processors:
        p_id = p['id']
        p_name = p['component']['name']
        p_version = p['revision']['version']
        
        if 'InvokeHTTP' in p_name:
            invoke_http_id = p_id
            invoke_http_version = p_version
        else:
            req = urllib.request.Request(f'{url}/processors/{p_id}/run-status', method='PUT')
            req.add_header('Authorization', f'Bearer {token}')
            req.add_header('Content-Type', 'application/json')
            body = json.dumps({'revision': {'clientId': 'bash-script', 'version': p_version}, 'state': 'RUNNING'})
            urllib.request.urlopen(req, data=body.encode('utf-8'), context=ctx)
            count += 1

    if invoke_http_id:
        req = urllib.request.Request(f'{url}/processors/{invoke_http_id}/run-status', method='PUT')
        req.add_header('Authorization', f'Bearer {token}')
        req.add_header('Content-Type', 'application/json')
        body = json.dumps({'revision': {'clientId': 'bash-script', 'version': invoke_http_version}, 'state': 'RUN_ONCE'})
        urllib.request.urlopen(req, data=body.encode('utf-8'), context=ctx)
        print('Success.')
    else:
        print('Error: Processor InvokeHTTP non found')

except Exception as e:
    print(f'Error in python script: {e}')
"