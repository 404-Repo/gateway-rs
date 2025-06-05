# 404.xyz Gateway client API

**Note:** Replace `{REGION}` with one of the following:
- `gateway-eu.404.xyz`
- `gateway-us-east.404.xyz`
- `gateway-us-west.404.xyz`

The gateway requires the `x-api-key` header for authentication.

## Curl Examples

### 1. Get Load for the cluster
```console
curl --http3 "https://{REGION}:4443/get_load" \
```

### 2. Add Task
```console
curl --http3 -X POST "https://{REGION}:4443/add_task" \
     -H "content-type: application/json" \
     -H "x-api-key: <YOUR-API-KEY>" \
     -d '{"prompt": "mechanic robot"}'
```

### 3. Get Task Status.
**Note:** Use the task ID obtained from `/add_task`.
```console
curl --http3 "https://{REGION}:4443/get_status?id=123e4567-e89b-12d3-a456-426614174000" \
     -H "x-api-key: <YOUR-API-KEY>"
```

### 4. Get Result
**Note:** Use the task ID obtained from `/add_task`.

```console
curl --http3 "https://{REGION}:4443/get_result?id=123e4567-e89b-12d3-a456-426614174000" \
     -H "x-api-key: <YOUR-API-KEY>" \
     -o result.spz
```

## Python Examples

### 1. Get Load for the cluster
```python
import httpx

region = "gateway-eu.404.xyz"  # Replace with your region

url = f"https://{region}:4443/get_load"

with httpx.Client(http2=True) as client:
    response = client.get(url)
    print(response.json())
```

### 2. Add Task
```python
import httpx

region = "gateway-eu.404.xyz"  # Replace with your region
api_key = "<YOUR-API-KEY>"

url = f"https://{region}:4443/add_task"
headers = {"content-type": "application/json", "x-api-key": api_key}
data = {"prompt": "mechanic robot"}

with httpx.Client(http2=True) as client:
    response = client.post(url, headers=headers, json=data)
    print(response.json())
```

### 3. Get Task Status
```python
import httpx

region = "gateway-eu.404.xyz"  # Replace with your region
api_key = "<YOUR-API-KEY>"

url = f"https://{region}:4443/get_status"
headers = {"x-api-key": api_key}
params = {"id": "bc2e40a1-1e51-4a09-8a58-c42b93b573b2"}

with httpx.Client(http2=True) as client:
    response = client.get(url, headers=headers, params=params)
    print(response.status_code, response.text)
```

**Note:** This endpoint uses a GET request with a JSON body, which is non-standard but specified by the API.

### 4. Get Result
**Note:** Use the task ID obtained from `/add_task`.

```python
import httpx

region = "gateway-eu.404.xyz"  # Replace with your region
api_key = "<YOUR-API-KEY>"
task_id = "123e4567-e89b-12d3-a456-426614174000"

url = f"https://{region}:4443/get_result"
params = {"id": task_id}
headers = {"x-api-key": api_key}

with httpx.Client(http2=True) as client:
    response = client.get(url, headers=headers, params=params)
    with open("result.spz", "wb") as f:
        f.write(response.content)
```
