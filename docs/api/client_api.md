# 404.xyz Gateway client API

1) Get your API key: https://auth.404.xyz
2) Select the closest gateway and replace `{REGION}` with one of the following:
```
EU:      gateway-eu.404.xyz
US-EAST: gateway-us-east.404.xyz
US-WEST: gateway-us-west.404.xyz
```

The gateway requires the `x-api-key` header for authentication.

## Curl Examples

### 1. Add Task
```console
curl --http3 -X POST "https://{REGION}:4443/add_task" \
     -H "content-type: application/json" \
     -H "x-api-key: {YOUR-API-KEY}" \
     -d '{"prompt": "mechanic robot"}'
```

### 2. Get Task Status.
**Note:** Use the task ID obtained from `/add_task`.
```console
curl --http3 "https://{REGION}:4443/get_status?id=123e4567-e89b-12d3-a456-426614174000" \
     -H "x-api-key: {YOUR-API-KEY}"
```

### 3. Get Result in SPZ format.
**Note:** Use the task ID obtained from `/add_task`.<br>
To unpack the result, use C++ or Python bindings from https://github.com/404-Repo/spz or the web interface at https://spz.404.xyz/

```console
curl --http3 "https://{REGION}:4443/get_result?id=123e4567-e89b-12d3-a456-426614174000" \
     -H "x-api-key: {YOUR-API-KEY}" \
     -o result.spz
```

## Python Examples

### 1. Add Task
```python
import httpx

region = "gateway-eu.404.xyz"  # Replace with your region
api_key = "{YOUR-API-KEY}"

url = f"https://{region}:4443/add_task"
headers = {"content-type": "application/json", "x-api-key": api_key}
data = {"prompt": "mechanic robot"}

with httpx.Client(http2=True) as client:
    response = client.post(url, headers=headers, json=data)
    print(response.json())
```

### 2. Get Task Status
```python
import httpx

region = "gateway-eu.404.xyz"  # Replace with your region
api_key = "{YOUR-API-KEY}"

url = f"https://{region}:4443/get_status"
headers = {"x-api-key": api_key}
params = {"id": "bc2e40a1-1e51-4a09-8a58-c42b93b573b2"}

with httpx.Client(http2=True) as client:
    response = client.get(url, headers=headers, params=params)
    print(response.status_code, response.text)
```

**Note:** This endpoint uses a GET request with a JSON body, which is non-standard but specified by the API.

### 3. Get Result in SPZ format.
**Note:** Use the task ID obtained from `/add_task`.<br>
To unpack the result, use C++ or Python bindings from https://github.com/404-Repo/spz or the web interface at https://spz.404.xyz/

```python
import httpx

region = "gateway-eu.404.xyz"  # Replace with your region
api_key = "{YOUR-API-KEY}"
task_id = "123e4567-e89b-12d3-a456-426614174000"

url = f"https://{region}:4443/get_result"
params = {"id": task_id}
headers = {"x-api-key": api_key}

with httpx.Client(http2=True) as client:
    response = client.get(url, headers=headers, params=params)
    with open("result.spz", "wb") as f:
        f.write(response.content)
```


### Complete example with SPZ decompression
Download and install prebuilt [pyspz package](https://github.com/404-Repo/spz/releases) for your system.
```Python
import httpx
import time
import pyspz

API_KEY = "API_KEY"
GATEWAY = "gateway-eu.404.xyz"
BASE_URL = f"https://{GATEWAY}:4443"

def generate_ply(prompt: str, output_file: str = "result.ply") -> bytes:
    with httpx.Client(base_url=BASE_URL, headers={"x-api-key": API_KEY}) as client:
        task_id = client.post("/add_task", json={"prompt": prompt}).json()["id"]
        print(f"Task submitted: {task_id}")

        while True:
            try:
                status = client.get("/get_status", params={"id": task_id}).json()["status"]
                if status == "Success": break
            except (httpx.HTTPError, ValueError, KeyError) as e:
                print(f"Error getting status: {e}")
                raise
            time.sleep(1)

        compressed_data = client.get("/get_result", params={"id": task_id}).content
        ply = pyspz.decompress(compressed_data, include_normals=False)
        with open(output_file, 'wb') as f:
            f.write(ply)
        print(f"Result saved to {output_file}")
        return ply

if __name__ == "__main__":
    prompt = "human with happy face"
    generate_ply(prompt=prompt, output_file="result.ply")
```
