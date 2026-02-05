# 404.xyz Gateway Python Examples

This page contains Python examples for the gateway. Replace `{REGION}` and
`{YOUR-API-KEY}` with your values.
Optional: include `model` in `/add_task` to select output format. For `404-3dgs`,
`/get_result` returns SPZ by default (PLY with `compress=0`). For `404-mesh`,
`/get_result` returns GLB.

## Text to 3D (Python)
Note: `model` is only sent to `/add_task`, not `/get_status` or `/get_result`.

### 1. Add task
```python
import httpx

region = "gateway-eu.404.xyz"  # Replace with your region
api_key = "{YOUR-API-KEY}"

url = f"https://{region}:4443/add_task"
headers = {"content-type": "application/json", "x-api-key": api_key}
data = {"prompt": "mechanic robot", "model": "404-3dgs"}  # Optional model

with httpx.Client(http2=True) as client:
    response = client.post(url, headers=headers, json=data)
    print(response.json())
```

### 2. Get task status
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

### 3. Get result (SPZ, default)
Note: use the task ID returned from `/add_task`.
To unpack SPZ, use C++/Python bindings from https://github.com/404-Repo/spz
or the web interface at https://spz.404.xyz/
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

### Complete example with selectable output (SPZ or PLY)
```python
import httpx
import time

API_KEY = "API_KEY"
GATEWAY = "gateway-eu.404.xyz"
BASE_URL = f"https://{GATEWAY}:4443"

# Choose output format: "spz" (default, compressed) or "ply" (compress=0)
OUTPUT_FORMAT = "spz"  # or "ply"

with httpx.Client(base_url=BASE_URL, headers={"x-api-key": API_KEY}) as client:
    task_id = client.post(
        "/add_task",
        json={"prompt": "human with happy face", "model": "404-3dgs"},
    ).json()["id"]
    while True:
        try:
            status = client.get("/get_status", params={"id": task_id}).json().get("status", "")
            if status == "Success":
                break
        except (httpx.HTTPError, ValueError, KeyError) as e:
            print(f"Error getting status: {e}")
            raise
        time.sleep(1)
    params = {"id": task_id} if OUTPUT_FORMAT == "spz" else {"id": task_id, "compress": "0"}
    data = client.get("/get_result", params=params).content
    out_file = f"text3d_result.{ 'spz' if OUTPUT_FORMAT == 'spz' else 'ply' }"
    with open(out_file, "wb") as f:
        f.write(data)

print("Saved Text to 3D to:", out_file)
```

## 2D to 3D (Python)
Note: `model` is only sent to `/add_task`, not `/get_result`.

### 1. Add 2D to 3D task
```python
import httpx

region = "gateway-eu.404.xyz"  # Replace with your region
api_key = "{YOUR-API-KEY}"

url = f"https://{region}:4443/add_task"
headers = {"x-api-key": api_key}

with httpx.Client(http2=True) as client:
    with open("input_2d_image.jpg", "rb") as f:
        files = {"image": f}
        data = {"model": "404-3dgs"}  # Optional model
        response = client.post(url, headers=headers, files=files, data=data)
    print(response.json())
```

### 2. Get result (SPZ, default)
Note: use the task ID returned from `/add_task`.
To unpack SPZ, use C++/Python bindings from https://github.com/404-Repo/spz
or the web interface at https://spz.404.xyz/
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

### 3. Get result (PLY)
```python
import httpx

region = "gateway-eu.404.xyz"  # Replace with your region
api_key = "{YOUR-API-KEY}"
task_id = "123e4567-e89b-12d3-a456-426614174000"

url = f"https://{region}:4443/get_result"
params = {"id": task_id, "compress": "0"}
headers = {"x-api-key": api_key}

with httpx.Client(http2=True) as client:
    response = client.get(url, headers=headers, params=params)
    with open("model.ply", "wb") as f:
        f.write(response.content)
```

### Complete 2D to 3D pipeline example (selectable output)
```python
import httpx
import time

API_KEY = "{YOUR-API-KEY}"
GATEWAY = "gateway-eu.404.xyz"
BASE_URL = f"https://{GATEWAY}:4443"

# Choose output format: "spz" (default, compressed) or "ply" (compress=0)
OUTPUT_FORMAT = "ply"  # or "spz"

with httpx.Client(base_url=BASE_URL, headers={"x-api-key": API_KEY}) as client:
    with open("input_2d_image.jpg", "rb") as f:
        files = {"image": f}
        task_id = client.post(
            "/add_task",
            files=files,
            data={"model": "404-3dgs"},
        ).json()["id"]
    while True:
        try:
            status = client.get("/get_status", params={"id": task_id}).json().get("status", "")
            if status == "Success":
                break
        except (httpx.HTTPError, ValueError, KeyError) as e:
            print(f"Error getting status: {e}")
            raise
        time.sleep(1)
    params = {"id": task_id} if OUTPUT_FORMAT == "spz" else {"id": task_id, "compress": "0"}
    data = client.get("/get_result", params=params).content
    out_file = f"image3d_result.{ 'spz' if OUTPUT_FORMAT == 'spz' else 'ply' }"
    with open(out_file, "wb") as f:
        f.write(data)

print("Saved 2D to 3D to:", out_file)
```
