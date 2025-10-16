# 404.xyz Gateway client API

1) Get your API key: https://auth.404.xyz
2) Select the closest gateway and replace `{REGION}` with one of the following:
```
EU:      gateway-eu.404.xyz
US-EAST: gateway-us-east.404.xyz
US-WEST: gateway-us-west.404.xyz
```

The gateway requires the `x-api-key` header for authentication.

## Text to 3D Pipeline

This section covers generating 3D models from text prompts using the gateway API.

### Curl Examples for Text→3D

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

### 4. Get Result in PLY format.
**Note:** Use the task ID obtained from `/add_task`. Returns PLY data.

```console
curl --http3 "https://{REGION}:4443/get_result?id=123e4567-e89b-12d3-a456-426614174000&format=ply" \
     -H "x-api-key: {YOUR-API-KEY}" \
     -o model.ply
```

### Python Examples for Text→3D

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


### Complete example with selectable output (SPZ or PLY)
```python
import httpx
import time

API_KEY = "API_KEY"
GATEWAY = "gateway-eu.404.xyz"
BASE_URL = f"https://{GATEWAY}:4443"

# Choose output format: "spz" (default, compressed) or "ply"
OUTPUT_FORMAT = "spz"  # or "ply"

with httpx.Client(base_url=BASE_URL, headers={"x-api-key": API_KEY}) as client:
    task_id = client.post("/add_task", json={"prompt": "human with happy face"}).json()["id"]
    while True:
        try:
            status = client.get("/get_status", params={"id": task_id}).json().get("status", "")
            if status == "Success":
                break
        except (httpx.HTTPError, ValueError, KeyError) as e:
            print(f"Error getting status: {e}")
            raise
        time.sleep(1)
    params = {"id": task_id} if OUTPUT_FORMAT == "spz" else {"id": task_id, "format": "ply"}
    data = client.get("/get_result", params=params).content
    out_file = f"text3d_result.{ 'spz' if OUTPUT_FORMAT == 'spz' else 'ply' }"
    with open(out_file, "wb") as f:
        f.write(data)

print("Saved Text→3D to:", out_file)
```

## 2D to 3D Pipeline

This section covers the specialized workflow for converting 2D images into 3D models using the gateway API.

### Curl Examples for 2D→3D

#### 1. Add 2D→3D Task

**Important:** Do **not** submit a `prompt` together with an `image` for 2D→3D tasks. Submit **only** an image file for 2D→3D. Prompts are used for Text→3D tasks and should be sent to the Text→3D endpoints.

```console
curl --http3 -X POST "https://{REGION}:4443/add_task" \
     -F "image=@image.jpg" \
     -H "x-api-key: {YOUR-API-KEY}"
```

#### 2. Get 3D Result in SPZ format
**Note:** Use the task ID obtained from `/add_task`.<br>
To unpack the result, use C++ or Python bindings from https://github.com/404-Repo/spz or the web interface at https://spz.404.xyz/

```console
curl --http3 "https://{REGION}:4443/get_result?id=123e4567-e89b-12d3-a456-426614174000" \
     -H "x-api-key: {YOUR-API-KEY}" \
     -o result.spz
```

#### 3. Get 3D Result in PLY format
**Note:** Use the task ID obtained from `/add_task`. Returns PLY data.

```console
curl --http3 "https://{REGION}:4443/get_result?id=123e4567-e89b-12d3-a456-426614174000&format=ply" \
     -H "x-api-key: {YOUR-API-KEY}" \
     -o model.ply
```

### Python Examples for 2D→3D

#### 1. Add 2D→3D Task
```python
import httpx

region = "gateway-eu.404.xyz"  # Replace with your region
api_key = "{YOUR-API-KEY}"

url = f"https://{region}:4443/add_task"
headers = {"x-api-key": api_key}

with httpx.Client(http2=True) as client:
    with open("input_2d_image.jpg", "rb") as f:
        files = {"image": f}
        response = client.post(url, headers=headers, files=files)
    print(response.json())
```

#### 2. Get 3D Result in SPZ format
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

#### 3. Get 3D Result in PLY format
```python
import httpx

region = "gateway-eu.404.xyz"  # Replace with your region
api_key = "{YOUR-API-KEY}"
task_id = "123e4567-e89b-12d3-a456-426614174000"

url = f"https://{region}:4443/get_result"
params = {"id": task_id, "format": "ply"}
headers = {"x-api-key": api_key}

with httpx.Client(http2=True) as client:
    response = client.get(url, headers=headers, params=params)
    with open("model.ply", "wb") as f:
        f.write(response.content)
```

### Complete 2D→3D Pipeline Example (selectable output)
```python
import httpx
import time

API_KEY = "{YOUR-API-KEY}"
GATEWAY = "gateway-eu.404.xyz"
BASE_URL = f"https://{GATEWAY}:4443"

# Choose output format: "spz" (default, compressed) or "ply"
OUTPUT_FORMAT = "ply"  # or "spz"

with httpx.Client(base_url=BASE_URL, headers={"x-api-key": API_KEY}) as client:
    with open("input_2d_image.jpg", "rb") as f:
        files = {"image": f}
        task_id = client.post("/add_task", files=files).json()["id"]
    while True:
        try:
            status = client.get("/get_status", params={"id": task_id}).json().get("status", "")
            if status == "Success":
                break
        except (httpx.HTTPError, ValueError, KeyError) as e:
            print(f"Error getting status: {e}")
            raise
        time.sleep(1)
    params = {"id": task_id} if OUTPUT_FORMAT == "spz" else {"id": task_id, "format": "ply"}
    data = client.get("/get_result", params=params).content
    out_file = f"image3d_result.{ 'spz' if OUTPUT_FORMAT == 'spz' else 'ply' }"
    with open(out_file, "wb") as f:
        f.write(data)

print("Saved 2D→3D to:", out_file)
```
