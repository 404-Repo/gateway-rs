# 404.xyz Gateway Console Examples

This page contains curl examples for the gateway. Replace `{REGION}` and
`{YOUR-API-KEY}` with your values.
Optional: include `model` in `/add_task` to select output format. For `404-3dgs`,
`/get_result` returns SPZ by default (PLY with `compress=0`). For `404-mesh`,
`/get_result` returns GLB.

## Text to 3D (console)
Note: `model` is only sent to `/add_task`, not `/get_status` or `/get_result`.

### 1. Add task
```console
curl --http3 -X POST "https://{REGION}:4443/add_task" \
     -H "content-type: application/json" \
     -H "x-api-key: {YOUR-API-KEY}" \
     -d '{"prompt": "mechanic robot"}'
```

Optional: add `model` to select output format (example uses `404-mesh`).
```console
curl --http3 -X POST "https://{REGION}:4443/add_task" \
     -H "content-type: application/json" \
     -H "x-api-key: {YOUR-API-KEY}" \
     -d '{"prompt": "mechanic robot", "model": "404-mesh"}'
```

### 2. Get task status
Note: use the task ID returned from `/add_task`.
```console
curl --http3 "https://{REGION}:4443/get_status?id=123e4567-e89b-12d3-a456-426614174000" \
     -H "x-api-key: {YOUR-API-KEY}"
```

### 3. Get result (SPZ, default)
Note: use the task ID returned from `/add_task`.
To unpack SPZ, use C++/Python bindings from https://github.com/404-Repo/spz
or the web interface at https://spz.404.xyz/
```console
curl --http3 "https://{REGION}:4443/get_result?id=123e4567-e89b-12d3-a456-426614174000" \
     -H "x-api-key: {YOUR-API-KEY}" \
     -o result.spz
```

### 4. Get result (PLY, uncompressed)
Note: use the task ID returned from `/add_task`. Use `compress=0`.
```console
curl --http3 "https://{REGION}:4443/get_result?id=123e4567-e89b-12d3-a456-426614174000&compress=0" \
     -H "x-api-key: {YOUR-API-KEY}" \
     -o model.ply
```

## 2D to 3D (console)
Note: `model` is only sent to `/add_task`, not `/get_result`.

### 1. Add 2D to 3D task
Important: send only an image for 2D to 3D tasks. Do not send a prompt.
```console
curl --http3 -X POST "https://{REGION}:4443/add_task" \
     -F "image=@image.jpg" \
     -H "x-api-key: {YOUR-API-KEY}"
```

Optional: add `model` to select output format (example uses `404-mesh`).
```console
curl --http3 -X POST "https://{REGION}:4443/add_task" \
     -F "image=@image.jpg" \
     -F "model=404-mesh" \
     -H "x-api-key: {YOUR-API-KEY}"
```

### 2. Get result (SPZ, default)
Note: use the task ID returned from `/add_task`.
To unpack SPZ, use C++/Python bindings from https://github.com/404-Repo/spz
or the web interface at https://spz.404.xyz/
```console
curl --http3 "https://{REGION}:4443/get_result?id=123e4567-e89b-12d3-a456-426614174000" \
     -H "x-api-key: {YOUR-API-KEY}" \
     -o result.spz
```

### 3. Get result (PLY, uncompressed)
Note: use the task ID returned from `/add_task`. Use `compress=0`.
```console
curl --http3 "https://{REGION}:4443/get_result?id=123e4567-e89b-12d3-a456-426614174000&compress=0" \
     -H "x-api-key: {YOUR-API-KEY}" \
     -o model.ply
```
