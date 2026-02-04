# 404.xyz Gateway TypeScript Examples (Bun/Node.js)

These examples work with Bun and Node.js 18+ (which includes `fetch`).
Replace `{REGION}` and `{YOUR-API-KEY}` with your values.
Optional: include `model` in `/add_task` to select output format. For `404-3dgs`,
`/get_result` returns SPZ by default (PLY with `compress=0`). For `404-mesh`,
`/get_result` returns GLB.

## Text to 3D (TypeScript)
Note: `model` is only sent to `/add_task`, not `/get_status` or `/get_result`.

### 1. Add task
```ts
const region = "gateway-eu.404.xyz"; // Replace with your region
const apiKey = "{YOUR-API-KEY}";

const res = await fetch(`https://${region}:4443/add_task`, {
  method: "POST",
  headers: {
    "content-type": "application/json",
    "x-api-key": apiKey,
  },
  body: JSON.stringify({ prompt: "mechanic robot", model: "404-3dgs" }), // Optional model
});

const json = await res.json();
console.log(json);
```

### 2. Get task status
```ts
const region = "gateway-eu.404.xyz";
const apiKey = "{YOUR-API-KEY}";
const taskId = "bc2e40a1-1e51-4a09-8a58-c42b93b573b2";

const url = new URL(`https://${region}:4443/get_status`);
url.searchParams.set("id", taskId);

const res = await fetch(url, {
  headers: { "x-api-key": apiKey },
});

console.log(res.status, await res.text());
```

### 3. Get result (SPZ, default)
```ts
const region = "gateway-eu.404.xyz";
const apiKey = "{YOUR-API-KEY}";
const taskId = "123e4567-e89b-12d3-a456-426614174000";

const url = new URL(`https://${region}:4443/get_result`);
url.searchParams.set("id", taskId);

const res = await fetch(url, { headers: { "x-api-key": apiKey } });
const buf = Buffer.from(await res.arrayBuffer());

await Bun.write("result.spz", buf); // Bun
// await fs.promises.writeFile("result.spz", buf); // Node.js
```

### Complete example with selectable output (SPZ or PLY)
```ts
const API_KEY = "{YOUR-API-KEY}";
const GATEWAY = "gateway-eu.404.xyz";
const BASE_URL = `https://${GATEWAY}:4443`;

// Choose output format: "spz" (default, compressed) or "ply" (compress=0)
const OUTPUT_FORMAT = "spz"; // or "ply"

const addRes = await fetch(`${BASE_URL}/add_task`, {
  method: "POST",
  headers: {
    "content-type": "application/json",
    "x-api-key": API_KEY,
  },
  body: JSON.stringify({ prompt: "human with happy face", model: "404-3dgs" }),
});
const { id: taskId } = await addRes.json();

while (true) {
  const statusUrl = new URL(`${BASE_URL}/get_status`);
  statusUrl.searchParams.set("id", taskId);
  const statusRes = await fetch(statusUrl, { headers: { "x-api-key": API_KEY } });
  const statusJson = await statusRes.json();
  if (statusJson.status === "Success") break;
  await new Promise((r) => setTimeout(r, 1000));
}

const resultUrl = new URL(`${BASE_URL}/get_result`);
resultUrl.searchParams.set("id", taskId);
if (OUTPUT_FORMAT !== "spz") {
  resultUrl.searchParams.set("compress", "0");
}
const resultRes = await fetch(resultUrl, { headers: { "x-api-key": API_KEY } });
const outBuf = Buffer.from(await resultRes.arrayBuffer());
const outFile = OUTPUT_FORMAT === "spz" ? "text3d_result.spz" : "text3d_result.ply";

await Bun.write(outFile, outBuf); // Bun
// await fs.promises.writeFile(outFile, outBuf); // Node.js
console.log("Saved Text to 3D to:", outFile);
```

## 2D to 3D (TypeScript)
Note: `model` is only sent to `/add_task`, not `/get_result`.

### 1. Add 2D to 3D task
```ts
const region = "gateway-eu.404.xyz";
const apiKey = "{YOUR-API-KEY}";

const form = new FormData();
form.append("image", new Blob([await Bun.file("input_2d_image.jpg").arrayBuffer()]), "input_2d_image.jpg");
form.append("model", "404-3dgs"); // Optional model
// Node.js alternative:
// import fs from "node:fs";
// form.append("image", new Blob([fs.readFileSync("input_2d_image.jpg")]), "input_2d_image.jpg");

const res = await fetch(`https://${region}:4443/add_task`, {
  method: "POST",
  headers: { "x-api-key": apiKey },
  body: form,
});

console.log(await res.json());
```

### 2. Get result (SPZ, default)
```ts
const region = "gateway-eu.404.xyz";
const apiKey = "{YOUR-API-KEY}";
const taskId = "123e4567-e89b-12d3-a456-426614174000";

const url = new URL(`https://${region}:4443/get_result`);
url.searchParams.set("id", taskId);

const res = await fetch(url, { headers: { "x-api-key": apiKey } });
const buf = Buffer.from(await res.arrayBuffer());

await Bun.write("result.spz", buf); // Bun
// await fs.promises.writeFile("result.spz", buf); // Node.js
```

### 3. Get result (PLY)
```ts
const region = "gateway-eu.404.xyz";
const apiKey = "{YOUR-API-KEY}";
const taskId = "123e4567-e89b-12d3-a456-426614174000";

const url = new URL(`https://${region}:4443/get_result`);
url.searchParams.set("id", taskId);
url.searchParams.set("compress", "0");

const res = await fetch(url, { headers: { "x-api-key": apiKey } });
const buf = Buffer.from(await res.arrayBuffer());

await Bun.write("model.ply", buf); // Bun
// await fs.promises.writeFile("model.ply", buf); // Node.js
```

### Complete 2D to 3D pipeline example (selectable output)
```ts
const API_KEY = "{YOUR-API-KEY}";
const GATEWAY = "gateway-eu.404.xyz";
const BASE_URL = `https://${GATEWAY}:4443`;

// Choose output format: "spz" (default, compressed) or "ply" (compress=0)
const OUTPUT_FORMAT = "ply"; // or "spz"

const form = new FormData();
form.append("image", new Blob([await Bun.file("input_2d_image.jpg").arrayBuffer()]), "input_2d_image.jpg");
form.append("model", "404-3dgs"); // Optional model
// Node.js alternative:
// import fs from "node:fs";
// form.append("image", new Blob([fs.readFileSync("input_2d_image.jpg")]), "input_2d_image.jpg");

const addRes = await fetch(`${BASE_URL}/add_task`, {
  method: "POST",
  headers: { "x-api-key": API_KEY },
  body: form,
});
const { id: taskId } = await addRes.json();

while (true) {
  const statusUrl = new URL(`${BASE_URL}/get_status`);
  statusUrl.searchParams.set("id", taskId);
  const statusRes = await fetch(statusUrl, { headers: { "x-api-key": API_KEY } });
  const statusJson = await statusRes.json();
  if (statusJson.status === "Success") break;
  await new Promise((r) => setTimeout(r, 1000));
}

const resultUrl = new URL(`${BASE_URL}/get_result`);
resultUrl.searchParams.set("id", taskId);
if (OUTPUT_FORMAT !== "spz") {
  resultUrl.searchParams.set("compress", "0");
}
const resultRes = await fetch(resultUrl, { headers: { "x-api-key": API_KEY } });
const outBuf = Buffer.from(await resultRes.arrayBuffer());
const outFile = OUTPUT_FORMAT === "spz" ? "image3d_result.spz" : "image3d_result.ply";

await Bun.write(outFile, outBuf); // Bun
// await fs.promises.writeFile(outFile, outBuf); // Node.js
console.log("Saved 2D to 3D to:", outFile);
```
