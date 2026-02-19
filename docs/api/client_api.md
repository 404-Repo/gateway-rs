# 404.xyz Gateway client API

This page gives the minimal setup and where to find runnable examples.

## Quick start

1) Get your API key: https://auth.404.xyz
2) Pick the closest gateway base URL:

- **EU**: `https://gateway-eu.404.xyz:4443`
- **US-EAST**: `https://gateway-us-east.404.xyz:4443`
- **US-WEST**: `https://gateway-us-west.404.xyz:4443`
3) Send requests with the `x-api-key` header.

## Two pipelines

- Text to 3D: send a prompt to `/add_task`, then poll `/get_status`, then download from `/get_result`.
- 2D to 3D: send an image file to `/add_task` (no prompt), then poll and download the result.

You can also pass an optional `model` in `/add_task` to control the output format:
`404-3dgs` returns SPZ by default (PLY with `compress=0`), while `404-mesh` returns GLB.
**Note: `404-mesh` currently supports only image-to-3d (no text prompt yet).**

Optional `seed` in `/add_task` sets the random seed for reproducible outputs; if omitted, a random seed is generated. Seed is a signed 32-bit integer (`-2147483648..2147483647`), if you send an unsigned 32-bit integer (`0..4294967295`), it is converted to signed using two's-complement cast semantics (for example, `4294967295` becomes `-1`). The task's seed is included in `/get_tasks` responses.

## Examples

- Console (curl): [console.md](console.md)
- Python: [python.md](python.md)
- TypeScript (Bun/Node.js): [typescript.md](typescript.md)
