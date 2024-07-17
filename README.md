# ⚡ Bun Starter Function

A simple starter function. Edit `src/main.ts` to get started and create something awesome! 🚀

## 🧰 Usage

### GET /

- Returns a "Hello, World!" message.

**Response**

Sample `200` Response:

```text
Hello, World!
```

### POST, PUT, PATCH, DELETE /

- Returns a "Learn More" JSON response.

**Response**

Sample `200` Response:

```json
{
  "motto": "Build like a team of hundreds_",
  "learn": "https://appwrite.io/docs",
  "connect": "https://appwrite.io/discord",
  "getInspired": "https://builtwith.appwrite.io"
}
```

## Local development

Just use [`entrypoint.ts`](./entrypoint.ts) as the start script by running `bun entrypoint.ts`. 

You can also set breakpoints and debug in IDE like [vscode](https://marketplace.visualstudio.com/items?itemName=oven.bun-vscode)/[idea](https://github.com/oven-sh/bun/issues/5720).

## ⚙️ Configuration

| Setting           | Value         |
| ----------------- | ------------- |
| Runtime           | Bun (1.1)     |
| Entrypoint        | `src/main.ts` |
| Build Commands    | `bun install` |
| Permissions       | `any`         |
| Timeout (Seconds) | 15            |

## 🔒 Environment Variables

No environment variables required.
