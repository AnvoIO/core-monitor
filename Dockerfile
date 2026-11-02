FROM node:22.23.4-alpine3.24 AS builder

WORKDIR /app

COPY package.json package-lock.json* ./
RUN npm ci

COPY tsconfig.json ./
COPY src/ ./src/

RUN npm run build

FROM node:22.23.4-alpine3.24

WORKDIR /app

RUN apk add --no-cache tini

COPY package.json package-lock.json* ./
RUN npm ci --omit=dev

COPY --from=builder /app/dist ./dist
COPY --from=builder /app/src/api/static ./dist/api/static

RUN mkdir -p /app/data

RUN addgroup -S app && adduser -S app -G app
USER app

ENTRYPOINT ["/sbin/tini", "--"]
# Default: combined mode (backwards compatible)
# Override with "node dist/writer.js" or "node dist/reader.js"
CMD ["node", "dist/index.js"]
