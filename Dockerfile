FROM node:22-alpine AS builder

WORKDIR /app

COPY package.json package-lock.json* ./
RUN npm ci

COPY tsconfig.json ./
COPY src/ ./src/

RUN npm run build

FROM node:22-alpine

WORKDIR /app

RUN apk add --no-cache tini

COPY package.json package-lock.json* ./
RUN npm ci --omit=dev

COPY --from=builder /app/dist ./dist
COPY --from=builder /app/src/api/static ./dist/api/static

RUN mkdir -p /app/data

ENTRYPOINT ["/sbin/tini", "--"]
CMD ["node", "dist/index.js"]
