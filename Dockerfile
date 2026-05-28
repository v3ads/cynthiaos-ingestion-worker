FROM node:20-alpine AS builder

WORKDIR /app

COPY package*.json ./
RUN npm ci --ignore-scripts

COPY tsconfig.json ./
COPY src ./src
RUN npm run build

# ── Production image ──────────────────────────────────────────────────────────
FROM node:20-alpine AS runner

WORKDIR /app
ENV NODE_ENV=production

COPY package*.json ./
RUN npm ci --omit=dev --ignore-scripts

COPY --from=builder /app/dist ./dist
# fetchReports.js is plain CommonJS with no deps — copy it to /app root.
# dist/index.js does require('../fetchReports.js') → resolves to /app/fetchReports.js
COPY fetchReports.js ./fetchReports.js

EXPOSE 3001

CMD ["node", "dist/index.js"]
