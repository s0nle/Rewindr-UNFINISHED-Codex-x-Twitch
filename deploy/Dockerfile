FROM node:24-bookworm-slim AS build
WORKDIR /app
COPY package.json package-lock.json ./
RUN npm ci
COPY tsconfig.json ./
COPY src ./src
COPY scripts ./scripts
COPY web ./web
COPY supabase ./supabase
# These values are public frontend configuration. Never pass secrets as build arguments.
ARG TWITCH_EBS_PUBLIC_URL
ARG TWITCH_EXTENSION_CLIENT_ID
ARG TWITCH_REDIRECT_URI=http://localhost:3000/twitch/callback
RUN TWITCH_EBS_PUBLIC_URL="$TWITCH_EBS_PUBLIC_URL" TWITCH_EXTENSION_CLIENT_ID="$TWITCH_EXTENSION_CLIENT_ID" TWITCH_REDIRECT_URI="$TWITCH_REDIRECT_URI" npm run build
RUN npm prune --omit=dev

FROM node:24-bookworm-slim
RUN apt-get update && apt-get install -y --no-install-recommends ffmpeg ca-certificates && rm -rf /var/lib/apt/lists/*
WORKDIR /app
ENV NODE_ENV=production FFMPEG_PATH=/usr/bin/ffmpeg FFPROBE_PATH=/usr/bin/ffprobe DATA_DIR=/app/.local/rewindr
COPY --from=build --chown=node:node /app/node_modules ./node_modules
COPY --from=build --chown=node:node /app/package.json ./package.json
COPY --from=build --chown=node:node /app/dist ./dist
COPY --from=build --chown=node:node /app/supabase ./supabase
RUN mkdir -p /app/.local/rewindr && chown -R node:node /app/.local
USER node
EXPOSE 3001
CMD ["node", "dist/src/twitch-server.js"]
