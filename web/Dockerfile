FROM node:22-alpine AS build

RUN apk add --no-cache git && corepack enable
WORKDIR /src
ARG WEB_COMMIT=1c66c50c4a6bb897fa5e77f549231557d04b0c6d
RUN git clone https://github.com/meshtastic/web.git . \
    && git checkout "$WEB_COMMIT" \
    && pnpm install --frozen-lockfile

COPY sdk-broadcast-noack.patch /tmp/sdk-broadcast-noack.patch
RUN git apply /tmp/sdk-broadcast-noack.patch

COPY app /src/apps/web/embedded
RUN pnpm --filter meshtastic-web exec vite build \
      --config embedded/vite.config.ts embedded \
    && mkdir -p /output \
    && gzip -9 -c /src/apps/embedded-dist/index.html > /output/index.html.gz \
    && gzip -9 -c /src/apps/embedded-dist/app.js > /output/app.js.gz \
    && gzip -9 -c /src/apps/embedded-dist/style.css > /output/style.css.gz \
    && cp /src/apps/embedded-dist/index.html /output/index.html \
    && cp /src/apps/embedded-dist/app.js /output/app.js \
    && cp /src/apps/embedded-dist/style.css /output/style.css \
    && cp /src/apps/embedded-dist/node-location.json /output/node-location.json

FROM scratch
COPY --from=build /output /
