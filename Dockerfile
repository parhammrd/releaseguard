FROM node:22-slim AS frontend
WORKDIR /build
COPY package.json package-lock.json ./
RUN --mount=type=cache,target=/root/.npm npm ci --no-audit --no-fund
COPY app ./app
COPY public ./public
COPY next.config.ts tsconfig.json ./
RUN npm run build

FROM python:3.12-slim AS runtime
ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    PYTHONPATH=/app/backend \
    FRONTEND_DIST=/app/out
WORKDIR /app
COPY backend/requirements.txt ./backend/requirements.txt
RUN pip install --no-cache-dir -r backend/requirements.txt
COPY backend ./backend
COPY infra/schemas ./infra/schemas
COPY --from=frontend /build/out ./out
EXPOSE 8000
HEALTHCHECK --interval=5s --timeout=3s --retries=12 CMD python -c "import urllib.request; urllib.request.urlopen('http://127.0.0.1:8000/healthz')"
CMD ["uvicorn", "releaseguard.app:app", "--host", "0.0.0.0", "--port", "8000"]
