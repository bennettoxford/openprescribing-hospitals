# Stage 1: Build Node.js dependencies and Svelte components
FROM node:20 as node-builder

WORKDIR /app

COPY package.json package-lock.json ./
COPY vite.config.js tailwind.config.mjs postcss.config.cjs svelte.config.js ./

RUN npm ci

COPY src ./src

# Copy templates directory
COPY templates ./templates

RUN npm run build

# Stage 2: Set up Python environment and run Django
FROM python:3.11-slim

WORKDIR /app

# Install system dependencies
RUN apt-get update && apt-get install -y --no-install-recommends \
    libpq-dev \
    gcc \
    && rm -rf /var/lib/apt/lists/*

# Install Python dependencies
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt gunicorn

# Copy the Django project
COPY . .

# Copy ALL built assets from the node-builder stage
COPY --from=node-builder /app/assets /app/assets

# Collect static files
RUN SECRET_KEY=dummy-key-for-build \
    DATABASE_URL=sqlite:///tmp/build.db \
    python manage.py collectstatic --noinput

# Expose the port the app runs on
EXPOSE 8000

# Copy the entrypoint script
COPY entrypoint.sh .
RUN chmod +x entrypoint.sh

# Set the entrypoint
ENTRYPOINT ["./entrypoint.sh"]
