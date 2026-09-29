# docker/Dockerfile
FROM python:3.13-slim

WORKDIR /app

# Install dependencies from the manifest alone first. Because the source is not
# copied yet, editing a source file cannot invalidate this layer, so rebuilds
# skip re-downloading every dependency. A stub package satisfies setuptools,
# which needs the package directory to exist at install time.
COPY pyproject.toml ./
RUN mkdir -p almacen && touch almacen/__init__.py \
    && pip install --no-cache-dir . \
    && pip uninstall -y almacen

# Now the real source, and an install that only has to wire up this package.
COPY almacen ./almacen
RUN pip install --no-cache-dir --no-deps .

# Blobs and the SQLite database live here; mounted as a volume per node.
ENV ALMACEN_DATA_DIR=/data
RUN mkdir -p /data

EXPOSE 8000 50051

CMD ["uvicorn", "almacen.main:app", "--host", "0.0.0.0", "--port", "8000"]
