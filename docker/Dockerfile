# docker/Dockerfile
FROM python:3.13-slim

WORKDIR /app

# Copy only what the install needs first, so dependency layers cache across
# source edits.
COPY pyproject.toml ./
COPY almacen ./almacen

RUN pip install --no-cache-dir .

# Blobs and the SQLite database live here; mounted as a volume per node.
ENV ALMACEN_DATA_DIR=/data
RUN mkdir -p /data

EXPOSE 8000 50051

CMD ["uvicorn", "almacen.main:app", "--host", "0.0.0.0", "--port", "8000"]
