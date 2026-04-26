FROM python:3.12-slim

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1

WORKDIR /app

# System deps for some uvicorn[standard] extras (eg, httptools/uvloop not used on Windows
# but can be pulled in by the extra); keep minimal and rely on wheels when available.
RUN pip install --no-cache-dir --upgrade pip

COPY requirements.txt /app/requirements.txt
RUN pip install --no-cache-dir -r /app/requirements.txt

COPY . /app

EXPOSE 8585

# Ensure config writes DBs/uploads to /app/data (mounted as a volume in compose)
ENV VM_HOST=0.0.0.0 \
    VM_PORT=8585

CMD ["uvicorn", "app.main:app", "--host", "0.0.0.0", "--port", "8585"]
