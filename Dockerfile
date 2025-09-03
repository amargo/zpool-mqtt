FROM python:3.13.7-slim

WORKDIR /app

# Install zfsutils-linux
RUN set -eux; \
  if [ -f /etc/apt/sources.list.d/debian.sources ]; then \
    sed -i -E 's/^Components: .*/Components: main contrib non-free non-free-firmware/' /etc/apt/sources.list.d/debian.sources; \
  else \
    sed -i 's/ main$/ main contrib non-free non-free-firmware/' /etc/apt/sources.list; \
  fi; \
  apt-get update; \
  apt-get install -y --no-install-recommends zfsutils-linux ca-certificates; \
  rm -rf /var/lib/apt/lists/*

# Copy requirements first for better caching
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy code
COPY app/ .

ENTRYPOINT ["python", "zpool-list.py"]