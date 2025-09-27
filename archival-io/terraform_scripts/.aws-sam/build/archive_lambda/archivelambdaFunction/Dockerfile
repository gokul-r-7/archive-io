FROM amazonlinux:2

# Set working directory
WORKDIR /app

# Install Python 3.9 and build tools
RUN yum -y update && \
    yum install -y \
    python39 \
    python39-devel \
    gcc \
    gcc-c++ \
    make \
    zip \
    unixODBC \
    unixODBC-devel \
    freetds-devel \
    libaio \
    curl \
    git \
    which \
    unzip && \
    curl -O https://download.oracle.com/otn_software/linux/instantclient/211000/instantclient-basiclite-linux.x64-21.1.0.0.0.zip && \
    unzip instantclient-basiclite-linux.x64-21.1.0.0.0.zip -d /opt/oracle && \
    echo /opt/oracle/instantclient_21_1 > /etc/ld.so.conf.d/oracle-instantclient.conf && \
    ldconfig && \
    yum clean all

ENV PATH="/usr/local/bin:/opt/oracle/instantclient_21_1:$PATH"
ENV LD_LIBRARY_PATH="/opt/oracle/instantclient_21_1"

# Upgrade pip and install pip packages into /app
COPY requirements.txt .
RUN python3.9 -m ensurepip && \
    python3.9 -m pip install --upgrade pip && \
    python3.9 -m pip install --no-cache-dir -r requirements.txt --target .

# Copy your lambda function code
COPY lambda_function.py .

# Create the final zip
RUN zip -r9 /lambda_function.zip .

CMD ["echo", "Packaging complete"]
