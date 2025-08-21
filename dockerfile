FROM python:3.12-alpine

RUN apk add --no-cache ca-certificates curl py3-pip jq bash git gcc make python3-dev libc-dev libffi-dev openssl-dev libc6-compat gcompat && rm -f /var/cache/apk/*

RUN AZCOPY_VERSION=10.30.0 && \
  curl -sSL https://aka.ms/downloadazcopy-v10-linux -o /tmp/azcopy_linux.tar.gz && \
  mkdir -p /opt/azcopy && \
  tar xzf /tmp/azcopy_linux.tar.gz -C /opt/azcopy && \
  chmod +x /opt/azcopy/azcopy_linux_amd64_${AZCOPY_VERSION}/azcopy && \
  ln -s /opt/azcopy/azcopy_linux_amd64_${AZCOPY_VERSION}/azcopy /usr/bin/azcopy && \
  rm /tmp/azcopy_linux.tar.gz

RUN apk add --no-cache \
    less \
    ncurses-terminfo-base \
    krb5-libs \
    libgcc \
    libintl \
    libssl3 \
    libstdc++ \
    tzdata \
    userspace-rcu \
    zlib \
    curl \
    icu-libs && \
    apk -X https://dl-cdn.alpinelinux.org/alpine/edge/main add --no-cache lttng-ust && \
    rm -f /var/cache/apk/*

RUN apk -X https://dl-cdn.alpinelinux.org/alpine/edge/main add --no-cache \
    lttng-ust \
    openssh-client

RUN POWERSHELL_VERSION=7.5.2 && \
    curl -L https://github.com/PowerShell/PowerShell/releases/download/v${POWERSHELL_VERSION}/powershell-${POWERSHELL_VERSION}-linux-musl-x64.tar.gz -o /tmp/powershell.tar.gz && \
    mkdir -p /opt/microsoft/powershell/7 && \
    tar zxf /tmp/powershell.tar.gz -C /opt/microsoft/powershell/7 && \
    chmod +x /opt/microsoft/powershell/7/pwsh && \
    ln -s /opt/microsoft/powershell/7/pwsh /usr/bin/pwsh && \
    rm /tmp/powershell.tar.gz

ARG REPOSITORY=PSGallery
ARG MODULE_AZ=Az
ARG VERSION_AZ=14.1.0

### install azure-powershell from PSGallery
RUN pwsh -Command Set-PSRepository -Name ${REPOSITORY} -InstallationPolicy Trusted && \
    pwsh -Command Install-Module -Name ${MODULE_AZ} -RequiredVersion ${VERSION_AZ} -Scope AllUsers -Repository ${REPOSITORY} && \
    pwsh -Command Set-PSRepository -Name ${REPOSITORY} -InstallationPolicy Untrusted

ARG MODULE_AZ_TABLE=AzTable
ARG VERSION_AZ_TABLE=2.1.0

RUN pwsh -Command Set-PSRepository -Name ${REPOSITORY} -InstallationPolicy Trusted && \
    pwsh -Command Install-Module -Name ${MODULE_AZ_TABLE} -RequiredVersion ${VERSION_AZ_TABLE} -Scope AllUsers -Repository ${REPOSITORY} && \
    pwsh -Command Set-PSRepository -Name ${REPOSITORY} -InstallationPolicy Untrusted

# Copy Python source files into the container
COPY . /app

# Set working directory
WORKDIR /app

# Create Python virtual environment and install dependencies
RUN python3 -m venv /app/venv && \
    . /app/venv/bin/activate && \
    pip install --upgrade pip && \
    pip install --upgrade setuptools wheel && \
    pip install -r requirements.txt

ARG AZURE_SOURCE_STORAGE_ACCOUNT_BLOB
ARG AZURE_DESTINATION_STORAGE_ACCOUNT_BLOB
ARG OVERWRITE_STORAGE_ACCOUNT_BLOB
ARG AZURE_SOURCE_STORAGE_ACCOUNT_QUEUE
ARG AZURE_DESTINATION_STORAGE_ACCOUNT_QUEUE
ARG AZURE_SOURCE_STORAGE_ACCOUNT_TABLE
ARG AZURE_DESTINATION_STORAGE_ACCOUNT_TABLE
ARG AZURE_SOURCE_CONNECTION_STRING_FILE_SHARE
ARG AZURE_DEST_CONNECTION_STRING_FILE_SHARE

# Use venv Python interpreter to run your script
CMD ["/app/venv/bin/python", "main.py"]
