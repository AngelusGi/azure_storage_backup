FROM alpine:3.22.1

RUN apk add --no-cache ca-certificates curl py3-pip jq bash git gcc make python3-dev libc-dev libffi-dev openssl-dev libc6-compat gcompat && rm -f /var/cache/apk/*

RUN AZCOPY_VERSION=10.30.0 && \
  curl -sSL https://aka.ms/downloadazcopy-v10-linux -o /tmp/azcopy_linux.tar.gz && \
  mkdir -p /opt/azcopy && \
  tar xzf /tmp/azcopy_linux.tar.gz -C /opt/azcopy && \
  chmod +x /opt/azcopy/azcopy_linux_amd64_${AZCOPY_VERSION}/azcopy && \
  ln -s /opt/azcopy/azcopy_linux_amd64_${AZCOPY_VERSION}/azcopy /usr/bin/azcopy && \
  rm /tmp/azcopy_linux.tar.gz

RUN pip3 install --upgrade pip
RUN pip3 install --upgrade setuptools wheel
RUN pip3 install --no-cache azure-cli==2.76.0

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

RUN POWERSHELL_VERSION=7.4.10 && \
    curl -L https://github.com/PowerShell/PowerShell/releases/download/v${POWERSHELL_VERSION}/powershell-${POWERSHELL_VERSION}-linux-musl-x64.tar.gz -o /tmp/powershell.tar.gz && \
    mkdir -p /opt/microsoft/powershell/7 && \
    tar zxf /tmp/powershell.tar.gz -C /opt/microsoft/powershell/7 && \
    chmod +x /opt/microsoft/powershell/7/pwsh && \
    ln -s /opt/microsoft/powershell/7/pwsh /usr/bin/pwsh && \
    rm /tmp/powershell.tar.gz

ARG CONFIG=config
ARG AZURERM_CONTEXT_SETTINGS=AzureRmContextSettings.json
ARG REPOSITORY=PSGallery
ARG MODULE=Az
ARG CONFIG=config
ARG VERSION=7.1.0

### install azure-powershell from PSGallery
RUN pwsh -Command Set-PSRepository -Name ${REPOSITORY} -InstallationPolicy Trusted && \
    pwsh -Command Install-Module -Name ${MODULE} -RequiredVersion ${VERSION} -Scope AllUsers -Repository ${REPOSITORY} && \
    pwsh -Command Set-PSRepository -Name ${REPOSITORY} -InstallationPolicy Untrusted
