# 使用官方 Python 镜像
FROM python:3.12-slim

LABEL maintainer="Cybwan <baili@flomesh.io>"

ENV TZ=Asia/Shanghai
RUN ln -snf /usr/share/zoneinfo/$TZ /etc/localtime && echo $TZ > /etc/timezone
RUN ln -fs /bin/bash /bin/sh

RUN apt update && apt upgrade -y && apt install -y --no-install-recommends \
        sudo \
        ca-certificates openssh-server \
        gnupg git \
        net-tools wget curl \
        iproute2 tcpdump ethtool iputils-ping arping dnsutils unzip jq yq \
        nodejs npm \
    && rm -rf /var/lib/apt/lists/*

RUN pip install --upgrade pip uv
RUN rm -rf ~/.cache/pip

RUN mkdir -p /home/devuser/.local/bin && useradd -m devuser && chown -R devuser /home/devuser
USER devuser

ENV PATH=/home/devuser/.local/bin:$PATH
ENV UV_LINK_MODE=copy

RUN chown -R devuser:devuser /home/devuser/.local

# 暴露端口
EXPOSE 4326
EXPOSE 35139

# 设置工作目录
WORKDIR /home/devuser

# 复制依赖文件并安装
# COPY requirements.txt .
