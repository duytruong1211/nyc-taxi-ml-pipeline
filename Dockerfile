# syntax=docker/dockerfile:1

FROM mambaorg/micromamba:1.4.9
ENV GIT_PYTHON_REFRESH=quiet
WORKDIR /app

# Install environment fast with micromamba
COPY environment.yml /tmp/environment.yml
RUN micromamba create -y -n spark_env -f /tmp/environment.yml && \
    micromamba clean --all --yes

SHELL ["/bin/bash", "-c"]
ENV PATH=/opt/conda/envs/spark_env/bin:$PATH

# Copy all source code
COPY . .

# ✅ No entry.sh hack, use micromamba directly
ENTRYPOINT ["micromamba", "run", "-n", "spark_env", "python", "main.py"]
