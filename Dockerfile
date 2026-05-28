FROM apache/spark:3.5.1

USER root

RUN pip install \
    faker \
    pandas \
    pyarrow

WORKDIR /app

CMD ["tail", "-f", "/dev/null"]