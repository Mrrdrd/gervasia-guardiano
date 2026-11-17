FROM python:3.13-slim

RUN pip3 install --no-cache-dir anthropic==2.9.0 aiohttp==3.14.5

COPY guardiano.py /app/guardiano.py
WORKDIR /app

CMD [ "python3", "-u", "/app/guardiano.py" ]
