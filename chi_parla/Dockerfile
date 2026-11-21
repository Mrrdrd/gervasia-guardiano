FROM python:3.13-slim

RUN pip3 install --no-cache-dir wyoming==1.11.0 sherpa-onnx==2.0.2 numpy==2.5.3 aiohttp==3.14.5

# CAM++ speaker model (3D-Speaker), from the sherpa-onnx releases on GitHub
ADD https://github.com/k2-fsa/sherpa-onnx/releases/download/speaker-recongition-models/3dspeaker_speech_campplus_sv_zh_en_16k-common_advanced.onnx /app/campplus.onnx

COPY chi_parla.py /app/chi_parla.py
WORKDIR /app

CMD [ "python3", "-u", "/app/chi_parla.py" ]
