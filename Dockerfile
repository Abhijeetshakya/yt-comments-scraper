FROM apify/actor-python:3.13
COPY requirements.txt ./
RUN python -m pip install --no-cache-dir -r requirements.txt
COPY src/ ./src/
COPY .actor/ ./.actor/
CMD ["python", "-m", "src"]
