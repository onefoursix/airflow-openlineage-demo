FROM quay.io/astronomer/astro-runtime:3.1
COPY requirements.txt .
RUN pip install -r requirements.txt
