FROM python:3.12-slim

WORKDIR /app

# install python deps first (this layer cached unless requirements.txt changes)
COPY webapp/requirements.txt /tmp/requirements.txt
RUN pip install --no-cache-dir -r /tmp/requirements.txt

# app code is mounted as a volume in dev (see docker-compose.yml),
# but we COPY here too so the image is self-contained for prod
COPY webapp/ /app/webapp/

EXPOSE 8000

# --reload watches webapp/ for changes so you don't rebuild on every edit
CMD ["uvicorn", "webapp.app:app", \
     "--host", "0.0.0.0", "--port", "8000", \
     "--reload", "--reload-dir", "/app/webapp"]
