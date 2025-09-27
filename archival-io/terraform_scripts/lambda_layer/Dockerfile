FROM public.ecr.aws/sam/build-python3.9:latest

WORKDIR /var/task

COPY requirements.txt .

RUN pip install --upgrade pip && \
    pip install -r requirements.txt -t python/

RUN zip -r lambda_layer.zip python > /dev/null

CMD cp lambda_layer.zip /output/lambda_layer.zip
