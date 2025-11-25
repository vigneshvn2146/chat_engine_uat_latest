FROM public.ecr.aws/lambda/python:3.12

# Disable annobin plugin globally by editing RPM config
RUN sed -i 's/^plugins=.*$/plugins= nada/' /etc/rpm/macros && \
    mv /usr/lib/rpm/redhat/redhat-annobin-plugin-select.sh /usr/lib/rpm/redhat/redhat-annobin-plugin-select.sh.disabled || true

# Install build tools
RUN dnf -y update && \
    dnf -y install gcc gcc-c++ make glibc-langpack-en && \
    dnf -y remove annobin annobin-plugin-gcc || true && \
    dnf clean all

# 📦 Install Python dependencies
COPY requirements.txt .
# 📦 Install Python dependencies (force reinstall pyspellchecker first)
RUN pip install --upgrade pip && \
    pip install -r requirements.txt && \
    pip install boto3 botocore

# 📁 Copy application code
COPY app1.py .
COPY dynamodb.py .
COPY utils/ ./utils/
COPY .env .
COPY . ./

# 🧠 Set the Lambda handler
CMD ["app1.lambda_handler"]
