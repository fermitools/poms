# For more information, please refer to https://aka.ms/vscode-docker-python
FROM scientificlinux/sl:7
RUN yum update -y
RUN yum -y groupinstall "Development Tools" -y
#RUN yum -y groupinstall "fermilab"
RUN yum -y install openssl-devel bzip2-devel libffi-devel -y
RUN yum install -y spack
#RUN yum install -y wget
#RUN wget https://www.python.org/ftp/python/3.8.3/Python-3.8.3.tgz
#RUN tar xvf Python-3.8.3.tgz
#RUN cd Python-3.8*/
#RUN ./configure --enable-optimizations
#RUN make install

RUN yum install gcc -y gcc-c++ -y
COPY yum.repos.d /etc/yum.repos.d
RUN ls /etc/yum.repos.d/
RUN yum install devtoolset-6-gcc-c++ devtoolset-6-gcc-gdb-plugin
#RUN yum install postgresql.x86_64 -y rh-postgresql96 -y
RUN yum install rh-postgresql96-postgresql-devel.x86_64 -y rh-postgresql96.x86_64 -y

# Keeps Python from generating .pyc files in the container
ENV PYTHONDONTWRITEBYTECODE=1

# Turns off buffering for easier container logging
ENV PYTHONUNBUFFERED=1

# Install pip requirements
COPY requirements.txt .
COPY get-pip.py .
RUN python get-pip.py
RUN python -m pip install -r requirements.txt

WORKDIR /home/poms
COPY . /home/poms

# Creates a non-root user with an explicit UID and adds permission to access the /app folder
# For more info, please refer to https://aka.ms/vscode-docker-python-configure-containers
RUN adduser -u 5678 --disabled-password --gecos "" appuser && chown -R appuser /app
USER appuser

# During debugging, this entry point will be overridden. For more information, please refer to https://aka.ms/vscode-docker-python-debug
CMD ["python", "setup.py"]
