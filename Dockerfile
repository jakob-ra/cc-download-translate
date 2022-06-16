FROM amazonlinux:latest

RUN yum -y install git
RUN git clone https://github.com/jakob-ra/cc-download
RUN cd cc-download
RUN yum -y install pip
RUN pip install -r requirements.txt
ADD fetch_and_run.sh /usr/local/bin/fetch_and_run.sh
WORKDIR /tmp
USER nobody

ENTRYPOINT ["/usr/local/bin/fetch_and_run.sh"]