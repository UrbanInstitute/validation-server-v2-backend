FROM public.ecr.aws/lambda/python:3.9

ENV R_VERSION=4.2.1
ENV PATH="${PATH}:/opt/R/${R_VERSION}/bin/" 
ENV LD_LIBRARY_PATH="/opt/R/${R_VERSION}/lib/R/lib/" 

RUN yum -y install wget git tar

RUN yum -y install https://dl.fedoraproject.org/pub/epel/epel-release-latest-7.noarch.rpm \
  && wget https://cdn.rstudio.com/r/centos-7/pkgs/R-${R_VERSION}-1-1.x86_64.rpm \
  && yum -y install R-${R_VERSION}-1-1.x86_64.rpm \
  && rm R-${R_VERSION}-1-1.x86_64.rpm \ 
  && yum -y install openssl-devel \
  && yum -y install libxml2-devel 

RUN R -e "install.packages(c('dplyr', 'tidyr', 'aws.s3', 'broom', 'remotes'), \
  repos = c(CRAN = 'https://packagemanager.posit.co/cran/__linux__/centos7/latest'))"
RUN R -e "remotes::install_github('UrbanInstitute/validation-server-v2-r-package', dependencies = FALSE)"

COPY requirements.txt  .
RUN  pip3 install -r requirements.txt --target "${LAMBDA_TASK_ROOT}"

COPY . ${LAMBDA_TASK_ROOT} 