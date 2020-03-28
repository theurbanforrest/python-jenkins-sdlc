FROM godatadriven/pyspark
ENV SPARK_HOME "/opt/miniconda3/lib/python3.7/site-packages/pyspark"

COPY requirements_local.txt /tmp/fching_utils/
RUN pip install -r /tmp/fching_utils/requirements_local.txt

COPY ./package_install.sh /
COPY ./ /tmp/fching_utils

RUN sh /package_install.sh

ENTRYPOINT ["jupyter", "notebook", "--ip=0.0.0.0", "--no-browser", "--allow-root", "--notebook-dir=/tmp", "--NotebookApp.token='' ", "--NotebookApp.password='' "]

EXPOSE 8888