FROM python:3.8

ENV OUTPATH=results

RUN pip install pandas
RUN pip install pyyaml
RUN pip install requests



RUN mkdir /scrapper
RUN mkdir /results

WORKDIR /scrapper

# COPY ./PoapScrapper.py /scrapper/PoapScrapper.py
# COPY ./parameters.yaml /scrapper/parameters.yaml
# COPY ./results /scrapper/results
COPY . /scrapper

CMD python PoapScrapper.py -o $OUTPATH -c

