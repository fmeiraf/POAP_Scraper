FROM python:3.8

ENV BASEURL=https://hub.snapshot.org/graphql
ENV OUTPATH=/results
ENV SPACE_ID=None

RUN pip install gql[all]
RUN pip install tqdm

RUN mkdir /scrapper
RUN mkdir /results
WORKDIR /scrapper

COPY ./SnapshotScrapper.py /scrapper/SnapshotScrapper.py

CMD python SnapshotScrapper.py -b $BASEURL -o $OUTPATH -s $SPACE_ID

