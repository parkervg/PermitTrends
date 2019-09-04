FROM python:3.7.2
EXPOSE 5000
COPY requirements.txt /requirements.txt
WORKDIR /api

RUN pip3 install -r /requirements.txt


# Build and install geos - needed for basemap (several warnings/errors on compiling that can be ignored)
RUN echo '/usr/local/lib' >> /etc/ld.so.conf
RUN wget -q -N http://download.osgeo.org/geos/geos-3.7.2.tar.bz2
RUN tar -xjf geos-3.7.2.tar.bz2
RUN cd geos-3.7.2 && ./configure && make -j 2 && make install && ldconfig && cd ..

# Install Basemap
ADD https://github.com/matplotlib/basemap/archive/v1.2.0rel.tar.gz ./
RUN tar -xzf v1.2.0rel.tar.gz && rm v1.2.0rel.tar.gz
RUN cd basemap-1.2.0rel && python3 setup.py install


COPY . /api
# Allows print statements to output to console
ENV PYTHONUNBUFFERED=1
CMD ["gunicorn", "-b", "0.0.0.0:5000" "api.py"]
