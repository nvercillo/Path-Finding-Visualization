FROM python:alpine3.7 
COPY . /app
WORKDIR /app
RUN pip install -r requirements.txt # Write Flask in this file
EXPOSE 5001 
ENTRYPOINT [ "python" ] 
CMD [ "flaskapp.py" ]