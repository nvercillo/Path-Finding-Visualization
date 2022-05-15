FROM python:3.7.3-stretch
ENV PYTHONUNBUFFERED=1

USER root 

WORKDIR /home/root/app/tests

RUN mkdir /home/root/.gnupg

# To fix the " gpg: WARNING: unsafe permissions on homedir '/home/path/to/user/.gnupg' " error
# Make sure that the .gnupg directory and its contents is accessibile by your user.
RUN chown -R root /home/root/.gnupg/

# Also correct the permissions and access rights on the directory
RUN chmod 600 /home/root/.gnupg

RUN touch /home/root/.gnupg/txt

RUN chmod 700 /home/root/.gnupg/*


COPY . ../

RUN pip install --user -r ../requirements.txt 

ENTRYPOINT ["python", "-u",  "test.py"]