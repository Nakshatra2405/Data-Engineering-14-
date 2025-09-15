CREATE USER retailuser WITH PASSWORD 'yourpasswor';

GRANT ALL PRIVILEGES ON DATABASE retaildb TO retailuser;

GRANT ALL PRIVILEGES ON SCHEMA public TO retailuser;

Select * from sales;