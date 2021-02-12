# HFP Loader

The task of this application is to fetch archived HFP data from an Azure storage container and load it into a Postgres database.

## Current state

The app runs on a local computer as a Node script. The date is provided as the first command line argument.

First install all dependencies:

```
yarn
```

Then create a `.env` file using the example and set the Postgres connection parameters as well as the Azure storage key.

If you need a local database, set one up with Docker:

```
docker run -p 5432:5432 -v [host path to data dir]:/var/lib/postgresql/data --env POSTGRES_PASSWORD=password --name loader-db postgres postgres
```

Use the included `postgres_schema.sql` file to create the database tables.

Then, run the app with this command:

```
yarn start 2021-02-09
```

Substitute the date for the date you want to load.

## The Plan

Ultimately this app will run as a web service where an authenticated user can input a date, which starts a queued job to run the import. Once it is done, the user is notified by email and can view the data in the Reittiloki application.

The data will hang around for a while and then be automatically removed if it is older than the window which the Reittiloki database keeps data around for.

If the date which is requested is already being loaded, this should be detected and a duplicate process should not start.
