# HFP Loader

The task of this application is to fetch archived HFP data from an Azure storage container and load it into a Postgres database.

## Current state

The app runs on a computer as a Node script. The date is provided as the first command line argument.

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

Then, run the app with this command, giving the date to load data for as the only argument:

```
yarn start 2021-02-09
```

### Run with Docker

To run on a server, the best option is probably using Docker. The image can be built and pushed to the Transitlog registry in Azure with the `./deploy-all.sh` script. Make sure you are logged in to the registry prior to running it.

Pull the image onto a server with:

```
docker pull transitlogregistry.azurecr.io/hsl/hfp-loader
```

Then create an env file where you are planning to run the HFP loader. The env file should at least contain all secret variables that are missing from .env.example.

Run the HFP Loader like this:

```
docker run --env-file .env -it transitlogregistry.azurecr.io/hsl/hfp-loader 2021-02-11
```

Substitute `.env` with the name of the env file you just created, and the date at the end with the date you want to load HFP data for. Optionally substitute `-it` with `-d` to make it run detached, or in the background.

## The Plan

Ultimately this app will run as a web service where an authenticated user can input a date, which starts a queued job to run the import. Once it is done, the user is notified by email and can view the data in the Reittiloki application.

The data will hang around for a while and then be automatically removed if it is older than the window which the Reittiloki database keeps data around for.

If the date which is requested is already being loaded, this should be detected and a duplicate process should not start.
