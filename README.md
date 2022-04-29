OpenTSDB Horizon Config
=======================

This repo contains modules used for handling various configurations of an OpenTSDB
observability system including alerts, dashboards, namespaces, users, etc.

# NOTE: This is still an OSS work in progress and needs some additional plugins to
tie it all together.

## IDE configuration

Please follow the [IntelliJ Idea](https://projectlombok.org/setup/intellij) setup procedure.

Step by step guide and setup for Eclipse are available [here](https://www.baeldung.com/lombok-ide).

## Usefull commands
- Update local db schema:
```
./gradlew local-db update
```

- Upgrade Liquibase

  Ref: [Liquibase doc](https://github.com/liquibase/liquibase-gradle-plugin#upgrading-the-version-of-liquibase-itself)
1. Make sure all of your Liquibase managed databases are up to date by running 
   `gradle update` on them before upgrading to the new version of the Liquibase plugin.
2. Create a new, throw away database to test your Liquibase change sets. Run 
   `gradle update` on the new database using the latest version of the Liquibase plugin. 
   This is important because of the deprecated items in the Groovy DSL, and because 
   there are some subtle differences in the ways the different Liquibase versions generate 
   SQL. For example, adding a default value to a boolean column in MySql using 
   `defaultValue: "0"` worked fine in Liquibase 2, but in Liquibase 3, it generates 
   SQL that doesn't work for MySql - `defaultValueNumeric: 0` needs to be used instead.
3. Once you are sure all of your change sets work with the latest Liquibase plugin, 
   clear all checksums that were calculated by the old version of Liquibase 2 by 
   running `gradle clearChecksums` against all databases.
4. Finally, run `gradle changeLogSync` on all databases to calculate new checksums.

- Upgrade Local Liquibase
```
./gradlew local-db clearChecksums
./gradlew local-db changeLogSync
```

- Run unit tests:
```
./gradlew test
```
- Run integration tests:
```
./gradlew integrationTest -Penv=dev
```

## Start local server on Mac
Follow these steps:

1. Create certificates
Run the following command from the project root. It creates a java keystore file
(javakeystore.jks) with the self signed certificates in it.
```
scripts/create_dev_certificates.sh
```

2. Add the following entry to the host file (/etc/hosts)
```
127.0.0.1	dev-config.horizon.com
```

4. Swagger Configuration
Download [Swagger UI](https://github.com/swagger-api/swagger-ui/archive/master.zip). 
Extract the zip file and copy the `dist` directory to your user home directory.
You can find the file `index.html` under `dist`. Modify the url link from 
`https://petstore.swagger.io/v2/swagger.json` to `https://dev-config.horizon.com:4443/api/swagger.json` 
in index.html. Rename the directory `dist` to `swagger`

5. Start server
```
./gradlew run --args 'dev'
```

6. Start server in debug mode
```
./gradlew run --args 'dev' --debug-jvm
```

7. Connect to remote db machine through ssh tunnel
```
ssh -L 3306:127.0.0.1:3306 {db_host_name}

```

You should be able access the [Swagger page](https://dev-config.horizon.com:4443/swagger)

## Run Docker container on Mac

1. Docker run
```
docker run --name configapi -p 8080:8080 IMAGE_ID

or
 
docker run -dit --rm -e JAVA_OPTS=-Dlog4j.configurationFile=/opt/opentsdb/horizon/configapi/log4j2/log4j2.xml -e LOG_DIR=/opt/opentsdb/horizon/configapi/shared-data/logs --mount type=bind,source="$(pwd)",target=/opt/opentsdb/horizon/configapi/shared-data --name configapi -p 8080:8080 {IMAGE_ID} -c=file:///opt/opentsdb/horizon/configapi/shared-data/config.yaml
```

2. SSH into docker container:
```
docker exec -it configapi /bin/sh
```

## Run Under OpenTSDB With Horizon UI

This is still a bit of a work in progress but ConfigDB can run as servlets in the
OpenTSDB server and work with the [OpenTSDB Horizon](https://github.com/OpenTSDB/opentsdb-horizon)
UI to manage dashboards for a default user. Here are the steps to get it running.

1. Setup a MySQL server (help us support others!). Tons of resources on how to do this.
   1. Setup a database called `opentsdb`, e.g. via `CREATE DATABASE 'opentsdb';`
   2. Create and grant a user full rights to the `opentsdb` database.
   3. Temporarily edit the `schema/build.gradle` file, modifying the `local-db` tasks
      liquibase stanza ala:
      ```groofy
        liquibase {
          activities {
            main {
              changeLogFile changeLog
              url 'jdbc:mysql://<DB_HOSTNAME>/opentsdb?serverTimezone=UTC'
              username '<DB_USER>'
              password '<DB_PASS>'
            }
          }
        }
      ```
      Replace the `DB_HOSTNAME`, `DB_USERNAME` and `DB_PASSWORD` with the proper host
      and credentials. E.g.
      ```groofy
        liquibase {
          activities {
            main {
              changeLogFile changeLog
              url 'jdbc:mysql://localhost/opentsdb?serverTimezone=UTC'
              username 'tsdb'
              password 'temppass'
            }
          }
        }
      ```
      :warning: **Do NOT** commit the contents after you edit.
   4. Run `./gradlew local-db update` and it *should* connect and create tables
      in the DB.
2. **TODO** - Somehow get a collection of the Horizon Config libraries and dependencies
   in a directory. In the OpenTSDB config (typically `opentsdb.yaml`)  plugins 
   config section ala:
   ```yaml
   pluginLocations:
       - /usr/share/opentsdb/opentsdb-horizon-config/build/libs
   ```
3. Also in the `opentsdb.yaml` config, add the MySQL pool, Horizon service and
   Horizon config resources (Some day we should be able to load them automatically)
   e.g.:
   ```yaml
     tsd.plugin.config:
       configs:
         - plugin: net.opentsdb.threadpools.UserAwareThreadPoolExecutor
           isDefault: true
           type: net.opentsdb.threadpools.TSDBThreadPoolExecutor
         - plugin: net.opentsdb.horizon.SharedJDBCPool
           isDefault: true
           type: net.opentsdb.horizon.SharedJDBCPool
         - plugin: net.opentsdb.horizon.service.HorizonConfigServices
           isDefault: true
           type: net.opentsdb.horizon.service.HorizonConfigServices
   
         - plugin: net.opentsdb.horizon.resource.DashboardResource
           id: DashboardResource
           type: net.opentsdb.servlet.resources.ServletResource
         - plugin: net.opentsdb.horizon.resource.NamespaceResource
           id: NamespaceResource
           type: net.opentsdb.servlet.resources.ServletResource
         - plugin: net.opentsdb.horizon.resource.UserResource
           id: UserResource
           type: net.opentsdb.servlet.resources.ServletResource
         - plugin: net.opentsdb.horizon.resource.SnapshotResource
           id: UserResource
           type: net.opentsdb.servlet.resources.ServletResource
         - plugin: net.opentsdb.horizon.resource.AlertResource
           id: UserResource
           type: net.opentsdb.servlet.resources.ServletResource
         - plugin: net.opentsdb.horizon.resource.ContactResource
           id: UserResource
           type: net.opentsdb.servlet.resources.ServletResource
         ...
   ```
4. Next in the same `opentsdb.yaml` you'll need to set the passwords for the MySQL pool.
   Add the following lines:
   ```yaml
   mysqlpool.read.user: <DB_USER>
   mysqlpool.read.secret.key: PT:dbpass
   mysqlpool.read.url: jdbc:mysql://<DB_HOSTNAME>/opentsdb?serverTimezone=UTC
   
   mysqlpool.write.user: <DB_USER>
   mysqlpool.write.secret.key: PT:dbpass
   mysqlpool.write.url: jdbc:mysql://<DB_HOSTNAME>/opentsdb?serverTimezone=UTC
   
   # NOTE: This is the insecure plain-text password. Use a proper secrets plugin.
   dbpass: <DB_PASS>
   ```
   Again, replace the `<DB_USER>`, `<DB_HOSTNAME>` and `<DB_PASS>`. Also make sure
   that the plain text secret plugin is loaded and named `PT` by providing a command 
   line config like:
   `--config.providers=secrets://net.opentsdb.configuration.provider.PlainTextSecretProvider:PT,file:///etc/opentsdb/opentsdb.yaml`
5. Start the TSDB process and check the logs to make sure the resources loaded
   properly. If so you should be able to add a default user by making a POST
   call to the TSD `http://localhost:4242/api/v1/user/list` with a payload of:
   ```json
     [{
       "userid":"user.noauth",
       "name": "Default User",
       "creationMode": 0,
       "enabled":true
     }]
   ```
   If you get a 200 back with a payload containing an `updatetime` you're good to
   go. 

   At this point the config API should be ready to go. 
6. To use the Horizon UI with OpenTSDB and the config API you'll need a few more
   settings:
   1. First add rewrites to the `opentsdb.yaml` config:
      ```yaml
       tsd.http.rewrites:
         ^/$: /index.html
         ^/d/|a/.*: /index.html
       ```
   2. Set the `opentsdb.yaml` static directory to point to the location of the 
      packaged UI files. If building from source it would be in the `server/public`
      directory from the git repo. For the pre-build OpenTSDB docker container the
      files are in `/usr/share/opentsdb/static`.
      ```yaml
      tsd.http.staticroot: /usr/share/opentsdb/static
      ```
   3. Add CORs settings to the `opentsdb.yaml` file:
      ```yaml
       tsd.http.request.cors.pattern: .*
       tsd.http.request.cors.headers: Authorization,Content-Type,Link,X-Total-Count,Range,X-Horizon-DSHBID,X-Horizon-WID
       ```
   4. And finally you'll want to edit the Horizon UI config with a file named
      `config` in the same directory as the Horizon UI static files:
      ```json
      {
        "name"  : "OpenTSDB Horizon",
        "production": true,
        "readonly": false,
        "queryParams": null,
        "debugLevel": "ERROR",
        "uiBranding": {
          "logo": {
            "imageUrl": "/assets/horizon-logo-icon-only.png",
            "homeUrl": "/main"
          }
        },
        "tsdbCacheMode": null,
        "tsdbSource": null,
        "tsdb_hosts": [
          "http://localhost:4242"
        ],
        "configdb": "http://localhost:4242/api/v1",
        "metaApi": "http://localhost:4242/api",
        "auraUI": "http://localhost:4242",
        "alert": {
          "recipient": {
            "http": {
              "enable": false
            },
            "email": {
              "enable": true
            }
          }
        },
        "helpLinks": [
          { "label": "User guide", "href": "#" },
          { "label": "File a ticket", "href": "#" },
          { "label": "Talk to us", "icon": "d-slack", "href": "#" }
        ],
        "modules": {
          "dashboard": {
            "widget": {
              "overrideTime": true
            }
          }
        },
        "namespace": {
          "enabled": false,
          "default": "_default"
        },
        "auth": {
        "loginURL": "/login",
          "heartbeatURL": "/heartbeat",
          "heartbeatImgURL": "/heartbeatimg",
          "heartbeatInterval": 600
        }
      }
      ```
   5. Run the TSD and if you hit `http://localhost:4242` you should see the 
      Horizon UI load!

## Pluggable components

### Authentication Filter
To use a custom authentication filter, you will have to provide an implementation 
of `javax.servlet.Filter` and configure the fully qualified class name as shown below.
You can provide the filter init parameters as key and value pair. After successful 
authentication, the filter should set `java.security.Principal` in the `HttpServletRequest`.

```java
import javax.servlet.*;
import javax.servlet.http.*;
import java.io.IOException;
import java.security.Principal;

public class CustomAuthFilter implements Filter {
  @Override
  public void init(FilterConfig filterConfig) throws ServletException {
    // initialize the filter
  }
    
  @Override
  public void doFilter(ServletRequest request, ServletResponse response, FilterChain chain) throws IOException, ServletException {
    final HttpServletRequest httpServletRequest = (HttpServletRequest) request;
    final HttpServletResponse httpServletResponse = (HttpServletResponse) response;
    Cookie cookie = readCookie(httpServletRequest);
    if (null == cookie) {
      httpServletResponse.sendError(HttpServletResponse.SC_UNAUTHORIZED, "auth cookie not found");
    } else {
      Principal principal = authenticate(cookie.getValue());
      if (principal == null) {
        httpServletResponse.sendError(
        HttpServletResponse.SC_UNAUTHORIZED, "authentication failure");
      } else {
        HttpServletRequestWrapper requestWrapper =
          new HttpServletRequestWrapper(httpServletRequest) {
            @Override
            public Principal getUserPrincipal() {
              return principal;
            }
        };
        chain.doFilter(requestWrapper, response);
      }
    }
  }
}
```

```yaml
serverConfig:
  port: 8080
  ...
  authConfig:
    authFilterClassName: {FQCN of CustomAuthFilter}
    initParams:
          {key1}: {value1}
          {key2}: {value2}
```

### Secret Reader
It's not a good idea to mention the secrets for example the database user password 
in the free text format in the configuration file. It's recommended to store the 
secrets in an external key store and use the key name in the configuration file. 
You need to provide an implementation of `net.opentsdb.horizon.secrets.KeyReader` 
for the application to read the secrets from the key store.
You would also have to provide and implementation of 
`net.opentsdb.horizon.secrets.KeyReaderFactory` and configure it as shown below. 

```java
public class CustomKeyReader extends KeyReader {

  @Override
  protected byte[] readSecret(final String key) {
    // read the secret from the key store and return.
  }
}

public class CustomKeyReaderFactory implements KeyReaderFactory<CustomKeyReader> {
  @Override
  public CustomKeyReader createKeyReader(Map<String, Object> initParams) {
    // instantiate the CustomKeyReader and return
  }
}

```

Configure the key read factory in the `applicationConfig` section as shown here.

```yaml
applicationConfig:
  keyReaderFactoryClassName: {FQCN of CustomKeyReaderFactory}
  initParams:
    initParams:
          {key1}: {value1}
          {key2}: {value2}
```

Contribute
----------

Please see the [Contributing](contributing.md) file for information on how to
get involved. We welcome issues, questions, and pull requests.

Maintainers
-----------

* Smruti Ranjan Sahoo

License
-------

This project is licensed under the terms of the Apache 2.0 open source license.
Please refer to [LICENSE](LICENSE.md) for the full terms.