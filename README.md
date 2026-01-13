# OceanBase Database Proxy

TODO: some badges here

OceanBase Database Proxy (ODP for short) is a dedicated proxy server for OceanBase Database. OceanBase Database stores its user data in multiple copies on different OBServers. ODP receives the SQL requests from the users, transfers the SQL requests to the target OBServer, then returns the execution results to the users.

## Quick start

Refer to the Get Started guide [link TODO] to try out ODP.

### build.sh Quick Build Guide

`build.sh` is the main build script for the ODP project, supporting multiple build modes and platforms. Here are the commonly used build commands:

#### Basic Usage

```bash
# Clean previous build files
./build.sh clean
# Initialize dependencies (first-time build)
./build.sh init
# Initialize dependencies (non-first-time build)
./build.sh qinit
# Configure for debug build (debug can be replaced with release or other options)
./build.sh config debug
# Execute build
./build.sh make
# Build output: binary executable located at `src/obproxy/obproxy`

# Other operations
# Generate RPM package (Note: In branch 435 and earlier versions, calling ./build.sh rpm may cause "cp: No such file or directory" error, which can be resolved by calling ./build.sh rpm obproxy-ce)
./build.sh rpm
```

## Documentation

- English [link TODO]
- Simplified Chinese (简体中文) [link TODO]

## Licencing

ODP is under [MulanPubL - 2.0](https://license.coscl.org.cn/MulanPubL-2.0/index.html) licence. You can freely copy and use the source code. When you modify or distribute the source code, please obey the MulanPubL - 1.0 licence.

## Contributing

Contributions are warmly welcomed and greatly appreciated. Here are a few ways you can contribute:

- Raise us an issue [link TODO].
- Submit Pull Requests. For details, see [How to contribute](CONTRIBUTING.md).

## Support

In case you have any problems when using OceanBase Database, welcome reach out for help:

- GitHub Issue [link TODO]
- Official forum [link TODO]
- Knowledge base [link TODO]
